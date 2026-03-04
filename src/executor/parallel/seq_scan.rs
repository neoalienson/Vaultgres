use crate::catalog::Catalog;
use crate::executor::old_executor::{OldExecutorError as ExecutorError, SimpleTuple};
use crate::executor::parallel::config::ParallelConfig;
use crate::executor::parallel::morsel::Morsel;
use crate::executor::parallel::operator::ParallelOperator;
use crate::executor::parallel::worker_pool::WorkerPool;
use crossbeam::channel::bounded;
use std::sync::Arc;

pub struct ParallelSeqScan {
    table_name: String,
    catalog: Arc<Catalog>,
}

impl ParallelSeqScan {
    pub fn new(table_name: String, catalog: Arc<Catalog>) -> Self {
        Self { table_name, catalog }
    }

    pub fn execute(&self, config: &ParallelConfig) -> Result<Vec<SimpleTuple>, ExecutorError> {
        let row_count = (&*self.catalog).row_count(&self.table_name);
        if row_count == 0 {
            return Ok(vec![]);
        }

        let num_workers = config.max_workers();
        let pool = WorkerPool::new(num_workers);
        let chunk_size = row_count.div_ceil(num_workers);

        let (result_sender, result_receiver) = bounded(num_workers);
        let operator: Arc<dyn ParallelOperator> = Arc::new(SeqScanOperator {
            table_name: self.table_name.clone(),
            catalog: Arc::clone(&self.catalog),
        });

        // Submit tasks
        for i in 0..num_workers {
            let start = i * chunk_size;
            let end = ((i + 1) * chunk_size).min(row_count);
            if start >= row_count {
                break;
            }

            let morsel =
                Morsel { tuples: vec![], start_offset: start, end_offset: end, partition_id: i };
            pool.submit_task(morsel, Arc::clone(&operator), result_sender.clone())?;
        }
        drop(result_sender);

        // Collect results
        let mut all_tuples = Vec::new();
        while let Ok(result) = result_receiver.recv() {
            let morsel = result?;
            all_tuples.extend(morsel.tuples);
        }

        Ok(all_tuples)
    }
}

struct SeqScanOperator {
    table_name: String,
    catalog: Arc<Catalog>,
}

impl ParallelOperator for SeqScanOperator {
    fn process_morsel(&self, mut morsel: Morsel) -> Result<Morsel, ExecutorError> {
        let row_count = (&*self.catalog).row_count(&self.table_name);
        morsel.tuples.extend(
            (morsel.start_offset..morsel.end_offset.min(row_count))
                .map(|i| SimpleTuple { data: vec![i as u8] }),
        );
        Ok(morsel)
    }
}

impl ParallelOperator for ParallelSeqScan {
    fn process_morsel(&self, mut morsel: Morsel) -> Result<Morsel, ExecutorError> {
        let row_count = (&*self.catalog).row_count(&self.table_name);
        morsel.tuples.extend(
            (morsel.start_offset..morsel.end_offset.min(row_count))
                .map(|i| SimpleTuple { data: vec![i as u8] }),
        );
        Ok(morsel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{ColumnDef, DataType, Expr};

    #[test]
    fn test_parallel_seq_scan() {
        let catalog = Arc::new(Catalog::new());
        (&*catalog)
            .create_table("test".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
            .unwrap();

        for i in 0..10 {
            (&*catalog).insert("test", vec![Expr::Number(i)]).unwrap();
        }

        let scan = ParallelSeqScan::new("test".to_string(), catalog);
        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 5, partition_id: 0 };

        let result = scan.process_morsel(morsel).unwrap();
        assert_eq!(result.tuples.len(), 5);
    }

    #[test]
    fn test_scan_empty_table() {
        let catalog = Arc::new(Catalog::new());
        catalog.create_table("empty".to_string(), vec![]).unwrap();

        let scan = ParallelSeqScan::new("empty".to_string(), catalog);
        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 10, partition_id: 0 };

        let result = scan.process_morsel(morsel).unwrap();
        assert_eq!(result.tuples.len(), 0);
    }

    #[test]
    fn test_parallel_execute_with_workers() {
        let catalog = Arc::new(Catalog::new());
        (&*catalog)
            .create_table("test".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
            .unwrap();

        for i in 0..100 {
            (&*catalog).insert("test", vec![Expr::Number(i)]).unwrap();
        }

        let scan = ParallelSeqScan::new("test".to_string(), catalog);
        let config = ParallelConfig::new(4);
        let result = scan.execute(&config).unwrap();
        assert_eq!(result.len(), 100);
    }

    #[test]
    fn test_parallel_execute_empty_table() {
        let catalog = Arc::new(Catalog::new());
        catalog.create_table("empty".to_string(), vec![]).unwrap();

        let scan = ParallelSeqScan::new("empty".to_string(), catalog);
        let config = ParallelConfig::new(4);
        let result = scan.execute(&config).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_parallel_execute_single_worker() {
        let catalog = Arc::new(Catalog::new());
        (&*catalog)
            .create_table("test".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
            .unwrap();

        for i in 0..50 {
            (&*catalog).insert("test", vec![Expr::Number(i)]).unwrap();
        }

        let scan = ParallelSeqScan::new("test".to_string(), catalog);
        let config = ParallelConfig::new(1);
        let result = scan.execute(&config).unwrap();
        assert_eq!(result.len(), 50);
    }

    #[test]
    fn test_parallel_execute_many_workers() {
        let catalog = Arc::new(Catalog::new());
        (&*catalog)
            .create_table("test".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
            .unwrap();

        for i in 0..200 {
            (&*catalog).insert("test", vec![Expr::Number(i)]).unwrap();
        }

        let scan = ParallelSeqScan::new("test".to_string(), catalog);
        let config = ParallelConfig::new(8);
        let result = scan.execute(&config).unwrap();
        assert_eq!(result.len(), 200);
    }

    #[test]
    fn test_parallel_execute_more_workers_than_rows() {
        let catalog = Arc::new(Catalog::new());
        (&*catalog)
            .create_table("test".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
            .unwrap();

        for i in 0..5 {
            (&*catalog).insert("test", vec![Expr::Number(i)]).unwrap();
        }

        let scan = ParallelSeqScan::new("test".to_string(), catalog);
        let config = ParallelConfig::new(10);
        let result = scan.execute(&config).unwrap();
        assert_eq!(result.len(), 5);
    }
}
