use crate::catalog::Catalog;
use crate::executor::executor::{ExecutorError, SimpleTuple};
use crate::executor::parallel::morsel::Morsel;
use crate::executor::parallel::operator::ParallelOperator;
use std::sync::Arc;

pub struct ParallelSeqScan {
    table_name: String,
    catalog: Arc<Catalog>,
}

impl ParallelSeqScan {
    pub fn new(table_name: String, catalog: Arc<Catalog>) -> Self {
        Self { table_name, catalog }
    }
}

impl ParallelOperator for ParallelSeqScan {
    fn process_morsel(&self, mut morsel: Morsel) -> Result<Morsel, ExecutorError> {
        let row_count = self.catalog.row_count(&self.table_name);

        for i in morsel.start_offset..morsel.end_offset {
            if i < row_count {
                morsel.tuples.push(SimpleTuple { data: vec![i as u8] });
            }
        }

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
        catalog
            .create_table(
                "test".to_string(),
                vec![ColumnDef { name: "id".to_string(), data_type: DataType::Int }],
            )
            .unwrap();

        for i in 0..10 {
            catalog.insert("test", vec![Expr::Number(i)]).unwrap();
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
}
