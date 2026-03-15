use crate::executor::operators::executor::{ExecutorError, Tuple};
use crate::executor::parallel::config::ParallelConfig;
use crate::executor::parallel::morsel::Morsel;
use crate::executor::parallel::operator::ParallelOperator;
use crate::executor::parallel::worker_pool::WorkerPool;
use crossbeam::channel::bounded;
use std::sync::Arc;

pub struct ParallelSort {
    child: Arc<dyn ParallelOperator>,
    ascending: bool,
}

impl ParallelSort {
    pub fn new(child: Arc<dyn ParallelOperator>, ascending: bool) -> Self {
        Self { child, ascending }
    }

    pub fn execute(&self, config: &ParallelConfig) -> Result<Vec<Tuple>, ExecutorError> {
        let num_workers = config.max_workers();
        let pool = WorkerPool::new(num_workers);

        let (result_sender, result_receiver) = bounded(num_workers);

        // Submit local sort tasks
        for i in 0..num_workers {
            let morsel =
                Morsel { tuples: vec![], start_offset: 0, end_offset: 100, partition_id: i };
            pool.submit_task(morsel, Arc::clone(&self.child), result_sender.clone())?;
        }
        drop(result_sender);

        // Collect and merge sorted runs
        let mut runs = Vec::new();
        while let Ok(result) = result_receiver.recv() {
            let morsel = result?;
            runs.push(self.local_sort(morsel)?);
        }

        Ok(self.merge_sorted_runs(runs))
    }

    pub fn local_sort(&self, morsel: Morsel) -> Result<Vec<Tuple>, ExecutorError> {
        let child_result = self.child.process_morsel(morsel)?;
        let mut tuples = child_result.tuples;

        tuples.sort_by(|a: &Tuple, b: &Tuple| {
            let val_a = a.values().next();
            let val_b = b.values().next();
            match (val_a, val_b) {
                (Some(a), Some(b)) => a.cmp(b),
                (Some(_), None) => std::cmp::Ordering::Greater,
                (None, Some(_)) => std::cmp::Ordering::Less,
                (None, None) => std::cmp::Ordering::Equal,
            }
        });

        if !self.ascending {
            tuples.reverse();
        }

        Ok(tuples)
    }

    pub fn merge_sorted_runs(&self, mut runs: Vec<Vec<Tuple>>) -> Vec<Tuple> {
        let mut result = Vec::new();

        while runs.iter().any(|r| !r.is_empty()) {
            let mut min_idx = None;
            let mut min_val = None;

            for (i, run) in runs.iter().enumerate() {
                if let Some(first) = run.first() {
                    if min_val.is_none() || first.values().next() < min_val {
                        min_val = first.values().next();
                        min_idx = Some(i);
                    }
                }
            }

            if let Some(idx) = min_idx {
                let tuple = runs[idx].remove(0);
                result.push(tuple);
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Value;
    use crate::executor::test_helpers::TupleBuilder;

    struct MockOperator {
        tuples: Vec<Tuple>,
    }

    impl ParallelOperator for MockOperator {
        fn process_morsel(&self, mut morsel: Morsel) -> Result<Morsel, ExecutorError> {
            morsel.tuples = self.tuples.clone();
            Ok(morsel)
        }
    }

    #[test]
    fn test_local_sort() {
        let tuples = vec![
            TupleBuilder::new().with_int("val", 3).build(),
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 2).build(),
        ];

        let child = Arc::new(MockOperator { tuples });
        let sort = ParallelSort::new(child, true);

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 3, partition_id: 0 };

        let result = sort.local_sort(morsel).unwrap();
        assert_eq!(result[0].get("val"), Some(&Value::Int(1)));
        assert_eq!(result[1].get("val"), Some(&Value::Int(2)));
        assert_eq!(result[2].get("val"), Some(&Value::Int(3)));
    }

    #[test]
    fn test_merge_sorted_runs() {
        let run1 = vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 3).build(),
        ];
        let run2 = vec![
            TupleBuilder::new().with_int("val", 2).build(),
            TupleBuilder::new().with_int("val", 4).build(),
        ];

        let child = Arc::new(MockOperator { tuples: vec![] });
        let sort = ParallelSort::new(child, true);

        let result = sort.merge_sorted_runs(vec![run1, run2]);
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].get("val"), Some(&Value::Int(1)));
        assert_eq!(result[1].get("val"), Some(&Value::Int(2)));
        assert_eq!(result[2].get("val"), Some(&Value::Int(3)));
        assert_eq!(result[3].get("val"), Some(&Value::Int(4)));
    }

    #[test]
    fn test_empty_merge() {
        let child = Arc::new(MockOperator { tuples: vec![] });
        let sort = ParallelSort::new(child, true);

        let result = sort.merge_sorted_runs(vec![]);
        assert_eq!(result.len(), 0);
    }
}
