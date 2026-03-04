use crate::executor::old_executor::{OldExecutorError as ExecutorError, SimpleTuple};
use crate::executor::parallel::morsel::{Morsel, MorselGenerator};
use crate::executor::parallel::operator::ParallelOperator;
use crate::executor::parallel::worker_pool::WorkerPool;
use crossbeam::channel::bounded;
use std::sync::Arc;

pub struct ParallelCoordinator {
    worker_pool: Arc<WorkerPool>,
}

impl ParallelCoordinator {
    pub fn new(num_workers: usize) -> Self {
        Self { worker_pool: Arc::new(WorkerPool::new(num_workers)) }
    }

    pub fn execute_parallel(
        &self,
        operator: Arc<dyn ParallelOperator>,
        morsel_gen: Arc<MorselGenerator>,
    ) -> Result<Vec<SimpleTuple>, ExecutorError> {
        let num_workers = self.worker_pool.num_workers();
        let (result_sender, result_receiver) = bounded(num_workers * 2);

        let mut tasks_submitted = 0;
        while let Some(range) = morsel_gen.next_morsel() {
            let morsel = Morsel {
                tuples: vec![],
                start_offset: range.start,
                end_offset: range.end,
                partition_id: 0,
            };
            self.worker_pool.submit_task(morsel, Arc::clone(&operator), result_sender.clone())?;
            tasks_submitted += 1;
        }

        drop(result_sender);

        let mut all_tuples = Vec::new();
        for _ in 0..tasks_submitted {
            if let Ok(result) = result_receiver.recv() {
                let morsel = result?;
                all_tuples.extend(morsel.tuples);
            }
        }

        Ok(all_tuples)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestOperator;

    impl ParallelOperator for TestOperator {
        fn process_morsel(&self, mut morsel: Morsel) -> Result<Morsel, ExecutorError> {
            for i in morsel.start_offset..morsel.end_offset {
                morsel.tuples.push(SimpleTuple { data: vec![i as u8] });
            }
            Ok(morsel)
        }
    }

    #[test]
    fn test_parallel_execution() {
        let coordinator = ParallelCoordinator::new(2);
        let operator = Arc::new(TestOperator);
        let morsel_gen = Arc::new(MorselGenerator::new(100, 10));

        let result = coordinator.execute_parallel(operator, morsel_gen).unwrap();
        assert_eq!(result.len(), 100);
    }

    #[test]
    fn test_empty_execution() {
        let coordinator = ParallelCoordinator::new(2);
        let operator = Arc::new(TestOperator);
        let morsel_gen = Arc::new(MorselGenerator::new(0, 10));

        let result = coordinator.execute_parallel(operator, morsel_gen).unwrap();
        assert_eq!(result.len(), 0);
    }
}
