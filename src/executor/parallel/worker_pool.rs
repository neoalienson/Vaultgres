use crate::executor::operators::executor::ExecutorError;
use crate::executor::parallel::morsel::Morsel;
use crate::executor::parallel::operator::ParallelOperator;
use crossbeam::channel::{bounded, Receiver, Sender};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

type TaskResult = Result<Morsel, ExecutorError>;

pub struct WorkerPool {
    workers: Vec<Worker>,
    task_sender: Sender<Task>,
    num_workers: usize,
    shutdown: Arc<AtomicBool>,
}

struct Worker {
    id: usize,
    handle: Option<thread::JoinHandle<()>>,
}

struct Task {
    morsel: Morsel,
    operator: Arc<dyn ParallelOperator>,
    result_sender: Sender<TaskResult>,
}

impl WorkerPool {
    pub fn new(num_workers: usize) -> Self {
        let (task_sender, task_receiver) = bounded::<Task>(num_workers * 2);
        let task_receiver = Arc::new(task_receiver);
        let shutdown = Arc::new(AtomicBool::new(false));

        let workers = (0..num_workers)
            .map(|id| {
                let receiver = Arc::clone(&task_receiver);
                let shutdown_flag = Arc::clone(&shutdown);
                let handle = thread::spawn(move || {
                    Self::worker_loop(id, receiver, shutdown_flag);
                });
                Worker { id, handle: Some(handle) }
            })
            .collect();

        Self { workers, task_sender, num_workers, shutdown }
    }

    fn worker_loop(
        _worker_id: usize,
        task_receiver: Arc<Receiver<Task>>,
        shutdown: Arc<AtomicBool>,
    ) {
        while !shutdown.load(Ordering::Relaxed) {
            match task_receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(task) => {
                    let result = task.operator.process_morsel(task.morsel);
                    let _ = task.result_sender.send(result);
                }
                Err(_) => {
                    // Timeout or disconnected, check shutdown flag
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                }
            }
        }
    }

    pub fn submit_task(
        &self,
        morsel: Morsel,
        operator: Arc<dyn ParallelOperator>,
        result_sender: Sender<TaskResult>,
    ) -> Result<(), ExecutorError> {
        let task = Task { morsel, operator, result_sender };
        self.task_sender
            .send(task)
            .map_err(|_| ExecutorError::IoError("Failed to submit task".to_string()))
    }

    pub fn num_workers(&self) -> usize {
        self.num_workers
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        for worker in &mut self.workers {
            if let Some(handle) = worker.handle.take() {
                let _ = handle.join();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Value;
    use crate::executor::test_helpers::TupleBuilder;

    struct TestOperator;

    impl ParallelOperator for TestOperator {
        fn process_morsel(&self, mut morsel: Morsel) -> Result<Morsel, ExecutorError> {
            let mut tuple = std::collections::HashMap::new();
            tuple.insert("val".to_string(), Value::Int(1));
            morsel.tuples.push(tuple);
            Ok(morsel)
        }
    }

    #[test]
    fn test_worker_pool_creation() {
        let pool = WorkerPool::new(4);
        assert_eq!(pool.num_workers(), 4);
    }

    #[test]
    fn test_task_execution() {
        let pool = WorkerPool::new(2);
        let operator = Arc::new(TestOperator);
        let (result_sender, result_receiver) = bounded(1);

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 10, partition_id: 0 };

        pool.submit_task(morsel, operator, result_sender).unwrap();
        let result = result_receiver.recv().unwrap().unwrap();
        assert_eq!(result.tuples.len(), 1);
    }
}
