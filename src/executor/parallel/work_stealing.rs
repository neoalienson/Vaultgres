use crossbeam::channel::{Sender, bounded};
use crossbeam::deque::{Injector, Stealer, Worker};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

pub struct WorkStealingScheduler<T> {
    global_queue: Arc<Injector<T>>,
    stealers: Vec<Stealer<T>>,
    workers: Vec<WorkerThread>,
    shutdown: Arc<AtomicBool>,
}

struct WorkerThread {
    handle: Option<thread::JoinHandle<()>>,
}

impl<T: Send + 'static> WorkStealingScheduler<T> {
    pub fn new<F>(num_workers: usize, task_handler: F) -> Self
    where
        F: Fn(T) + Send + Sync + 'static + Clone,
    {
        let global_queue = Arc::new(Injector::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut local_queues = Vec::new();
        let mut stealers = Vec::new();

        for _ in 0..num_workers {
            let worker = Worker::new_fifo();
            stealers.push(worker.stealer());
            local_queues.push(worker);
        }

        let workers = local_queues
            .into_iter()
            .enumerate()
            .map(|(id, local_queue)| {
                let global = Arc::clone(&global_queue);
                let stealers_clone = stealers.clone();
                let shutdown_flag = Arc::clone(&shutdown);
                let handler = task_handler.clone();

                let handle = thread::spawn(move || {
                    Self::worker_loop(
                        id,
                        local_queue,
                        global,
                        stealers_clone,
                        shutdown_flag,
                        handler,
                    );
                });

                WorkerThread { handle: Some(handle) }
            })
            .collect();

        Self { global_queue, stealers, workers, shutdown }
    }

    fn worker_loop<F>(
        worker_id: usize,
        local_queue: Worker<T>,
        global_queue: Arc<Injector<T>>,
        stealers: Vec<Stealer<T>>,
        shutdown: Arc<AtomicBool>,
        handler: F,
    ) where
        F: Fn(T),
    {
        while !shutdown.load(Ordering::Relaxed) {
            let task = local_queue.pop().or_else(|| global_queue.steal().success()).or_else(|| {
                stealers
                    .iter()
                    .enumerate()
                    .filter(|(id, _)| *id != worker_id)
                    .find_map(|(_, s)| s.steal().success())
            });

            match task {
                Some(t) => handler(t),
                None => thread::sleep(Duration::from_micros(100)),
            }
        }
    }

    pub fn submit(&self, task: T) {
        self.global_queue.push(task);
    }

    pub fn num_workers(&self) -> usize {
        self.workers.len()
    }
}

impl<T> Drop for WorkStealingScheduler<T> {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        for worker in &mut self.workers {
            if let Some(handle) = worker.handle.take() {
                let _ = handle.join();
            }
        }
    }
}

pub struct WorkStealingExecutor<T, R> {
    scheduler: WorkStealingScheduler<T>,
    _result_sender: Sender<R>,
}

impl<T: Send + 'static, R: Send + 'static> WorkStealingExecutor<T, R> {
    pub fn new<F>(num_workers: usize, task_handler: F) -> (Self, crossbeam::channel::Receiver<R>)
    where
        F: Fn(T) -> R + Send + Sync + 'static + Clone,
    {
        let (result_sender, result_receiver) = bounded(num_workers * 2);
        let sender_clone = result_sender.clone();

        let scheduler = WorkStealingScheduler::new(num_workers, move |task| {
            let result = task_handler(task);
            let _ = sender_clone.send(result);
        });

        (Self { scheduler, _result_sender: result_sender }, result_receiver)
    }

    pub fn submit(&self, task: T) {
        self.scheduler.submit(task);
    }

    pub fn num_workers(&self) -> usize {
        self.scheduler.num_workers()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn test_scheduler_creation() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let scheduler = WorkStealingScheduler::new(4, move |_: i32| {
            counter_clone.fetch_add(1, Ordering::Relaxed);
        });
        assert_eq!(scheduler.num_workers(), 4);
    }

    #[test]
    fn test_task_execution() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let scheduler = WorkStealingScheduler::new(2, move |_: i32| {
            counter_clone.fetch_add(1, Ordering::Relaxed);
        });

        for i in 0..10 {
            scheduler.submit(i);
        }

        thread::sleep(Duration::from_millis(100));
        assert_eq!(counter.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn test_work_stealing() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let scheduler = WorkStealingScheduler::new(4, move |val: i32| {
            thread::sleep(Duration::from_micros(val as u64 * 10));
            counter_clone.fetch_add(1, Ordering::Relaxed);
        });

        for i in 0..100 {
            scheduler.submit(i);
        }

        thread::sleep(Duration::from_millis(500));
        assert_eq!(counter.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn test_executor_with_results() {
        let (executor, receiver) = WorkStealingExecutor::new(2, |x: i32| x * 2);

        for i in 0..5 {
            executor.submit(i);
        }

        let mut results = Vec::new();
        for _ in 0..5 {
            if let Ok(result) = receiver.recv_timeout(Duration::from_millis(500)) {
                results.push(result);
            }
        }

        drop(executor);

        assert_eq!(results.len(), 5);
        results.sort();
        assert_eq!(results, vec![0, 2, 4, 6, 8]);
    }

    #[test]
    fn test_single_worker() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let scheduler = WorkStealingScheduler::new(1, move |_: i32| {
            counter_clone.fetch_add(1, Ordering::Relaxed);
        });

        for i in 0..10 {
            scheduler.submit(i);
        }

        thread::sleep(Duration::from_millis(100));
        assert_eq!(counter.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn test_many_workers() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let scheduler = WorkStealingScheduler::new(8, move |_: i32| {
            counter_clone.fetch_add(1, Ordering::Relaxed);
        });

        for i in 0..100 {
            scheduler.submit(i);
        }

        thread::sleep(Duration::from_millis(200));
        assert_eq!(counter.load(Ordering::Relaxed), 100);
    }
}
