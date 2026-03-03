use vaultgres::executor::parallel::work_stealing::{WorkStealingExecutor, WorkStealingScheduler};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn test_work_stealing_basic() {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = Arc::clone(&counter);

    let scheduler = WorkStealingScheduler::new(4, move |_: i32| {
        counter_clone.fetch_add(1, Ordering::Relaxed);
    });

    for i in 0..20 {
        scheduler.submit(i);
    }

    thread::sleep(Duration::from_millis(100));
    assert_eq!(counter.load(Ordering::Relaxed), 20);
}

#[test]
fn test_work_stealing_with_computation() {
    let (executor, receiver) = WorkStealingExecutor::new(4, |x: i32| x * x);

    for i in 0..10 {
        executor.submit(i);
    }

    let mut results = Vec::new();
    for _ in 0..10 {
        if let Ok(result) = receiver.recv_timeout(Duration::from_millis(500)) {
            results.push(result);
        }
    }

    drop(executor);

    assert_eq!(results.len(), 10);
    results.sort();
    assert_eq!(results, vec![0, 1, 4, 9, 16, 25, 36, 49, 64, 81]);
}

#[test]
fn test_work_stealing_load_balancing() {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = Arc::clone(&counter);

    let scheduler = WorkStealingScheduler::new(4, move |delay: u64| {
        thread::sleep(Duration::from_micros(delay));
        counter_clone.fetch_add(1, Ordering::Relaxed);
    });

    // Submit tasks with varying delays
    for i in 0..40 {
        scheduler.submit((i % 10) as u64 * 100);
    }

    thread::sleep(Duration::from_millis(500));
    assert_eq!(counter.load(Ordering::Relaxed), 40);
}

#[test]
fn test_work_stealing_single_worker() {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = Arc::clone(&counter);

    let scheduler = WorkStealingScheduler::new(1, move |_: i32| {
        counter_clone.fetch_add(1, Ordering::Relaxed);
    });

    for i in 0..15 {
        scheduler.submit(i);
    }

    thread::sleep(Duration::from_millis(100));
    assert_eq!(counter.load(Ordering::Relaxed), 15);
}

#[test]
fn test_work_stealing_many_workers() {
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

#[test]
fn test_work_stealing_executor_string_processing() {
    let (executor, receiver) = WorkStealingExecutor::new(4, |s: String| s.len());

    let words = vec!["hello", "world", "rust", "database", "parallel"];
    for word in words {
        executor.submit(word.to_string());
    }

    thread::sleep(Duration::from_millis(100));
    drop(executor);

    let mut results = Vec::new();
    while let Ok(result) = receiver.recv_timeout(Duration::from_millis(100)) {
        results.push(result);
    }

    assert_eq!(results.len(), 5);
    results.sort();
    assert_eq!(results, vec![4, 5, 5, 8, 8]);
}

#[test]
fn test_work_stealing_concurrent_submission() {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = Arc::clone(&counter);

    let scheduler = Arc::new(WorkStealingScheduler::new(4, move |_: i32| {
        counter_clone.fetch_add(1, Ordering::Relaxed);
    }));

    let mut handles = vec![];
    for thread_id in 0..4 {
        let sched = Arc::clone(&scheduler);
        let handle = thread::spawn(move || {
            for i in 0..10 {
                sched.submit(thread_id * 10 + i);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    thread::sleep(Duration::from_millis(200));
    assert_eq!(counter.load(Ordering::Relaxed), 40);
}

#[test]
fn test_work_stealing_empty_queue() {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = Arc::clone(&counter);

    let scheduler = WorkStealingScheduler::new(2, move |_: i32| {
        counter_clone.fetch_add(1, Ordering::Relaxed);
    });

    // Don't submit any tasks
    thread::sleep(Duration::from_millis(50));
    assert_eq!(counter.load(Ordering::Relaxed), 0);
}

#[test]
fn test_work_stealing_executor_aggregation() {
    let (executor, receiver) =
        WorkStealingExecutor::new(4, |nums: Vec<i32>| nums.iter().sum::<i32>());

    executor.submit(vec![1, 2, 3]);
    executor.submit(vec![4, 5, 6]);
    executor.submit(vec![7, 8, 9]);

    thread::sleep(Duration::from_millis(100));
    drop(executor);

    let mut results = Vec::new();
    while let Ok(result) = receiver.recv_timeout(Duration::from_millis(100)) {
        results.push(result);
    }

    assert_eq!(results.len(), 3);
    results.sort();
    assert_eq!(results, vec![6, 15, 24]);
}

#[test]
fn test_work_stealing_high_throughput() {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = Arc::clone(&counter);

    let scheduler = WorkStealingScheduler::new(8, move |_: i32| {
        counter_clone.fetch_add(1, Ordering::Relaxed);
    });

    for i in 0..1000 {
        scheduler.submit(i);
    }

    thread::sleep(Duration::from_millis(500));
    assert_eq!(counter.load(Ordering::Relaxed), 1000);
}
