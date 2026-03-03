use rustgres::config::{Config, PerformanceConfig};
use rustgres::executor::{ParallelConfig, ParallelExecutor};

#[test]
fn test_config_max_parallel_workers() {
    let mut config = Config::default();
    assert_eq!(config.performance.max_parallel_workers, 4);

    config.performance.max_parallel_workers = 8;
    assert_eq!(config.performance.max_parallel_workers, 8);
}

#[test]
fn test_config_max_parallel_workers_from_struct() {
    let perf_config =
        PerformanceConfig { worker_threads: 4, query_cache: false, max_parallel_workers: 16 };
    assert_eq!(perf_config.max_parallel_workers, 16);
}

#[test]
fn test_parallel_config_integration() {
    let config = Config::default();
    let parallel_config = ParallelConfig::new(config.performance.max_parallel_workers);
    assert_eq!(parallel_config.max_workers(), 4);
}

#[test]
fn test_parallel_executor_from_config() {
    let config = Config::default();
    let parallel_config = ParallelConfig::new(config.performance.max_parallel_workers);
    let executor = ParallelExecutor::new(parallel_config);
    assert_eq!(executor.max_workers(), 4);
}

#[test]
fn test_parallel_executor_update_workers() {
    let executor = ParallelExecutor::default();
    assert_eq!(executor.max_workers(), 4);

    executor.set_max_workers(8);
    assert_eq!(executor.max_workers(), 8);

    executor.set_max_workers(16);
    assert_eq!(executor.max_workers(), 16);
}

#[test]
fn test_parallel_executor_min_workers_enforcement() {
    let executor = ParallelExecutor::default();
    executor.set_max_workers(0);
    assert_eq!(executor.max_workers(), 1);
}

#[test]
fn test_parallel_config_various_values() {
    let values = vec![1, 2, 4, 8, 16, 32, 64, 128];
    for val in values {
        let config = ParallelConfig::new(val);
        assert_eq!(config.max_workers(), val);
    }
}

#[test]
fn test_parallel_executor_concurrent_access() {
    use std::sync::Arc;
    use std::thread;

    let executor = Arc::new(ParallelExecutor::default());
    let mut handles = vec![];

    for i in 1..=10 {
        let exec = Arc::clone(&executor);
        let handle = thread::spawn(move || {
            exec.set_max_workers(i);
            exec.max_workers()
        });
        handles.push(handle);
    }

    for handle in handles {
        let workers = handle.join().unwrap();
        assert!(workers >= 1 && workers <= 10);
    }
}

#[test]
fn test_config_serialization_with_max_parallel_workers() {
    let config = Config::default();
    let yaml = serde_yaml::to_string(&config).unwrap();
    assert!(yaml.contains("max_parallel_workers"));
}
