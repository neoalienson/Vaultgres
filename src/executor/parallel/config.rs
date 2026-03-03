use parking_lot::RwLock;
use std::sync::Arc;

#[derive(Clone)]
pub struct ParallelConfig {
    max_workers: usize,
}

impl ParallelConfig {
    pub fn new(max_workers: usize) -> Self {
        Self { max_workers: max_workers.max(1) }
    }

    pub fn max_workers(&self) -> usize {
        self.max_workers
    }

    pub fn set_max_workers(&mut self, max_workers: usize) {
        self.max_workers = max_workers.max(1);
    }
}

impl Default for ParallelConfig {
    fn default() -> Self {
        Self::new(4)
    }
}

pub struct ParallelExecutor {
    config: Arc<RwLock<ParallelConfig>>,
}

impl ParallelExecutor {
    pub fn new(config: ParallelConfig) -> Self {
        Self { config: Arc::new(RwLock::new(config)) }
    }

    pub fn max_workers(&self) -> usize {
        self.config.read().max_workers()
    }

    pub fn set_max_workers(&self, max_workers: usize) {
        self.config.write().set_max_workers(max_workers);
    }
}

impl Default for ParallelExecutor {
    fn default() -> Self {
        Self::new(ParallelConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parallel_config_new() {
        let config = ParallelConfig::new(8);
        assert_eq!(config.max_workers(), 8);
    }

    #[test]
    fn test_parallel_config_min_workers() {
        let config = ParallelConfig::new(0);
        assert_eq!(config.max_workers(), 1);
    }

    #[test]
    fn test_parallel_config_set_max_workers() {
        let mut config = ParallelConfig::new(4);
        config.set_max_workers(16);
        assert_eq!(config.max_workers(), 16);
    }

    #[test]
    fn test_parallel_config_set_zero_workers() {
        let mut config = ParallelConfig::new(4);
        config.set_max_workers(0);
        assert_eq!(config.max_workers(), 1);
    }

    #[test]
    fn test_parallel_config_default() {
        let config = ParallelConfig::default();
        assert_eq!(config.max_workers(), 4);
    }

    #[test]
    fn test_parallel_executor_new() {
        let config = ParallelConfig::new(8);
        let executor = ParallelExecutor::new(config);
        assert_eq!(executor.max_workers(), 8);
    }

    #[test]
    fn test_parallel_executor_set_max_workers() {
        let executor = ParallelExecutor::default();
        assert_eq!(executor.max_workers(), 4);
        executor.set_max_workers(12);
        assert_eq!(executor.max_workers(), 12);
    }

    #[test]
    fn test_parallel_executor_default() {
        let executor = ParallelExecutor::default();
        assert_eq!(executor.max_workers(), 4);
    }
}
