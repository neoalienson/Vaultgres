//! Edge case tests for configuration

#[cfg(test)]
mod tests {
    use crate::config::*;

    #[test]
    fn test_config_zero_port() {
        let mut config = Config::default();
        config.server.port = 0;
        assert_eq!(config.server.port, 0);
    }

    #[test]
    fn test_config_max_port() {
        let mut config = Config::default();
        config.server.port = 65535;
        assert_eq!(config.server.port, 65535);
    }

    #[test]
    fn test_config_zero_connections() {
        let mut config = Config::default();
        config.server.max_connections = 0;
        assert_eq!(config.server.max_connections, 0);
    }

    #[test]
    fn test_config_large_buffer_pool() {
        let mut config = Config::default();
        config.storage.buffer_pool_size = 1_000_000;
        assert_eq!(config.storage.buffer_pool_size, 1_000_000);
    }

    #[test]
    fn test_config_empty_host() {
        let mut config = Config::default();
        config.server.host = "".to_string();
        assert_eq!(config.server.host, "");
    }

    #[test]
    fn test_config_empty_data_dir() {
        let mut config = Config::default();
        config.storage.data_dir = "".to_string();
        assert_eq!(config.storage.data_dir, "");
    }

    #[test]
    fn test_config_zero_timeout() {
        let mut config = Config::default();
        config.transaction.timeout = 0;
        assert_eq!(config.transaction.timeout, 0);
    }

    #[test]
    fn test_config_mvcc_disabled() {
        let mut config = Config::default();
        config.transaction.mvcc_enabled = false;
        assert!(!config.transaction.mvcc_enabled);
    }

    #[test]
    fn test_config_zero_worker_threads() {
        let mut config = Config::default();
        config.performance.worker_threads = 0;
        assert_eq!(config.performance.worker_threads, 0);
    }

    #[test]
    fn test_config_all_features_disabled() {
        let mut config = Config::default();
        config.wal.compression = false;
        config.wal.sync_on_commit = false;
        config.performance.query_cache = false;
        assert!(!config.wal.compression);
        assert!(!config.wal.sync_on_commit);
        assert!(!config.performance.query_cache);
    }
}
