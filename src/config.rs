use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub logging: LoggingConfig,
    pub transaction: TransactionConfig,
    pub wal: WalConfig,
    pub performance: PerformanceConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub max_connections: usize,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct StorageConfig {
    pub data_dir: String,
    pub wal_dir: String,
    pub buffer_pool_size: usize,
    pub page_size: usize,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct LoggingConfig {
    pub level: String,
    pub scope: String,
    pub file: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TransactionConfig {
    pub timeout: u64,
    pub mvcc_enabled: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct WalConfig {
    pub segment_size: usize,
    pub compression: bool,
    pub sync_on_commit: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PerformanceConfig {
    pub worker_threads: usize,
    pub query_cache: bool,
}

impl Config {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let content = fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    pub fn default() -> Self {
        Self {
            server: ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 5433,
                max_connections: 100,
            },
            storage: StorageConfig {
                data_dir: "./data".to_string(),
                wal_dir: "./wal".to_string(),
                buffer_pool_size: 1000,
                page_size: 8192,
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                scope: "*".to_string(),
                file: None,
            },
            transaction: TransactionConfig {
                timeout: 300,
                mvcc_enabled: true,
            },
            wal: WalConfig {
                segment_size: 16,
                compression: false,
                sync_on_commit: true,
            },
            performance: PerformanceConfig {
                worker_threads: 4,
                query_cache: false,
            },
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("YAML parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.server.port, 5433);
        assert_eq!(config.storage.buffer_pool_size, 1000);
        assert_eq!(config.logging.level, "info");
    }
}
