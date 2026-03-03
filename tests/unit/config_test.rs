use vaultgres::config::{Config, LoggingConfig, ServerConfig, StorageConfig};

#[test]
fn test_default_config_values() {
    let config = Config::default();

    assert_eq!(config.server.host, "127.0.0.1");
    assert_eq!(config.server.port, 5433);
    assert_eq!(config.server.max_connections, 100);

    assert_eq!(config.storage.data_dir, "./data");
    assert_eq!(config.storage.wal_dir, "./wal");
    assert_eq!(config.storage.buffer_pool_size, 1000);
    assert_eq!(config.storage.page_size, 8192);

    assert_eq!(config.logging.level, "info");
    assert_eq!(config.logging.scope, "*");
    assert!(config.logging.file.is_none());
}

#[test]
fn test_server_config() {
    let server = ServerConfig { host: "0.0.0.0".to_string(), port: 5432, max_connections: 200 };

    assert_eq!(server.host, "0.0.0.0");
    assert_eq!(server.port, 5432);
    assert_eq!(server.max_connections, 200);
}

#[test]
fn test_storage_config() {
    let storage = StorageConfig {
        data_dir: "/var/data".to_string(),
        wal_dir: "/var/wal".to_string(),
        buffer_pool_size: 5000,
        page_size: 8192,
    };

    assert_eq!(storage.data_dir, "/var/data");
    assert_eq!(storage.wal_dir, "/var/wal");
    assert_eq!(storage.buffer_pool_size, 5000);
}

#[test]
fn test_logging_config() {
    let logging = LoggingConfig {
        level: "debug".to_string(),
        scope: "protocol,parser".to_string(),
        file: Some("/var/log/vaultgres.log".to_string()),
    };

    assert_eq!(logging.level, "debug");
    assert_eq!(logging.scope, "protocol,parser");
    assert!(logging.file.is_some());
}

#[test]
fn test_config_clone() {
    let config1 = Config::default();
    let config2 = config1.clone();

    assert_eq!(config1.server.port, config2.server.port);
    assert_eq!(config1.storage.data_dir, config2.storage.data_dir);
}
