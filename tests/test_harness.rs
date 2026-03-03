use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(15433);

pub struct TestServer {
    port: u16,
    process: Child,
    _data_dir: TempDir,
    _wal_dir: TempDir,
}

impl TestServer {
    pub fn start() -> Self {
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        let data_dir = TempDir::new().expect("Failed to create temp data dir");
        let wal_dir = TempDir::new().expect("Failed to create temp WAL dir");

        // Create config file for this test instance
        let config_content = format!(
            r#"
server:
  host: "127.0.0.1"
  port: {}
  max_connections: 10

storage:
  data_dir: "{}"
  wal_dir: "{}"
  buffer_pool_size: 100
  page_size: 8192

logging:
  level: "error"
  scope: "*"

transaction:
  timeout: 300
  mvcc_enabled: true

wal:
  segment_size: 16
  compression: false
  sync_on_commit: true

performance:
  worker_threads: 2
  query_cache: false
"#,
            port,
            data_dir.path().display(),
            wal_dir.path().display()
        );

        let config_path = data_dir.path().join("config.yaml");
        std::fs::write(&config_path, config_content).expect("Failed to write config");

        let process = Command::new("./target/release/vaultgres")
            .env("VAULTGRES_CONFIG", config_path.to_str().unwrap())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("Failed to start server");

        thread::sleep(Duration::from_secs(2));

        Self { port, process, _data_dir: data_dir, _wal_dir: wal_dir }
    }

    pub fn execute_sql(&self, sql: &str) -> Result<String, String> {
        let output = Command::new("psql")
            .args([
                "-h",
                "localhost",
                "-p",
                &self.port.to_string(),
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-c",
                sql,
            ])
            .output()
            .map_err(|e| format!("Failed to execute psql: {}", e))?;

        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        } else {
            Err(String::from_utf8_lossy(&output.stderr).to_string())
        }
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        thread::sleep(Duration::from_millis(100));
        // TempDir automatically cleans up data and WAL directories
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_isolated_servers() {
        let server1 = TestServer::start();
        let server2 = TestServer::start();

        // Each server has unique port
        assert_ne!(server1.port(), server2.port());

        // Each server has isolated state
        server1.execute_sql("CREATE TABLE test1 (id INT)").unwrap();
        server2.execute_sql("CREATE TABLE test2 (id INT)").unwrap();

        // test1 table only exists in server1
        assert!(server1.execute_sql("SELECT * FROM test1").is_ok());
        assert!(server2.execute_sql("SELECT * FROM test1").is_err());

        // test2 table only exists in server2
        assert!(server2.execute_sql("SELECT * FROM test2").is_ok());
        assert!(server1.execute_sql("SELECT * FROM test2").is_err());
    }
}
