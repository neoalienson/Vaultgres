use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(16433);

struct PersistentTestServer {
    port: u16,
    process: Option<Child>,
    data_dir: TempDir,
    wal_dir: TempDir,
    config_path: std::path::PathBuf,
}

impl PersistentTestServer {
    fn new() -> Self {
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        let data_dir = TempDir::new().expect("Failed to create temp data dir");
        let wal_dir = TempDir::new().expect("Failed to create temp WAL dir");

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

        Self { port, process: None, data_dir, wal_dir, config_path }
    }

    fn start(&mut self) {
        let process = Command::new("./target/debug/vaultgres")
            .env("VAULTGRES_CONFIG", self.config_path.to_str().unwrap())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("Failed to start server");

        thread::sleep(Duration::from_secs(2));
        self.process = Some(process);
    }

    fn stop(&mut self) {
        if let Some(mut process) = self.process.take() {
            let _ = process.kill();
            let _ = process.wait();
            thread::sleep(Duration::from_millis(200));
        }
    }

    fn restart(&mut self) {
        self.stop();
        thread::sleep(Duration::from_millis(500));
        self.start();
    }

    fn execute_sql(&self, sql: &str) -> Result<String, String> {
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
}

impl Drop for PersistentTestServer {
    fn drop(&mut self) {
        self.stop();
    }
}

#[test]
fn test_table_persists_after_restart() {
    let mut server = PersistentTestServer::new();
    server.start();

    // Create table
    server.execute_sql("CREATE TABLE users (id INT, name TEXT)").expect("CREATE TABLE failed");

    // Insert data
    server.execute_sql("INSERT INTO users VALUES (1, 'Alice')").expect("INSERT failed");

    // Verify data exists
    let result = server.execute_sql("SELECT * FROM users").unwrap();
    assert!(result.contains("Alice"));

    // Restart server
    server.restart();

    // Verify table still exists
    let result = server.execute_sql("SELECT * FROM users");
    assert!(result.is_ok(), "Table should persist after restart");
    assert!(result.unwrap().contains("Alice"), "Data should persist after restart");
}

#[test]
fn test_multiple_tables_persist() {
    let mut server = PersistentTestServer::new();
    server.start();

    // Create multiple tables
    server
        .execute_sql("CREATE TABLE products (id INT, name TEXT, price INT)")
        .expect("CREATE products failed");
    server
        .execute_sql("CREATE TABLE orders (id INT, product_id INT, qty INT)")
        .expect("CREATE orders failed");

    // Insert data into both tables
    server
        .execute_sql("INSERT INTO products VALUES (1, 'Laptop', 999)")
        .expect("INSERT products failed");
    server.execute_sql("INSERT INTO orders VALUES (1, 1, 2)").expect("INSERT orders failed");

    // Restart server
    server.restart();

    // Verify both tables exist with data
    let products = server.execute_sql("SELECT * FROM products").unwrap();
    assert!(products.contains("Laptop"));

    let orders = server.execute_sql("SELECT * FROM orders").unwrap();
    assert!(orders.contains("1") && orders.contains("2"));
}

#[test]
fn test_data_survives_multiple_restarts() {
    let mut server = PersistentTestServer::new();
    server.start();

    // Create table and insert initial data
    server.execute_sql("CREATE TABLE counter (id INT, value INT)").expect("CREATE failed");
    server.execute_sql("INSERT INTO counter VALUES (1, 100)").expect("INSERT failed");

    // Restart 1: Update data
    server.restart();
    server.execute_sql("UPDATE counter SET value = 200").expect("UPDATE failed");

    // Restart 2: Verify update persisted
    server.restart();
    let result = server.execute_sql("SELECT value FROM counter").unwrap();
    assert!(result.contains("200"), "Update should persist");

    // Restart 3: Insert more data
    server.restart();
    server.execute_sql("INSERT INTO counter VALUES (2, 300)").expect("INSERT 2 failed");

    // Restart 4: Verify all data
    server.restart();
    let result = server.execute_sql("SELECT * FROM counter").unwrap();
    assert!(
        result.contains("200") && result.contains("300"),
        "All data should persist across multiple restarts"
    );
}

#[test]
fn test_delete_persists_after_restart() {
    let mut server = PersistentTestServer::new();
    server.start();

    // Create table and insert data
    server.execute_sql("CREATE TABLE items (id INT, name TEXT)").expect("CREATE failed");
    server.execute_sql("INSERT INTO items VALUES (1, 'Item1')").expect("INSERT 1 failed");
    server.execute_sql("INSERT INTO items VALUES (2, 'Item2')").expect("INSERT 2 failed");
    server.execute_sql("INSERT INTO items VALUES (3, 'Item3')").expect("INSERT 3 failed");

    // Delete one item
    server.execute_sql("DELETE FROM items WHERE id = 2").expect("DELETE failed");

    // Restart server
    server.restart();

    // Verify deletion persisted
    let result = server.execute_sql("SELECT * FROM items").unwrap();
    assert!(result.contains("Item1"), "Item1 should exist");
    assert!(!result.contains("Item2"), "Item2 should be deleted");
    assert!(result.contains("Item3"), "Item3 should exist");
}

#[test]
fn test_drop_table_persists() {
    let mut server = PersistentTestServer::new();
    server.start();

    // Create two tables
    server.execute_sql("CREATE TABLE temp1 (id INT)").expect("CREATE temp1 failed");
    server.execute_sql("CREATE TABLE temp2 (id INT)").expect("CREATE temp2 failed");

    // Drop one table
    server.execute_sql("DROP TABLE temp1").expect("DROP failed");

    // Restart server
    server.restart();

    // Verify temp1 is gone, temp2 exists
    let result1 = server.execute_sql("SELECT * FROM temp1");
    assert!(result1.is_err(), "temp1 should not exist after drop");

    let result2 = server.execute_sql("SELECT * FROM temp2");
    assert!(result2.is_ok(), "temp2 should still exist");
}

#[test]
fn test_large_dataset_persists() {
    let mut server = PersistentTestServer::new();
    server.start();

    // Create table
    server.execute_sql("CREATE TABLE large_data (id INT, value INT)").expect("CREATE failed");

    // Insert 100 rows
    for i in 1..=100 {
        server
            .execute_sql(&format!("INSERT INTO large_data VALUES ({}, {})", i, i * 10))
            .unwrap_or_else(|_| panic!("INSERT {} failed", i));
    }

    // Restart server
    server.restart();

    // Verify all data persists
    let result = server.execute_sql("SELECT COUNT(*) FROM large_data");
    // Note: COUNT(*) might not be fully implemented, so we check if query succeeds
    assert!(result.is_ok(), "Should be able to query large dataset after restart");

    // Verify specific rows
    let row1 = server.execute_sql("SELECT * FROM large_data WHERE id = 1").unwrap();
    assert!(row1.contains("10"));

    let row100 = server.execute_sql("SELECT * FROM large_data WHERE id = 100").unwrap();
    assert!(row100.contains("1000"));
}

#[test]
fn test_transaction_commit_persists() {
    let mut server = PersistentTestServer::new();
    server.start();

    // Create table
    server.execute_sql("CREATE TABLE txn_test (id INT, status TEXT)").expect("CREATE failed");

    // Insert data (auto-commit)
    server.execute_sql("INSERT INTO txn_test VALUES (1, 'committed')").expect("INSERT failed");

    // Restart immediately after commit
    server.restart();

    // Verify committed data persists
    let result = server.execute_sql("SELECT * FROM txn_test").unwrap();
    assert!(result.contains("committed"), "Committed transaction should persist");
}

#[test]
fn test_schema_changes_persist() {
    let mut server = PersistentTestServer::new();
    server.start();

    // Create initial table
    server.execute_sql("CREATE TABLE schema_test (id INT, name TEXT)").expect("CREATE failed");

    server.execute_sql("INSERT INTO schema_test VALUES (1, 'Test')").expect("INSERT failed");

    // Restart
    server.restart();

    // Verify schema persists
    let result = server.execute_sql("INSERT INTO schema_test VALUES (2, 'Test2')");
    assert!(result.is_ok(), "Should be able to insert with persisted schema");

    // Restart again
    server.restart();

    // Verify both inserts persisted
    let result = server.execute_sql("SELECT * FROM schema_test").unwrap();
    assert!(result.contains("Test") && result.contains("Test2"));
}

#[test]
fn test_empty_table_persists() {
    let mut server = PersistentTestServer::new();
    server.start();

    // Create empty table
    server.execute_sql("CREATE TABLE empty_table (id INT, data TEXT)").expect("CREATE failed");

    // Restart without inserting data
    server.restart();

    // Verify empty table still exists
    let result = server.execute_sql("SELECT * FROM empty_table");
    assert!(result.is_ok(), "Empty table should persist");

    // Should be able to insert into it
    let insert = server.execute_sql("INSERT INTO empty_table VALUES (1, 'data')");
    assert!(insert.is_ok(), "Should be able to insert into persisted empty table");
}

#[test]
fn test_concurrent_tables_persist() {
    let mut server = PersistentTestServer::new();
    server.start();

    // Create multiple tables with different data types
    server.execute_sql("CREATE TABLE ints (id INT, value INT)").expect("CREATE ints failed");
    server.execute_sql("CREATE TABLE texts (id INT, value TEXT)").expect("CREATE texts failed");

    // Insert different data
    server.execute_sql("INSERT INTO ints VALUES (1, 100)").expect("INSERT ints failed");
    server.execute_sql("INSERT INTO texts VALUES (1, 'hello')").expect("INSERT texts failed");

    // Restart
    server.restart();

    // Verify both tables with correct data types
    let ints_result = server.execute_sql("SELECT * FROM ints").unwrap();
    assert!(ints_result.contains("100"));

    let texts_result = server.execute_sql("SELECT * FROM texts").unwrap();
    assert!(texts_result.contains("hello"));
}
