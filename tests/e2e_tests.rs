use std::process::{Command, Child, Stdio};
use std::thread;
use std::time::Duration;
use std::sync::Mutex;

static TEST_LOCK: Mutex<()> = Mutex::new(());

struct TestServer {
    process: Child,
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl TestServer {
    fn start() -> Self {
        let lock = TEST_LOCK.lock().unwrap();
        
        let process = Command::new("./target/release/rustgres")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("Failed to start server");
        
        thread::sleep(Duration::from_secs(2));
        
        Self { process, _lock: lock }
    }
    
    fn execute_sql(&self, sql: &str) -> Result<String, String> {
        let output = Command::new("psql")
            .args(&["-h", "localhost", "-p", "5433", "-U", "postgres", "-d", "postgres", "-c", sql])
            .output()
            .map_err(|e| format!("Failed to execute psql: {}", e))?;
        
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        } else {
            Err(String::from_utf8_lossy(&output.stderr).to_string())
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

#[test]
fn test_create_table() {
    let server = TestServer::start();
    
    let result = server.execute_sql("CREATE TABLE users (id INT, name TEXT)");
    assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result);
    assert!(result.unwrap().contains("CREATE TABLE"));
    
    let result = server.execute_sql("CREATE TABLE users (id INT)");
    assert!(result.is_err(), "Duplicate table should fail");
    assert!(result.unwrap_err().contains("already exists"));
}

#[test]
fn test_drop_table() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE products (id INT, name TEXT)")
        .expect("CREATE TABLE failed");
    
    let result = server.execute_sql("DROP TABLE products");
    assert!(result.is_ok(), "DROP TABLE failed");
    assert!(result.unwrap().contains("DROP TABLE"));
    
    let result = server.execute_sql("DROP TABLE products");
    assert!(result.is_err(), "Drop non-existent table should fail");
    assert!(result.unwrap_err().contains("does not exist"));
}

#[test]
fn test_drop_table_if_exists() {
    let server = TestServer::start();
    
    let result = server.execute_sql("DROP TABLE IF EXISTS nonexistent");
    assert!(result.is_ok(), "DROP TABLE IF EXISTS should not fail: {:?}", result);
}

#[test]
fn test_ddl_workflow() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE test (id INT, data TEXT)")
        .expect("CREATE TABLE failed");
    
    server.execute_sql("DROP TABLE test")
        .expect("DROP TABLE failed");
    
    server.execute_sql("CREATE TABLE test (id INT, value INT)")
        .expect("CREATE TABLE failed");
    
    server.execute_sql("DROP TABLE IF EXISTS test")
        .expect("DROP TABLE IF EXISTS failed");
}
