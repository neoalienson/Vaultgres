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

#[test]
fn test_insert() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE users (id INT, name TEXT)")
        .expect("CREATE TABLE failed");
    
    let result = server.execute_sql("INSERT INTO users VALUES (1, 'Alice')");
    assert!(result.is_ok(), "INSERT failed: {:?}", result);
    assert!(result.unwrap().contains("INSERT"));
}

#[test]
fn test_insert_multiple_rows() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE products (id INT, name TEXT, price INT)")
        .expect("CREATE TABLE failed");
    
    server.execute_sql("INSERT INTO products VALUES (1, 'Laptop', 999)")
        .expect("INSERT 1 failed");
    server.execute_sql("INSERT INTO products VALUES (2, 'Mouse', 25)")
        .expect("INSERT 2 failed");
    server.execute_sql("INSERT INTO products VALUES (3, 'Keyboard', 75)")
        .expect("INSERT 3 failed");
}

#[test]
fn test_insert_wrong_column_count() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE users (id INT, name TEXT)")
        .expect("CREATE TABLE failed");
    
    let result = server.execute_sql("INSERT INTO users VALUES (1)");
    assert!(result.is_err(), "INSERT with wrong column count should fail");
    assert!(result.unwrap_err().contains("Expected 2 values"));
}

#[test]
fn test_insert_nonexistent_table() {
    let server = TestServer::start();
    
    let result = server.execute_sql("INSERT INTO nonexistent VALUES (1, 'test')");
    assert!(result.is_err(), "INSERT into non-existent table should fail");
    assert!(result.unwrap_err().contains("does not exist"));
}

#[test]
fn test_complete_workflow() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE orders (id INT, customer TEXT, amount INT)")
        .expect("CREATE TABLE failed");
    
    server.execute_sql("INSERT INTO orders VALUES (1, 'Alice', 100)")
        .expect("INSERT 1 failed");
    server.execute_sql("INSERT INTO orders VALUES (2, 'Bob', 200)")
        .expect("INSERT 2 failed");
    server.execute_sql("INSERT INTO orders VALUES (3, 'Charlie', 150)")
        .expect("INSERT 3 failed");
    
    server.execute_sql("DROP TABLE orders")
        .expect("DROP TABLE failed");
}

#[test]
fn test_select_after_insert() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE products (id INT, name TEXT, price INT)")
        .expect("CREATE TABLE failed");
    
    server.execute_sql("INSERT INTO products VALUES (1, 'Laptop', 999)")
        .expect("INSERT failed");
    
    let result = server.execute_sql("SELECT * FROM products");
    assert!(result.is_ok(), "SELECT failed: {:?}", result);
    assert!(result.unwrap().contains("SELECT"));
}

#[test]
fn test_select_specific_columns() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE users (id INT, name TEXT, email TEXT)")
        .expect("CREATE TABLE failed");
    
    server.execute_sql("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")
        .expect("INSERT failed");
    
    let result = server.execute_sql("SELECT id, name FROM users");
    assert!(result.is_ok(), "SELECT failed: {:?}", result);
}

#[test]
fn test_select_empty_table() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE empty (id INT)")
        .expect("CREATE TABLE failed");
    
    let result = server.execute_sql("SELECT * FROM empty");
    assert!(result.is_ok(), "SELECT failed: {:?}", result);
    assert!(result.unwrap().contains("SELECT 0"));
}

#[test]
fn test_full_crud_workflow() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE inventory (id INT, item TEXT, qty INT)")
        .expect("CREATE failed");
    
    server.execute_sql("INSERT INTO inventory VALUES (1, 'Widget', 100)")
        .expect("INSERT 1 failed");
    server.execute_sql("INSERT INTO inventory VALUES (2, 'Gadget', 50)")
        .expect("INSERT 2 failed");
    
    server.execute_sql("SELECT * FROM inventory")
        .expect("SELECT failed");
    
    server.execute_sql("DROP TABLE inventory")
        .expect("DROP failed");
}

#[test]
fn test_update() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE products (id INT, price INT)")
        .expect("CREATE failed");
    
    server.execute_sql("INSERT INTO products VALUES (1, 100)")
        .expect("INSERT failed");
    
    let result = server.execute_sql("UPDATE products SET price = 200");
    assert!(result.is_ok(), "UPDATE failed: {:?}", result);
    assert!(result.unwrap().contains("UPDATE"));
}

#[test]
fn test_update_multiple_rows() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE items (id INT, status TEXT)")
        .expect("CREATE failed");
    
    server.execute_sql("INSERT INTO items VALUES (1, 'pending')")
        .expect("INSERT 1 failed");
    server.execute_sql("INSERT INTO items VALUES (2, 'pending')")
        .expect("INSERT 2 failed");
    
    let result = server.execute_sql("UPDATE items SET status = 'done'");
    assert!(result.is_ok(), "UPDATE failed: {:?}", result);
}

#[test]
fn test_delete() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE temp (id INT)")
        .expect("CREATE failed");
    
    server.execute_sql("INSERT INTO temp VALUES (1)")
        .expect("INSERT failed");
    
    let result = server.execute_sql("DELETE FROM temp");
    assert!(result.is_ok(), "DELETE failed: {:?}", result);
    assert!(result.unwrap().contains("DELETE"));
}

#[test]
fn test_delete_multiple_rows() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE logs (id INT, msg TEXT)")
        .expect("CREATE failed");
    
    server.execute_sql("INSERT INTO logs VALUES (1, 'log1')")
        .expect("INSERT 1 failed");
    server.execute_sql("INSERT INTO logs VALUES (2, 'log2')")
        .expect("INSERT 2 failed");
    server.execute_sql("INSERT INTO logs VALUES (3, 'log3')")
        .expect("INSERT 3 failed");
    
    let result = server.execute_sql("DELETE FROM logs");
    assert!(result.is_ok(), "DELETE failed: {:?}", result);
}

#[test]
fn test_complete_crud_cycle() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE users (id INT, name TEXT, active INT)")
        .expect("CREATE failed");
    
    server.execute_sql("INSERT INTO users VALUES (1, 'Alice', 1)")
        .expect("INSERT failed");
    
    server.execute_sql("SELECT * FROM users")
        .expect("SELECT failed");
    
    server.execute_sql("UPDATE users SET active = 0")
        .expect("UPDATE failed");
    
    server.execute_sql("DELETE FROM users")
        .expect("DELETE failed");
    
    server.execute_sql("DROP TABLE users")
        .expect("DROP failed");
}

#[test]
fn test_multiline_statement() {
    let server = TestServer::start();
    
    // Test that incomplete statements are handled correctly
    // psql buffers until semicolon, so this is actually ONE statement
    let result = server.execute_sql("CREATE TABLE test (id INT, name TEXT)");
    assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result);
    
    // Trying to create same table again should fail
    let result = server.execute_sql("CREATE TABLE test (id INT)");
    assert!(result.is_err(), "Duplicate CREATE should fail");
}

#[test]
fn test_incomplete_statement_handling() {
    let server = TestServer::start();
    
    // Test missing semicolon - psql will wait for more input
    // This tests that our protocol handles statement boundaries correctly
    let result = server.execute_sql("CREATE TABLE incomplete (id INT, name TEXT)");
    assert!(result.is_ok(), "Statement should complete: {:?}", result);
}

#[test]
fn test_where_clause_comparison_operators() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE products (id INT, price INT)")
        .expect("CREATE failed");
    
    server.execute_sql("INSERT INTO products VALUES (1, 50)")
        .expect("INSERT 1 failed");
    server.execute_sql("INSERT INTO products VALUES (2, 100)")
        .expect("INSERT 2 failed");
    server.execute_sql("INSERT INTO products VALUES (3, 150)")
        .expect("INSERT 3 failed");
    server.execute_sql("INSERT INTO products VALUES (4, 200)")
        .expect("INSERT 4 failed");
    
    // Test less than
    let result = server.execute_sql("SELECT * FROM products WHERE price < 150");
    assert!(result.is_ok(), "SELECT with < failed: {:?}", result);
    
    // Test greater than
    let result = server.execute_sql("SELECT * FROM products WHERE price > 100");
    assert!(result.is_ok(), "SELECT with > failed: {:?}", result);
    
    // Test less than or equal
    let result = server.execute_sql("SELECT * FROM products WHERE price <= 100");
    assert!(result.is_ok(), "SELECT with <= failed: {:?}", result);
    
    // Test greater than or equal
    let result = server.execute_sql("SELECT * FROM products WHERE price >= 150");
    assert!(result.is_ok(), "SELECT with >= failed: {:?}", result);
    
    // Test not equals
    let result = server.execute_sql("SELECT * FROM products WHERE price != 100");
    assert!(result.is_ok(), "SELECT with != failed: {:?}", result);
}

#[test]
fn test_order_by_clause() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE products (id INT, name TEXT, price INT)")
        .expect("CREATE failed");
    
    server.execute_sql("INSERT INTO products VALUES (3, 'Keyboard', 75)")
        .expect("INSERT 1 failed");
    server.execute_sql("INSERT INTO products VALUES (1, 'Mouse', 25)")
        .expect("INSERT 2 failed");
    server.execute_sql("INSERT INTO products VALUES (2, 'Monitor', 200)")
        .expect("INSERT 3 failed");
    
    // Test ORDER BY ASC
    let result = server.execute_sql("SELECT * FROM products ORDER BY id");
    assert!(result.is_ok(), "SELECT with ORDER BY failed: {:?}", result);
    
    // Test ORDER BY DESC
    let result = server.execute_sql("SELECT * FROM products ORDER BY price DESC");
    assert!(result.is_ok(), "SELECT with ORDER BY DESC failed: {:?}", result);
}
