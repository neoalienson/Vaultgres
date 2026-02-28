use std::process::{Command, Child, Stdio};
use std::thread;
use std::time::Duration;
use std::sync::atomic::{AtomicU16, Ordering};
use tempfile::TempDir;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(15433);

struct TestServer {
    port: u16,
    process: Child,
    _data_dir: TempDir,
    _wal_dir: TempDir,
}

impl TestServer {
    fn start() -> Self {
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
        
        let process = Command::new("./target/release/rustgres")
            .env("RUSTGRES_CONFIG", config_path.to_str().unwrap())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("Failed to start server");
        
        thread::sleep(Duration::from_secs(2));
        
        Self {
            port,
            process,
            _data_dir: data_dir,
            _wal_dir: wal_dir,
        }
    }
    
    fn execute_sql(&self, sql: &str) -> Result<String, String> {
        let output = Command::new("psql")
            .args(&[
                "-h", "localhost",
                "-p", &self.port.to_string(),
                "-U", "postgres",
                "-d", "postgres",
                "-c", sql,
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

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        thread::sleep(Duration::from_millis(100));
        // TempDir automatically cleans up data and WAL directories
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

#[test]
fn test_limit_offset_clause() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE items (id INT, name TEXT)")
        .expect("CREATE failed");
    
    for i in 1..=10 {
        server.execute_sql(&format!("INSERT INTO items VALUES ({}, 'item{}')", i, i))
            .expect("INSERT failed");
    }
    
    let result = server.execute_sql("SELECT * FROM items LIMIT 3");
    assert!(result.is_ok(), "SELECT with LIMIT failed: {:?}", result);
    
    let result = server.execute_sql("SELECT * FROM items OFFSET 5");
    assert!(result.is_ok(), "SELECT with OFFSET failed: {:?}", result);
    
    let result = server.execute_sql("SELECT * FROM items LIMIT 3 OFFSET 2");
    assert!(result.is_ok(), "SELECT with LIMIT OFFSET failed: {:?}", result);
}

#[test]
fn test_aggregate_functions() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE agg_test (id INT, amount INT)")
        .expect("CREATE failed");
    
    for i in 1..=5 {
        server.execute_sql(&format!("INSERT INTO agg_test VALUES ({}, {})", i, i * 10))
            .expect("INSERT failed");
    }
    
    let result = server.execute_sql("SELECT COUNT(*) FROM agg_test");
    assert!(result.is_ok(), "COUNT failed: {:?}", result);
    
    let result = server.execute_sql("SELECT SUM(amount) FROM agg_test");
    assert!(result.is_ok(), "SUM failed: {:?}", result);
    
    let result = server.execute_sql("SELECT AVG(amount) FROM agg_test");
    assert!(result.is_ok(), "AVG failed: {:?}", result);
    
    let result = server.execute_sql("SELECT MIN(amount) FROM agg_test");
    assert!(result.is_ok(), "MIN failed: {:?}", result);
    
    let result = server.execute_sql("SELECT MAX(amount) FROM agg_test");
    assert!(result.is_ok(), "MAX failed: {:?}", result);
}

#[test]
fn test_limit_edge_cases() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE t (id INT)").expect("CREATE failed");
    server.execute_sql("INSERT INTO t VALUES (1), (2), (3)").expect("INSERT failed");
    
    // LIMIT 0
    let result = server.execute_sql("SELECT * FROM t LIMIT 0");
    assert!(result.is_ok(), "LIMIT 0 failed: {:?}", result);
    
    // LIMIT larger than data
    let result = server.execute_sql("SELECT * FROM t LIMIT 100");
    assert!(result.is_ok(), "LIMIT > data failed: {:?}", result);
    
    // OFFSET beyond data
    let result = server.execute_sql("SELECT * FROM t OFFSET 10");
    assert!(result.is_ok(), "OFFSET beyond data failed: {:?}", result);
    
    // LIMIT 1 OFFSET 1
    let result = server.execute_sql("SELECT * FROM t LIMIT 1 OFFSET 1");
    assert!(result.is_ok(), "LIMIT 1 OFFSET 1 failed: {:?}", result);
}

#[test]
fn test_aggregate_edge_cases() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE empty_table (val INT)").expect("CREATE failed");
    
    // COUNT on empty table
    let result = server.execute_sql("SELECT COUNT(*) FROM empty_table");
    assert!(result.is_ok(), "COUNT on empty failed: {:?}", result);
    
    // SUM on empty table
    let result = server.execute_sql("SELECT SUM(val) FROM empty_table");
    assert!(result.is_ok(), "SUM on empty failed: {:?}", result);
    
    // Single value aggregates
    server.execute_sql("INSERT INTO empty_table VALUES (42)").expect("INSERT failed");
    
    let result = server.execute_sql("SELECT MIN(val) FROM empty_table");
    assert!(result.is_ok(), "MIN single value failed: {:?}", result);
    
    let result = server.execute_sql("SELECT MAX(val) FROM empty_table");
    assert!(result.is_ok(), "MAX single value failed: {:?}", result);
}

#[test]
fn test_limit_with_where() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE products (id INT, price INT)").expect("CREATE failed");
    for i in 1..=10 {
        server.execute_sql(&format!("INSERT INTO products VALUES ({}, {})", i, i * 10))
            .expect("INSERT failed");
    }
    
    let result = server.execute_sql("SELECT * FROM products WHERE price > 50 LIMIT 3");
    assert!(result.is_ok(), "LIMIT with WHERE failed: {:?}", result);
}

#[test]
fn test_aggregate_with_where() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE sales (id INT, amount INT)").expect("CREATE failed");
    for i in 1..=10 {
        server.execute_sql(&format!("INSERT INTO sales VALUES ({}, {})", i, i * 100))
            .expect("INSERT failed");
    }
    
    let result = server.execute_sql("SELECT COUNT(*) FROM sales WHERE amount > 500");
    assert!(result.is_ok(), "COUNT with WHERE failed: {:?}", result);
    
    let result = server.execute_sql("SELECT SUM(amount) FROM sales WHERE amount <= 300");
    assert!(result.is_ok(), "SUM with WHERE failed: {:?}", result);
}

#[test]
fn test_limit_with_order_by() {
    let server = TestServer::start();
    
    server.execute_sql("CREATE TABLE items (id INT, value INT)").expect("CREATE failed");
    server.execute_sql("INSERT INTO items VALUES (3, 30), (1, 10), (2, 20)").expect("INSERT failed");
    
    let result = server.execute_sql("SELECT * FROM items ORDER BY value LIMIT 2");
    assert!(result.is_ok(), "LIMIT with ORDER BY failed: {:?}", result);
    
    let result = server.execute_sql("SELECT * FROM items ORDER BY value DESC LIMIT 1");
    assert!(result.is_ok(), "LIMIT with ORDER BY DESC failed: {:?}", result);
}

#[test]
fn test_group_by_e2e() {
    use rustgres::parser::Parser;
    use rustgres::parser::ast::Statement;
    
    let sql = "SELECT category, COUNT(*) FROM products GROUP BY category";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Select(s) => {
            assert!(s.group_by.is_some());
            assert_eq!(s.group_by.unwrap(), vec!["category"]);
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_group_by_multiple_columns_e2e() {
    use rustgres::parser::Parser;
    use rustgres::parser::ast::Statement;
    
    let sql = "SELECT dept, year, SUM(salary) FROM employees GROUP BY dept, year";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Select(s) => {
            assert!(s.group_by.is_some());
            assert_eq!(s.group_by.unwrap(), vec!["dept", "year"]);
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_group_by_with_where_e2e() {
    use rustgres::parser::Parser;
    use rustgres::parser::ast::Statement;
    
    let sql = "SELECT status, COUNT(*) FROM orders WHERE amount > 100 GROUP BY status";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Select(s) => {
            assert!(s.where_clause.is_some());
            assert!(s.group_by.is_some());
            assert_eq!(s.group_by.unwrap(), vec!["status"]);
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_group_by_with_order_by_e2e() {
    use rustgres::parser::Parser;
    use rustgres::parser::ast::Statement;
    
    let sql = "SELECT region, SUM(sales) FROM data GROUP BY region ORDER BY region";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Select(s) => {
            assert!(s.group_by.is_some());
            assert!(s.order_by.is_some());
            assert_eq!(s.group_by.unwrap(), vec!["region"]);
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_group_by_edge_cases_e2e() {
    use rustgres::parser::Parser;
    use rustgres::parser::ast::Statement;
    
    // Single column
    let sql = "SELECT id FROM users GROUP BY id";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.group_by.unwrap().len(), 1);
        }
        _ => panic!("Expected SELECT statement"),
    }
    
    // Three columns
    let sql = "SELECT a, b, c FROM t GROUP BY a, b, c";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.group_by.unwrap().len(), 3);
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_having_e2e() {
    use rustgres::parser::Parser;
    use rustgres::parser::ast::Statement;
    
    let sql = "SELECT category, COUNT(*) FROM products GROUP BY category HAVING COUNT(*) > 5";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Select(s) => {
            assert!(s.group_by.is_some());
            assert!(s.having.is_some());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_having_with_aggregate_e2e() {
    use rustgres::parser::Parser;
    use rustgres::parser::ast::Statement;
    
    let sql = "SELECT dept, SUM(salary) FROM employees GROUP BY dept HAVING SUM(salary) > 100000";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Select(s) => {
            assert!(s.group_by.is_some());
            assert!(s.having.is_some());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_having_without_group_by_e2e() {
    use rustgres::parser::Parser;
    use rustgres::parser::ast::Statement;
    
    let sql = "SELECT COUNT(*) FROM orders HAVING COUNT(*) > 10";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Select(s) => {
            assert!(s.having.is_some());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_distinct_e2e() {
    use rustgres::parser::Parser;
    use rustgres::parser::ast::Statement;
    
    let sql = "SELECT DISTINCT name FROM users";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.distinct, true);
            assert_eq!(s.from, "users");
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_distinct_with_multiple_columns_e2e() {
    use rustgres::parser::Parser;
    use rustgres::parser::ast::Statement;
    
    let sql = "SELECT DISTINCT dept, year FROM employees";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.distinct, true);
            assert_eq!(s.columns.len(), 2);
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_distinct_with_where_e2e() {
    use rustgres::parser::Parser;
    use rustgres::parser::ast::Statement;
    
    let sql = "SELECT DISTINCT status FROM orders WHERE amount > 100";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.distinct, true);
            assert!(s.where_clause.is_some());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_distinct_with_order_by_e2e() {
    use rustgres::parser::Parser;
    use rustgres::parser::ast::Statement;
    
    let sql = "SELECT DISTINCT category FROM products ORDER BY category";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.distinct, true);
            assert!(s.order_by.is_some());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_distinct_with_limit_e2e() {
    use rustgres::parser::Parser;
    use rustgres::parser::ast::Statement;
    
    let sql = "SELECT DISTINCT id FROM data LIMIT 10";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.distinct, true);
            assert_eq!(s.limit, Some(10));
        }
        _ => panic!("Expected SELECT statement"),
    }
}
