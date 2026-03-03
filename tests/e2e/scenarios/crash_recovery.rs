use e2e::*;

#[test]
fn test_crash_recovery_basic() {
    let env = TestEnv::new()
        .with_vaultgres()
        .with_persistence()
        .start();
    
    let db = env.vaultgres();
    db.execute("CREATE TABLE crash_test (id INT, data TEXT)").unwrap();
    db.execute("INSERT INTO crash_test VALUES (1, 'before crash')").unwrap();
    
    env.kill_container();
    env.restart();
    
    let result = db.execute("SELECT * FROM crash_test").unwrap();
    assert!(result.contains("before crash"), "Data lost after crash");
}

#[test]
fn test_wal_recovery() {
    let env = TestEnv::new()
        .with_vaultgres()
        .with_persistence()
        .start();
    
    let db = env.vaultgres();
    db.execute("CREATE TABLE wal_test (id INT)").unwrap();
    
    for i in 0..1000 {
        db.execute(&format!("INSERT INTO wal_test VALUES ({})", i)).unwrap();
    }
    
    env.kill_container();
    env.restart();
    
    let count: i32 = db.query_scalar("SELECT COUNT(*) FROM wal_test");
    assert_eq!(count, 1000, "WAL recovery incomplete");
}

#[test]
fn test_multiple_crash_recovery() {
    let env = TestEnv::new()
        .with_vaultgres()
        .with_persistence()
        .start();
    
    let db = env.vaultgres();
    db.execute("CREATE TABLE multi_crash (id INT, value INT)").unwrap();
    
    for cycle in 0..5 {
        db.execute(&format!("INSERT INTO multi_crash VALUES ({}, {})", cycle, cycle * 100)).unwrap();
        env.kill_container();
        env.restart();
    }
    
    let count: i32 = db.query_scalar("SELECT COUNT(*) FROM multi_crash");
    assert_eq!(count, 5, "Data lost across multiple crashes");
}

#[test]
fn test_view_persistence() {
    let env = TestEnv::new()
        .with_vaultgres()
        .with_persistence()
        .start();
    
    let db = env.vaultgres();
    db.execute("CREATE TABLE products (id INT, price INT)").unwrap();
    db.execute("INSERT INTO products VALUES (1, 100), (2, 200)").unwrap();
    db.execute("CREATE VIEW expensive_products AS SELECT * FROM products WHERE price > 150").unwrap();
    
    env.kill_container();
    env.restart();
    
    let result = db.execute("SELECT * FROM expensive_products").unwrap();
    assert!(result.contains("200"), "View lost after restart");
}

#[test]
fn test_index_persistence() {
    let env = TestEnv::new()
        .with_vaultgres()
        .with_persistence()
        .start();
    
    let db = env.vaultgres();
    db.execute("CREATE TABLE users (id INT, email TEXT)").unwrap();
    db.execute("CREATE INDEX idx_email ON users(email)").unwrap();
    db.execute("INSERT INTO users VALUES (1, 'test@example.com')").unwrap();
    
    env.kill_container();
    env.restart();
    
    let result = db.execute("SELECT * FROM users WHERE email = 'test@example.com'").unwrap();
    assert!(result.contains("test@example.com"), "Index lost after restart");
}

#[test]
fn test_function_persistence() {
    let env = TestEnv::new()
        .with_vaultgres()
        .with_persistence()
        .start();
    
    let db = env.vaultgres();
    db.execute("CREATE FUNCTION add_ten(x INT) RETURNS INT AS 'SELECT x + 10' LANGUAGE SQL").unwrap();
    
    env.kill_container();
    env.restart();
    
    let result = db.execute("SELECT add_ten(5)").unwrap();
    assert!(result.contains("15"), "Function lost after restart");
}

#[test]
fn test_trigger_persistence() {
    let env = TestEnv::new()
        .with_vaultgres()
        .with_persistence()
        .start();
    
    let db = env.vaultgres();
    db.execute("CREATE TABLE audit_log (id INT, action TEXT)").unwrap();
    db.execute("CREATE TABLE orders (id INT, total INT)").unwrap();
    db.execute("CREATE TRIGGER log_insert AFTER INSERT ON orders FOR EACH ROW INSERT INTO audit_log VALUES (NEW.id, 'inserted')").unwrap();
    
    env.kill_container();
    env.restart();
    
    db.execute("INSERT INTO orders VALUES (1, 100)").unwrap();
    let result = db.execute("SELECT * FROM audit_log").unwrap();
    assert!(result.contains("inserted"), "Trigger lost after restart");
}

#[test]
fn test_constraint_persistence() {
    let env = TestEnv::new()
        .with_vaultgres()
        .with_persistence()
        .start();
    
    let db = env.vaultgres();
    db.execute("CREATE TABLE accounts (id INT PRIMARY KEY, balance INT CHECK (balance >= 0))").unwrap();
    db.execute("INSERT INTO accounts VALUES (1, 100)").unwrap();
    
    env.kill_container();
    env.restart();
    
    let result = db.execute("INSERT INTO accounts VALUES (2, -50)");
    assert!(result.is_err(), "CHECK constraint lost after restart");
}
