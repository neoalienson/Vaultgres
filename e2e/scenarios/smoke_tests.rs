use e2e::*;

#[test]
fn test_basic_create_table() {
    eprintln!("\n=== Test: Basic CREATE TABLE ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute("CREATE TABLE test (id INT, name TEXT)");
    assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_basic_insert_select() {
    eprintln!("\n=== Test: Basic INSERT/SELECT ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE users (id INT, name TEXT)").unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    
    let result = db.execute("SELECT * FROM users");
    assert!(result.is_ok(), "SELECT failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_multiple_inserts() {
    eprintln!("\n=== Test: Multiple INSERTs ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE products (id INT, name TEXT)").unwrap();
    
    for i in 0..10 {
        eprintln!("[Test] Inserting row {}", i);
        let result = db.execute(&format!("INSERT INTO products VALUES ({}, 'product{}')", i, i));
        assert!(result.is_ok(), "INSERT {} failed", i);
    }
    
    let result = db.execute("SELECT * FROM products");
    assert!(result.is_ok());
    eprintln!("=== Test PASSED ===");
}
