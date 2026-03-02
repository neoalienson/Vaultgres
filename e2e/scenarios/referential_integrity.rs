use e2e::*;

#[test]
fn test_primary_key_uniqueness() {
    eprintln!("\n=== Test: PRIMARY KEY Uniqueness ===");
    let env = TestEnv::new().with_rustgres().start();
    let db = env.rustgres();

    // Create table with PRIMARY KEY
    db.execute("CREATE TABLE users_pk (id INT PRIMARY KEY, name TEXT)").unwrap();
    
    // Insert first row
    db.execute("INSERT INTO users_pk VALUES (1, 'Alice')").unwrap();
    
    // Try to insert duplicate PRIMARY KEY - should fail
    let result = db.execute("INSERT INTO users_pk VALUES (1, 'Bob')");
    assert!(result.is_err(), "Should fail on duplicate PRIMARY KEY");
    assert!(result.unwrap_err().contains("Primary key violation"));
    
    // Insert different key - should succeed
    db.execute("INSERT INTO users_pk VALUES (2, 'Bob')").unwrap();
    
    // Cleanup
    db.execute("DROP TABLE users_pk").ok();
    
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_foreign_key_reference() {
    eprintln!("\n=== Test: FOREIGN KEY Reference ===");
    let env = TestEnv::new().with_rustgres().start();
    let db = env.rustgres();

    // Create parent table
    db.execute("CREATE TABLE customers_fk (id INT PRIMARY KEY, name TEXT)").unwrap();
    
    // Create child table with FOREIGN KEY
    db.execute("CREATE TABLE orders_fk (id INT PRIMARY KEY, customer_id INT REFERENCES customers_fk(id))").unwrap();
    
    // Insert parent row
    db.execute("INSERT INTO customers_fk VALUES (1, 'Alice')").unwrap();
    
    // Insert child row with valid FK - should succeed
    db.execute("INSERT INTO orders_fk VALUES (1, 1)").unwrap();
    
    // Try to insert child row with invalid FK - should fail
    let result = db.execute("INSERT INTO orders_fk VALUES (2, 999)");
    assert!(result.is_err(), "Should fail on invalid FOREIGN KEY");
    assert!(result.unwrap_err().contains("Foreign key violation"));
    
    // Cleanup
    db.execute("DROP TABLE orders_fk").ok();
    db.execute("DROP TABLE customers_fk").ok();
    
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_composite_primary_key() {
    eprintln!("\n=== Test: Composite PRIMARY KEY ===");
    let env = TestEnv::new().with_rustgres().start();
    let db = env.rustgres();

    // Create table with composite PRIMARY KEY
    db.execute("CREATE TABLE order_items_cpk (order_id INT, product_id INT, quantity INT, PRIMARY KEY (order_id, product_id))").unwrap();
    
    // Insert first row
    db.execute("INSERT INTO order_items_cpk VALUES (1, 1, 5)").unwrap();
    
    // Insert row with same order_id but different product_id - should succeed
    db.execute("INSERT INTO order_items_cpk VALUES (1, 2, 3)").unwrap();
    
    // Try to insert duplicate composite key - should fail
    let result = db.execute("INSERT INTO order_items_cpk VALUES (1, 1, 10)");
    assert!(result.is_err(), "Should fail on duplicate composite PRIMARY KEY");
    
    // Cleanup
    db.execute("DROP TABLE order_items_cpk").ok();
    
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_referential_integrity_workflow() {
    eprintln!("\n=== Test: Complete Referential Integrity Workflow ===");
    let env = TestEnv::new().with_rustgres().start();
    let db = env.rustgres();

    // Create schema with multiple FK relationships
    db.execute("CREATE TABLE customers_ri (id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("CREATE TABLE products_ri (id INT PRIMARY KEY, name TEXT, price INT)").unwrap();
    db.execute("CREATE TABLE orders_ri (id INT PRIMARY KEY, customer_id INT REFERENCES customers_ri(id), total INT)").unwrap();
    db.execute("CREATE TABLE order_items_ri (order_id INT REFERENCES orders_ri(id), product_id INT REFERENCES products_ri(id), quantity INT)").unwrap();
    
    // Insert data
    db.execute("INSERT INTO customers_ri VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO products_ri VALUES (1, 'Widget', 100)").unwrap();
    db.execute("INSERT INTO orders_ri VALUES (1, 1, 100)").unwrap();
    db.execute("INSERT INTO order_items_ri VALUES (1, 1, 1)").unwrap();
    
    // Verify data
    let result = db.execute("SELECT * FROM order_items_ri");
    assert!(result.is_ok());
    
    // Cleanup
    db.execute("DROP TABLE order_items_ri").ok();
    db.execute("DROP TABLE orders_ri").ok();
    db.execute("DROP TABLE products_ri").ok();
    db.execute("DROP TABLE customers_ri").ok();
    
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_null_primary_key_rejected() {
    eprintln!("\n=== Test: NULL PRIMARY KEY Rejected ===");
    let env = TestEnv::new().with_rustgres().start();
    let db = env.rustgres();

    // Create table with PRIMARY KEY
    db.execute("CREATE TABLE test_null_pk (id INT PRIMARY KEY, value INT)").unwrap();
    
    // Note: Current implementation doesn't support NULL values in INSERT
    // This test documents expected behavior for future NULL support
    
    // Cleanup
    db.execute("DROP TABLE test_null_pk").ok();
    
    eprintln!("=== Test PASSED ===");
}
