// Pet Store Edge Case Tests
use e2e::*;

pub fn run_edge_case_tests(env: &RunningEnv) {
    eprintln!("\n[PetStore] === Testing Edge Cases ===");

    let db = env.vaultgres();

    test_update_persistence_after_restart(env);
    test_update_with_null_handling(&db);
    test_update_with_case(&db);
    test_update_with_subquery(&db);
    test_update_all_rows(&db);
    test_update_large_values(&db);
    test_update_negative_values(&db);
    test_update_with_string_concat(&db);
    test_update_with_string_functions(&db);
    test_complex_multi_table_verification(&db);
}

fn test_update_persistence_after_restart(env: &RunningEnv) {
    eprintln!("[PetStore] Testing UPDATE persistence after restart...");
    let db = env.vaultgres();

    let result = db.execute("UPDATE accounts SET balance = 1000 WHERE id = 1");
    assert!(result.is_ok());

    // Restart and verify update persisted
    env.restart_graceful(5);
    let db = env.vaultgres();

    let result = db.execute("SELECT balance FROM accounts WHERE id = 1");
    assert!(result.is_ok(), "Account balance should persist after restart");
    let output = result.unwrap();
    assert!(output.contains("1000"), "Updated balance should persist");
}

fn test_update_with_null_handling(db: &DbConnection) {
    eprintln!("[PetStore] Testing UPDATE with NULL handling...");
    db.execute("CREATE TABLE nullable_test (id INT, value INT)").unwrap();
    db.execute("INSERT INTO nullable_test VALUES (1, 10), (2, NULL), (3, 30)").unwrap();

    let result = db.execute("UPDATE nullable_test SET value = value + 10 WHERE value IS NOT NULL");
    assert!(result.is_ok());

    let result = db.execute("SELECT value FROM nullable_test WHERE id = 1");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("20"), "Value should be 20 (10 + 10)");
}

fn test_update_with_case(db: &DbConnection) {
    eprintln!("[PetStore] Testing UPDATE with conditional logic...");
    let result = db.execute("UPDATE accounts SET balance = CASE WHEN balance > 100 THEN balance - 10 ELSE balance + 10 END WHERE id IN (1, 2)");
    assert!(result.is_ok());
}

fn test_update_with_subquery(db: &DbConnection) {
    eprintln!("[PetStore] Testing UPDATE with subquery...");
    let result = db
        .execute("UPDATE accounts SET balance = (SELECT MAX(balance) FROM accounts) WHERE id = 1");
    assert!(result.is_ok());
}

fn test_update_all_rows(db: &DbConnection) {
    eprintln!("[PetStore] Testing UPDATE all rows...");
    let result = db.execute("UPDATE accounts SET balance = 0");
    assert!(result.is_ok());

    let result = db.execute("SELECT COUNT(*) FROM accounts WHERE balance = 0");
    assert!(result.is_ok());
    // let output = result.unwrap();
    // assert!(output.contains("9"), "All 9 accounts should have balance 0");
}

fn test_update_large_values(db: &DbConnection) {
    eprintln!("[PetStore] Testing UPDATE with large values...");
    let result = db.execute("UPDATE accounts SET balance = 999999999 WHERE id = 1");
    assert!(result.is_ok());

    let result = db.execute("SELECT balance FROM accounts WHERE id = 1");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("999999999"), "Large value should be stored");
}

fn test_update_negative_values(db: &DbConnection) {
    eprintln!("[PetStore] Testing UPDATE with negative values...");
    let result = db.execute("UPDATE accounts SET balance = -100 WHERE id = 2");
    assert!(result.is_ok());

    let result = db.execute("SELECT balance FROM accounts WHERE id = 2");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("-100"), "Negative value should be stored");
}

fn test_update_with_string_concat(db: &DbConnection) {
    eprintln!("[PetStore] Testing UPDATE with string operations...");
    let result = db.execute("UPDATE customers SET name = CONCAT(name, ' Jr.') WHERE id = 1");
    assert!(result.is_ok());

    let result = db.execute("SELECT name FROM customers WHERE id = 1");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("Jr."), "Name should contain 'Jr.'");
}

fn test_update_with_string_functions(db: &DbConnection) {
    eprintln!("[PetStore] Testing UPDATE with string functions...");
    let result = db.execute("UPDATE customers SET name = UPPER(name) WHERE id = 2");
    assert!(result.is_ok());

    let result = db.execute("SELECT name FROM customers WHERE id = 2");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("BOB"), "Name should be uppercase");
}

fn test_complex_multi_table_verification(db: &DbConnection) {
    eprintln!("[PetStore] Testing complex multi-table verification...");
    let result = db.execute("SELECT c.name, COUNT(o.id) as order_count FROM customers c LEFT JOIN orders o ON c.id = o.customer_id GROUP BY c.name");
    assert!(result.is_ok());
}
