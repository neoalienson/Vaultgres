// Pet Store INSERT Edge Case Tests
use e2e::*;

pub fn run_insert_tests(db: &DbConnection) {
    eprintln!("\n[PetStore] === Testing INSERT Edge Cases ===");
    
    // Create accounts table first
    eprintln!("[PetStore] Creating accounts table for INSERT tests...");
    db.execute("CREATE TABLE accounts (id INT, name TEXT, balance INT)").unwrap();
    db.execute("INSERT INTO accounts VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)").unwrap();
    
    test_insert_single_row(db);
    test_insert_multiple_rows(db);
    test_insert_with_null(db);
    test_insert_special_characters(db);
    test_insert_large_values(db);
    test_insert_negative_values(db);
    test_insert_with_select(db);
    test_insert_with_expressions(db);
    test_insert_with_concat(db);
    test_insert_with_string_functions(db);
    test_insert_all_columns(db);
    test_insert_zero_values(db);
}

fn test_insert_single_row(db: &DbConnection) {
    eprintln!("[PetStore] Testing INSERT single row...");
    let result = db.execute("INSERT INTO accounts (id, name, balance) VALUES (100, 'Test User', 500)");
    assert!(result.is_ok());
}

fn test_insert_multiple_rows(db: &DbConnection) {
    eprintln!("[PetStore] Testing INSERT multiple rows...");
    let result = db.execute("INSERT INTO accounts (id, name, balance) VALUES (101, 'User 1', 100), (102, 'User 2', 200), (103, 'User 3', 300)");
    assert!(result.is_ok());
}

fn test_insert_with_null(db: &DbConnection) {
    eprintln!("[PetStore] Testing INSERT with NULL...");
    // Note: Using "txt" instead of "text" because TEXT is a reserved keyword
    db.execute("CREATE TABLE insert_null_test (id INT, value INT, txt TEXT)").unwrap();
    let result = db.execute("INSERT INTO insert_null_test (id, value, txt) VALUES (1, NULL, 'hello'), (2, 10, NULL)");
    assert!(result.is_ok());
}

fn test_insert_special_characters(db: &DbConnection) {
    eprintln!("[PetStore] Testing INSERT with special characters...");
    let result = db.execute("INSERT INTO customers (id, name, email, loyalty_points) VALUES (100, 'O''Brien', 'test@example.com', 0)");
    assert!(result.is_ok());
}

fn test_insert_large_values(db: &DbConnection) {
    eprintln!("[PetStore] Testing INSERT with large values...");
    let result = db.execute("INSERT INTO accounts (id, name, balance) VALUES (999, 'Big Spender', 999999999)");
    assert!(result.is_ok());
}

fn test_insert_negative_values(db: &DbConnection) {
    eprintln!("[PetStore] Testing INSERT with negative values...");
    let result = db.execute("INSERT INTO accounts (id, name, balance) VALUES (998, 'In Debt', -500)");
    assert!(result.is_ok());
}

fn test_insert_with_select(db: &DbConnection) {
    eprintln!("[PetStore] Testing INSERT with SELECT...");
    db.execute("CREATE TABLE accounts_copy (id INT, name TEXT, balance INT)").unwrap();
    let result = db.execute("INSERT INTO accounts_copy SELECT id, name, balance FROM accounts WHERE balance > 0");
    assert!(result.is_ok());
}

fn test_insert_with_expressions(db: &DbConnection) {
    eprintln!("[PetStore] Testing INSERT with expressions...");
    let result = db.execute("INSERT INTO accounts (id, name, balance) VALUES (888, 'Math User', 100 + 200)");
    assert!(result.is_ok());
    
    let result = db.execute("SELECT balance FROM accounts WHERE id = 888");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("300"), "Expression should be evaluated");
}

fn test_insert_with_concat(db: &DbConnection) {
    eprintln!("[PetStore] Testing INSERT with CONCAT...");
    let result = db.execute("INSERT INTO customers (id, name, email, loyalty_points) VALUES (888, CONCAT('John', ' ', 'Doe'), 'john@example.com', 50)");
    assert!(result.is_ok());
    
    let result = db.execute("SELECT name FROM customers WHERE id = 888");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("John Doe"), "CONCAT should work in INSERT");
}

fn test_insert_with_string_functions(db: &DbConnection) {
    eprintln!("[PetStore] Testing INSERT with string functions...");
    let result = db.execute("INSERT INTO customers (id, name, email, loyalty_points) VALUES (777, UPPER('alice'), 'alice2@example.com', 25)");
    assert!(result.is_ok());
    
    let result = db.execute("SELECT name FROM customers WHERE id = 777");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("ALICE"), "UPPER should work in INSERT");
}

fn test_insert_all_columns(db: &DbConnection) {
    eprintln!("[PetStore] Testing INSERT all columns...");
    let result = db.execute("INSERT INTO accounts VALUES (666, 'Full Row', 999)");
    assert!(result.is_ok());
}

fn test_insert_zero_values(db: &DbConnection) {
    eprintln!("[PetStore] Testing INSERT with zero values...");
    let result = db.execute("INSERT INTO accounts (id, name, balance) VALUES (555, 'Zero Balance', 0)");
    assert!(result.is_ok());
}
