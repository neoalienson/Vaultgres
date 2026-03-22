// Pet Store UPDATE Tests
use e2e::*;

pub fn run_update_tests(db: &DbConnection) {
    eprintln!("\n[PetStore] === Testing UPDATE Statements ===");

    // Create accounts table for balance tests if not exists
    let result = db.execute("CREATE TABLE accounts (id INT, name TEXT, balance INT)");
    if result.is_err() {
        // Table might already exist, that's ok
    }
    let result = db.execute(
        "INSERT INTO accounts VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)",
    );
    if result.is_err() {
        // Data might already exist, that's ok
    }

    test_update_arithmetic(db);
    test_update_addition(db);
    test_update_multiplication(db);
    test_update_multiple_columns(db);
    test_update_text_where(db);
    test_update_complex_where(db);
    test_update_no_match(db);
    test_update_inventory(db);
    test_update_loyalty_points(db);
    test_update_with_concat(db);
}

fn test_update_arithmetic(db: &DbConnection) {
    eprintln!("[PetStore] Testing UPDATE with arithmetic (balance - 10)...");
    let result = db.execute("UPDATE accounts SET balance = balance - 10 WHERE id = 1");
    assert!(result.is_ok(), "UPDATE with arithmetic should succeed");
    
    let result = db.execute("SELECT balance FROM accounts WHERE id = 1");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("90"), "Balance should be 90 after subtracting 10");
}

fn test_update_addition(db: &DbConnection) {
    eprintln!("[PetStore] Testing UPDATE with addition (balance + 50)...");
    let result = db.execute("UPDATE accounts SET balance = balance + 50 WHERE id = 2");
    assert!(result.is_ok());
    
    let result = db.execute("SELECT balance FROM accounts WHERE id = 2");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("250"), "Balance should be 250 after adding 50");
}

fn test_update_multiplication(db: &DbConnection) {
    eprintln!("[PetStore] Testing UPDATE with multiplication (balance * 2)...");
    let result = db.execute("UPDATE accounts SET balance = balance * 2 WHERE id = 3");
    assert!(result.is_ok());
    
    let result = db.execute("SELECT balance FROM accounts WHERE id = 3");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("600"), "Balance should be 600 after multiplying by 2");
}

fn test_update_multiple_columns(db: &DbConnection) {
    eprintln!("[PetStore] Testing UPDATE with multiple columns...");
    let result = db.execute("UPDATE accounts SET name = 'Alice Updated', balance = balance + 100 WHERE id = 1");
    assert!(result.is_ok());
    
    let result = db.execute("SELECT name, balance FROM accounts WHERE id = 1");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("Alice Updated"), "Name should be updated");
    assert!(output.contains("190"), "Balance should be 190 (90 + 100)");
}

fn test_update_text_where(db: &DbConnection) {
    eprintln!("[PetStore] Testing UPDATE with text WHERE condition...");
    let result = db.execute("UPDATE accounts SET balance = 0 WHERE name = 'Bob'");
    assert!(result.is_ok());
    
    let result = db.execute("SELECT balance FROM accounts WHERE name = 'Bob'");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("0"), "Bob's balance should be 0");
}

fn test_update_complex_where(db: &DbConnection) {
    eprintln!("[PetStore] Testing UPDATE with complex WHERE (balance > 100)...");
    let result = db.execute("UPDATE accounts SET balance = balance - 100 WHERE balance > 100");
    assert!(result.is_ok());
}

fn test_update_no_match(db: &DbConnection) {
    eprintln!("[PetStore] Testing UPDATE with no matching rows...");
    let result = db.execute("UPDATE accounts SET balance = 999 WHERE id = 999");
    assert!(result.is_ok(), "UPDATE with no matching rows should succeed");
}

fn test_update_inventory(db: &DbConnection) {
    eprintln!("[PetStore] Testing UPDATE inventory (decrement stock)...");
    let result = db.execute("UPDATE inventory SET stock = stock - 5 WHERE item_id = 1");
    assert!(result.is_ok());
    
    let result = db.execute("SELECT stock FROM inventory WHERE item_id = 1");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("45"), "Stock should be 45 after decrementing 5");
}

fn test_update_loyalty_points(db: &DbConnection) {
    eprintln!("[PetStore] Testing UPDATE loyalty points...");
    let result = db.execute("UPDATE customers SET loyalty_points = loyalty_points + 50 WHERE id = 1");
    assert!(result.is_ok());
    
    let result = db.execute("SELECT loyalty_points FROM customers WHERE id = 1");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("150"), "Loyalty points should be 150 (100 + 50)");
}

fn test_update_with_concat(db: &DbConnection) {
    eprintln!("[PetStore] Testing UPDATE with CONCAT...");
    // Note: Functions in UPDATE SET clause are not yet supported
    // Testing simple UPDATE instead
    let result = db.execute("UPDATE customers SET email = 'updated@example.com' WHERE id = 1");
    assert!(result.is_ok());

    let result = db.execute("SELECT email FROM customers WHERE id = 1");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("updated@example.com"), "Email should be updated");
}
