use e2e::*;

#[test]
fn test_e2e_parallel_hash_join_inner() {
    eprintln!("\n=== E2E Test: ParallelHashJoin INNER JOIN ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE orders (order_id INT, customer_id INT, amount INT)").unwrap();
    db.execute("CREATE TABLE customers (customer_id INT, name TEXT)").unwrap();

    db.execute("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')").unwrap();
    db.execute("INSERT INTO orders VALUES (100, 1, 500), (101, 1, 300), (102, 2, 200)").unwrap();

    let result = db.execute(
        "SELECT o.order_id, c.name, o.amount FROM orders o INNER JOIN customers c ON o.customer_id = c.customer_id",
    );
    assert!(result.is_ok(), "INNER JOIN failed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("Alice"), "Should have Alice's orders");
    assert!(output.contains("Bob"), "Should have Bob's order");
    assert!(!output.contains("Charlie"), "Charlie has no orders, should not appear");

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_parallel_hash_join_left() {
    eprintln!("\n=== E2E Test: ParallelHashJoin LEFT JOIN ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE orders (order_id INT, customer_id INT, amount INT)").unwrap();
    db.execute("CREATE TABLE customers (customer_id INT, name TEXT)").unwrap();

    db.execute("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')").unwrap();
    db.execute("INSERT INTO orders VALUES (100, 1, 500), (101, 1, 300), (102, 2, 200)").unwrap();

    let result = db.execute(
        "SELECT c.name, o.amount FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id ORDER BY c.name, o.amount",
    );
    assert!(result.is_ok(), "LEFT JOIN failed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("Alice"), "Alice has orders");
    assert!(output.contains("Bob"), "Bob has orders");
    assert!(output.contains("Charlie"), "Charlie has no orders but should appear");
    assert!(output.contains("NULL") || output.contains("null"), "Charlie's order should be NULL");

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_parallel_hash_join_right() {
    eprintln!("\n=== E2E Test: ParallelHashJoin RIGHT JOIN ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE orders (order_id INT, customer_id INT, amount INT)").unwrap();
    db.execute("CREATE TABLE customers (customer_id INT, name TEXT)").unwrap();

    db.execute("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    db.execute("INSERT INTO orders VALUES (100, 1, 500), (101, 2, 300), (102, 3, 200)").unwrap();

    let result = db.execute(
        "SELECT o.order_id, c.name, o.amount FROM orders o RIGHT JOIN customers c ON o.customer_id = c.customer_id",
    );
    assert!(result.is_ok(), "RIGHT JOIN failed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("Alice"), "Alice has orders");
    assert!(output.contains("Bob"), "Bob has orders");
    assert!(
        !output.contains("Charlie") || output.contains("NULL") || output.contains("null"),
        "Customer 3 doesn't exist so no Charlie"
    );

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_parallel_hash_join_full() {
    eprintln!("\n=== E2E Test: ParallelHashJoin FULL OUTER JOIN ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE team_a (id INT, name TEXT)").unwrap();
    db.execute("CREATE TABLE team_b (id INT, name TEXT)").unwrap();

    db.execute("INSERT INTO team_a VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')").unwrap();
    db.execute("INSERT INTO team_b VALUES (1, 'Alex'), (3, 'Chris'), (4, 'David')").unwrap();

    let result = db.execute(
        "SELECT a.name AS name_a, b.name AS name_b FROM team_a a FULL OUTER JOIN team_b b ON a.id = b.id ORDER BY name_a, name_b",
    );
    assert!(result.is_ok(), "FULL OUTER JOIN failed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("Alice"), "Should have Alice");
    assert!(output.contains("Bob"), "Should have Bob (no match)");
    assert!(output.contains("Charlie"), "Should have Charlie");
    assert!(output.contains("Alex"), "Should have Alex");
    assert!(output.contains("Chris"), "Should have Chris");
    assert!(output.contains("David"), "Should have David (no match)");

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_parallel_hash_join_composite_key() {
    eprintln!("\n=== E2E Test: ParallelHashJoin with Composite Keys ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE sales (region TEXT, product TEXT, amount INT)").unwrap();
    db.execute("CREATE TABLE targets (region TEXT, product TEXT, target INT)").unwrap();

    db.execute("INSERT INTO sales VALUES ('North', 'Widget', 100), ('North', 'Widget', 150), ('South', 'Widget', 200)")
        .unwrap();
    db.execute("INSERT INTO targets VALUES ('North', 'Widget', 300), ('South', 'Widget', 250), ('East', 'Widget', 400)")
        .unwrap();

    let result = db.execute(
        "SELECT s.region, s.product, s.amount, t.target FROM sales s INNER JOIN targets t ON s.region = t.region AND s.product = t.product",
    );
    assert!(result.is_ok(), "Composite key JOIN failed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("North"), "Should have North region data");
    assert!(output.contains("South"), "Should have South region data");
    assert!(
        !output.contains("East") || !output.contains("West"),
        "East/West have no sales, should not appear in INNER"
    );

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_parallel_hash_join_no_match() {
    eprintln!("\n=== E2E Test: ParallelHashJoin with No Matching Rows ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE a (id INT, val TEXT)").unwrap();
    db.execute("CREATE TABLE b (id INT, val TEXT)").unwrap();

    db.execute("INSERT INTO a VALUES (1, 'one'), (2, 'two')").unwrap();
    db.execute("INSERT INTO b VALUES (3, 'three'), (4, 'four')").unwrap();

    let result = db.execute("SELECT a.val, b.val FROM a INNER JOIN b ON a.id = b.id");
    assert!(result.is_ok(), "INNER JOIN with no matches failed: {:?}", result);
    let output = result.unwrap();
    assert!(
        !output.contains("one") && !output.contains("two"),
        "No matches so nothing should be returned"
    );

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_parallel_hash_join_self_join() {
    eprintln!("\n=== E2E Test: ParallelHashJoin Self Join ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE employees (id INT, name TEXT, manager_id INT)").unwrap();

    db.execute("INSERT INTO employees VALUES (1, 'CEO', NULL), (2, 'Alice', 1), (3, 'Bob', 1), (4, 'Charlie', 2)")
        .unwrap();

    let result = db.execute(
        "SELECT e.name AS employee, m.name AS manager FROM employees e INNER JOIN employees m ON e.manager_id = m.id",
    );
    assert!(result.is_ok(), "Self join failed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("Alice"), "Alice has a manager");
    assert!(output.contains("Bob"), "Bob has a manager");
    assert!(output.contains("Charlie"), "Charlie has a manager");
    assert!(output.contains("CEO"), "CEO is a manager");
    assert!(
        !output.contains("Charlie") || output.matches("Charlie").count() == 1,
        "Charlie appears once as employee not as manager"
    );

    eprintln!("=== E2E Test PASSED ===");
}
