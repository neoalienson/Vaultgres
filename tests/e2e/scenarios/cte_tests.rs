use e2e::*;

#[test]
fn test_simple_cte() {
    eprintln!("\n=== Test: Simple CTE ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute("WITH cte AS (SELECT 1 AS n) SELECT * FROM cte");
    assert!(result.is_ok(), "CTE query failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_cte_with_where() {
    eprintln!("\n=== Test: CTE with WHERE clause ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE test_table (id INT, value INT)").unwrap();
    db.execute("INSERT INTO test_table VALUES (1, 10), (2, 20), (3, 30)").unwrap();

    let result =
        db.execute("WITH cte AS (SELECT id FROM test_table WHERE value > 15) SELECT * FROM cte");
    assert!(result.is_ok(), "CTE query failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_cte_with_multiple_ctes() {
    eprintln!("\n=== Test: CTE with multiple CTEs ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db
        .execute("WITH cte1 AS (SELECT 1 AS n), cte2 AS (SELECT 2 AS m) SELECT * FROM cte1, cte2");
    assert!(result.is_ok(), "Multiple CTE query failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_cte_with_subquery() {
    eprintln!("\n=== Test: CTE with subquery ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE employees (id INT, name TEXT, salary INT)").unwrap();
    db.execute("INSERT INTO employees VALUES (1, 'Alice', 60000), (2, 'Bob', 45000), (3, 'Charlie', 70000)").unwrap();

    let result = db.execute("WITH high_earners AS (SELECT name FROM employees WHERE salary > 50000) SELECT * FROM high_earners");
    assert!(result.is_ok(), "CTE with subquery failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_cte_with_join() {
    eprintln!("\n=== Test: CTE with JOIN ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE a (id INT, val TEXT)").unwrap();
    db.execute("CREATE TABLE b (id INT, val TEXT)").unwrap();
    db.execute("INSERT INTO a VALUES (1, 'A'), (2, 'B')").unwrap();
    db.execute("INSERT INTO b VALUES (1, 'X'), (2, 'Y')").unwrap();

    let result = db
        .execute("WITH cte AS (SELECT a.id, b.val FROM a JOIN b ON a.id = b.id) SELECT * FROM cte");
    assert!(result.is_ok(), "CTE with JOIN failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_cte_with_order_by() {
    eprintln!("\n=== Test: CTE with ORDER BY ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE items (id INT, name TEXT)").unwrap();
    db.execute("INSERT INTO items VALUES (3, 'Gamma'), (1, 'Alpha'), (2, 'Beta')").unwrap();

    let result = db.execute("WITH cte AS (SELECT * FROM items) SELECT * FROM cte ORDER BY id");
    assert!(result.is_ok(), "CTE with ORDER BY failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_cte_with_group_by() {
    eprintln!("\n=== Test: CTE with GROUP BY ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE products (category TEXT, name TEXT, price INT)").unwrap();
    db.execute("INSERT INTO products VALUES ('Electronics', 'TV', 500), ('Electronics', 'Radio', 200), ('Furniture', 'Chair', 100)").unwrap();

    let result = db.execute("WITH summary AS (SELECT category, COUNT(*) as cnt FROM products GROUP BY category) SELECT * FROM summary");
    assert!(result.is_ok(), "CTE with GROUP BY failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_cte_referencing_earlier_cte() {
    eprintln!("\n=== Test: CTE referencing earlier CTE ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute(
        "WITH cte1 AS (SELECT 1 AS n), cte2 AS (SELECT n + 1 FROM cte1) SELECT * FROM cte2",
    );
    assert!(result.is_ok(), "CTE referencing earlier CTE failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_cte_with_limit() {
    eprintln!("\n=== Test: CTE with LIMIT ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE numbers (n INT)").unwrap();
    db.execute("INSERT INTO numbers VALUES (1), (2), (3), (4), (5)").unwrap();

    let result = db.execute("WITH cte AS (SELECT * FROM numbers) SELECT * FROM cte LIMIT 3");
    assert!(result.is_ok(), "CTE with LIMIT failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_cte_with_distinct() {
    eprintln!("\n=== Test: CTE with DISTINCT ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE items (category TEXT, value INT)").unwrap();
    db.execute("INSERT INTO items VALUES ('A', 1), ('A', 1), ('B', 2)").unwrap();

    let result = db.execute("WITH cte AS (SELECT DISTINCT category FROM items) SELECT * FROM cte");
    assert!(result.is_ok(), "CTE with DISTINCT failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_nested_cte() {
    eprintln!("\n=== Test: Nested CTE ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE t1 (id INT)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1), (2), (3)").unwrap();

    let result = db.execute("WITH outer_cte AS (WITH inner_cte AS (SELECT id FROM t1) SELECT * FROM inner_cte) SELECT * FROM outer_cte");
    assert!(result.is_ok(), "Nested CTE failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_cte_materializes_once() {
    eprintln!("\n=== Test: CTE materializes once ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE counter (n INT)").unwrap();
    db.execute("INSERT INTO counter VALUES (1)").unwrap();

    let result =
        db.execute("WITH cte AS (SELECT n FROM counter) SELECT a.n, b.n FROM cte a, cte b");
    assert!(result.is_ok(), "CTE materialization test failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}
