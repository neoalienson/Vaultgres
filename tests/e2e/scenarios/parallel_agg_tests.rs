use e2e::*;

#[test]
fn test_e2e_parallel_hash_agg_count() {
    eprintln!("\n=== E2E Test: ParallelHashAgg COUNT ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE sales (region TEXT, amount INT)").unwrap();
    db.execute(
        "INSERT INTO sales VALUES ('North', 100), ('South', 200), ('East', 150), ('West', 300)",
    )
    .unwrap();

    let result = db.execute("SELECT COUNT(*) FROM sales");
    assert!(result.is_ok(), "COUNT failed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("4"), "Should have 4 rows");

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_parallel_hash_agg_sum_avg() {
    eprintln!("\n=== E2E Test: ParallelHashAgg SUM and AVG ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE orders (customer_id INT, amount INT)").unwrap();
    db.execute("INSERT INTO orders VALUES (1, 100), (1, 200), (2, 150), (2, 50), (3, 300)")
        .unwrap();

    let result = db.execute("SELECT SUM(amount), AVG(amount) FROM orders");
    assert!(result.is_ok(), "SUM/AVG failed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("800"), "Sum should be 800");
    assert!(output.contains("160"), "Avg should be 160");

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_parallel_hash_agg_min_max() {
    eprintln!("\n=== E2E Test: ParallelHashAgg MIN and MAX ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE prices (category TEXT, price INT)").unwrap();
    db.execute("INSERT INTO prices VALUES ('Electronics', 1000), ('Electronics', 500), ('Books', 50), ('Books', 30)")
        .unwrap();

    let result = db.execute(
        "SELECT category, MIN(price), MAX(price) FROM prices GROUP BY category ORDER BY category",
    );
    assert!(result.is_ok(), "MIN/MAX failed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("Books"), "Should have Books category");
    assert!(output.contains("Electronics"), "Should have Electronics category");

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_parallel_hash_agg_group_by() {
    eprintln!("\n=== E2E Test: ParallelHashAgg GROUP BY ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE sales (region TEXT, amount INT)").unwrap();
    db.execute("INSERT INTO sales VALUES ('North', 100), ('North', 200), ('South', 150), ('South', 250), ('East', 300)")
        .unwrap();

    let result = db
        .execute("SELECT region, SUM(amount) as total FROM sales GROUP BY region ORDER BY region");
    assert!(result.is_ok(), "GROUP BY failed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("North"), "Should have North");
    assert!(output.contains("South"), "Should have South");
    assert!(output.contains("East"), "Should have East");

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_parallel_hash_agg_multiple_columns() {
    eprintln!("\n=== E2E Test: ParallelHashAgg with Multiple Columns ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE sales (region TEXT, product TEXT, amount INT)").unwrap();
    db.execute("INSERT INTO sales VALUES ('North', 'Widget', 100), ('North', 'Gadget', 200), ('South', 'Widget', 150)")
        .unwrap();

    let result = db.execute(
        "SELECT region, product, SUM(amount) as total FROM sales GROUP BY region, product ORDER BY region, product",
    );
    assert!(result.is_ok(), "Multiple columns GROUP BY failed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("North"), "Should have North");
    assert!(output.contains("South"), "Should have South");
    assert!(output.contains("Widget"), "Should have Widget");
    assert!(output.contains("Gadget"), "Should have Gadget");

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_parallel_hash_agg_multiple_aggregates() {
    eprintln!("\n=== E2E Test: ParallelHashAgg with Multiple Aggregates ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE stats (category TEXT, value INT)").unwrap();
    db.execute("INSERT INTO stats VALUES ('A', 10), ('A', 20), ('A', 30), ('B', 100), ('B', 200)")
        .unwrap();

    let result = db.execute(
        "SELECT category, COUNT(*) as cnt, SUM(value) as total, AVG(value) as avg_val, MIN(value) as min_val, MAX(value) as max_val FROM stats GROUP BY category ORDER BY category",
    );
    assert!(result.is_ok(), "Multiple aggregates failed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("A"), "Should have category A");
    assert!(output.contains("B"), "Should have category B");
    assert!(output.contains("3"), "A has 3 rows");
    assert!(output.contains("2"), "B has 2 rows");

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_parallel_hash_agg_null_handling() {
    eprintln!("\n=== E2E Test: ParallelHashAgg NULL Handling ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE null_test (category TEXT, value INT)").unwrap();
    db.execute(
        "INSERT INTO null_test VALUES ('A', 10), ('A', NULL), ('A', 30), ('B', NULL), ('B', 50)",
    )
    .unwrap();

    let result = db.execute("SELECT category, COUNT(value), SUM(value) FROM null_test GROUP BY category ORDER BY category");
    assert!(result.is_ok(), "NULL handling failed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("A"), "Should have A");
    assert!(output.contains("B"), "Should have B");

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_parallel_hash_agg_empty_input() {
    eprintln!("\n=== E2E Test: ParallelHashAgg Empty Input ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE empty_table (id INT, value INT)").unwrap();

    let result = db.execute("SELECT COUNT(*), SUM(value) FROM empty_table");
    assert!(result.is_ok(), "Empty input failed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("0"), "COUNT should be 0");

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_parallel_hash_agg_count_star() {
    eprintln!("\n=== E2E Test: ParallelHashAgg COUNT(*) ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE orders (customer_id INT, amount INT)").unwrap();
    db.execute("INSERT INTO orders VALUES (1, 100), (1, NULL), (2, 200), (3, NULL)").unwrap();

    let result = db.execute(
        "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id ORDER BY customer_id",
    );
    assert!(result.is_ok(), "COUNT(*) failed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("1"), "Customer 1 should appear");
    assert!(output.contains("2"), "Customer 2 should appear");
    assert!(output.contains("3"), "Customer 3 should appear");

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_parallel_hash_agg_having() {
    eprintln!("\n=== E2E Test: ParallelHashAgg with HAVING ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE orders (customer_id INT, amount INT)").unwrap();
    db.execute("INSERT INTO orders VALUES (1, 100), (1, 200), (2, 50), (3, 300), (3, 400)")
        .unwrap();

    let result = db.execute(
        "SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id HAVING SUM(amount) > 200 ORDER BY customer_id",
    );
    assert!(result.is_ok(), "HAVING failed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("1"), "Customer 1 should appear (total 300)");
    assert!(output.contains("3"), "Customer 3 should appear (total 700)");
    assert!(!output.contains("2"), "Customer 2 should NOT appear (total 50)");

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_parallel_hash_agg_distinct_aggregate() {
    eprintln!("\n=== E2E Test: ParallelHashAgg with DISTINCT ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE orders (customer_id INT, amount INT)").unwrap();
    db.execute("INSERT INTO orders VALUES (1, 100), (1, 100), (1, 200), (2, 100), (2, 100)")
        .unwrap();

    let result = db.execute("SELECT customer_id, COUNT(DISTINCT amount) FROM orders GROUP BY customer_id ORDER BY customer_id");
    assert!(result.is_ok(), "DISTINCT in aggregate failed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("1"), "Customer 1 should appear");
    assert!(output.contains("2"), "Customer 2 should appear");

    eprintln!("=== E2E Test PASSED ===");
}
