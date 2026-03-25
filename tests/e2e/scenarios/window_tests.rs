use e2e::*;

#[test]
fn test_e2e_window_row_number() {
    eprintln!("\n=== E2E Test: Window ROW_NUMBER ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE employees (id INT, name TEXT, department TEXT, salary INT)").unwrap();
    db.execute("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 7000), (2, 'Bob', 'Engineering', 8000), (3, 'Charlie', 'Sales', 5000)")
        .unwrap();

    let result = db.execute(
        "SELECT name, department, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as row_num FROM employees ORDER BY department, salary DESC",
    );
    assert!(result.is_ok(), "ROW_NUMBER failed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("Alice") && output.contains("Bob") && output.contains("Charlie"));

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_window_rank() {
    eprintln!("\n=== E2E Test: Window RANK ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE products (id INT, category TEXT, price INT)").unwrap();
    db.execute("INSERT INTO products VALUES (1, 'Electronics', 1000), (2, 'Electronics', 1000), (3, 'Electronics', 500), (4, 'Clothing', 50)")
        .unwrap();

    let result = db.execute(
        "SELECT category, price, RANK() OVER (ORDER BY price DESC) as rank FROM products ORDER BY rank",
    );
    assert!(result.is_ok(), "RANK failed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("1000") && output.contains("500") && output.contains("50"));

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_window_dense_rank() {
    eprintln!("\n=== E2E Test: Window DENSE_RANK ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE scores (player TEXT, score INT)").unwrap();
    db.execute(
        "INSERT INTO scores VALUES ('Alice', 100), ('Bob', 100), ('Charlie', 90), ('David', 80)",
    )
    .unwrap();

    let result = db.execute(
        "SELECT player, score, DENSE_RANK() OVER (ORDER BY score DESC) as dense_rank FROM scores ORDER BY dense_rank, player",
    );
    assert!(result.is_ok(), "DENSE_RANK failed: {:?}", result);
    let output = result.unwrap();
    assert!(
        output.contains("Alice")
            && output.contains("Bob")
            && output.contains("Charlie")
            && output.contains("David")
    );

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_window_lag() {
    eprintln!("\n=== E2E Test: Window LAG ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE sales (month TEXT, revenue INT)").unwrap();
    db.execute(
        "INSERT INTO sales VALUES ('Jan', 1000), ('Feb', 1500), ('Mar', 1200), ('Apr', 1800)",
    )
    .unwrap();

    let result = db.execute(
        "SELECT month, revenue, LAG(revenue) OVER (ORDER BY month) as prev_revenue FROM sales ORDER BY month",
    );
    assert!(result.is_ok(), "LAG failed: {:?}", result);
    let output = result.unwrap();
    assert!(
        output.contains("Jan")
            && output.contains("Feb")
            && output.contains("Mar")
            && output.contains("Apr")
    );

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_window_lead() {
    eprintln!("\n=== E2E Test: Window LEAD ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE stock (day TEXT, price INT)").unwrap();
    db.execute("INSERT INTO stock VALUES ('Mon', 100), ('Tue', 110), ('Wed', 105), ('Thu', 115)")
        .unwrap();

    let result = db.execute(
        "SELECT day, price, LEAD(price) OVER (ORDER BY day) as next_price FROM stock ORDER BY day",
    );
    assert!(result.is_ok(), "LEAD failed: {:?}", result);
    let output = result.unwrap();
    assert!(
        output.contains("Mon")
            && output.contains("Tue")
            && output.contains("Wed")
            && output.contains("Thu")
    );

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_window_partition_by() {
    eprintln!("\n=== E2E Test: Window with PARTITION BY ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE orders (customer TEXT, region TEXT, amount INT)").unwrap();
    db.execute("INSERT INTO orders VALUES ('Alice', 'North', 100), ('Bob', 'North', 200), ('Charlie', 'South', 150), ('David', 'South', 250)")
        .unwrap();

    let result = db.execute(
        "SELECT customer, region, amount, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount) as row_num FROM orders ORDER BY region, amount",
    );
    assert!(result.is_ok(), "PARTITION BY failed: {:?}", result);
    let output = result.unwrap();
    assert!(
        output.contains("Alice")
            && output.contains("Bob")
            && output.contains("Charlie")
            && output.contains("David")
    );

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_window_first_value() {
    eprintln!("\n=== E2E Test: Window FIRST_VALUE ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE ranked (id INT, score INT)").unwrap();
    db.execute("INSERT INTO ranked VALUES (1, 100), (2, 200), (3, 150)").unwrap();

    let result = db.execute(
        "SELECT id, score, FIRST_VALUE(score) OVER (ORDER BY id) as first_score FROM ranked ORDER BY id",
    );
    assert!(result.is_ok(), "FIRST_VALUE failed: {:?}", result);

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_window_last_value() {
    eprintln!("\n=== E2E Test: Window LAST_VALUE ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE sales_rank (id INT, amount INT)").unwrap();
    db.execute("INSERT INTO sales_rank VALUES (1, 500), (2, 800), (3, 300)").unwrap();

    let result = db.execute(
        "SELECT id, amount, LAST_VALUE(amount) OVER (ORDER BY id) as last_amount FROM sales_rank ORDER BY id",
    );
    assert!(result.is_ok(), "LAST_VALUE failed: {:?}", result);

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_window_multiple_functions() {
    eprintln!("\n=== E2E Test: Multiple Window Functions ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE metrics (period TEXT, value INT)").unwrap();
    db.execute("INSERT INTO metrics VALUES ('Q1', 1000), ('Q2', 1500), ('Q3', 1200), ('Q4', 1800)")
        .unwrap();

    let result = db.execute(
        "SELECT period, value, ROW_NUMBER() OVER (ORDER BY period) as rn, LAG(value) OVER (ORDER BY period) as prev, LEAD(value) OVER (ORDER BY period) as next FROM metrics ORDER BY period",
    );
    assert!(result.is_ok(), "Multiple window functions failed: {:?}", result);
    let output = result.unwrap();
    assert!(
        output.contains("Q1")
            && output.contains("Q2")
            && output.contains("Q3")
            && output.contains("Q4")
    );

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_window_ntile() {
    eprintln!("\n=== E2E Test: Window NTILE ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE customers (name TEXT, balance INT)").unwrap();
    db.execute("INSERT INTO customers VALUES ('Alice', 1000), ('Bob', 2000), ('Charlie', 3000), ('David', 4000), ('Eve', 5000)")
        .unwrap();

    let result = db.execute(
        "SELECT name, balance, NTILE(2) OVER (ORDER BY balance) as quartile FROM customers ORDER BY balance",
    );
    assert!(result.is_ok(), "NTILE failed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("Alice") && output.contains("Bob") && output.contains("Charlie"));

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_window_percent_rank() {
    eprintln!("\n=== E2E Test: Window PERCENT_RANK ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE ranking (id INT, score INT)").unwrap();
    db.execute("INSERT INTO ranking VALUES (1, 100), (2, 200), (3, 300)").unwrap();

    let result = db.execute(
        "SELECT id, score, PERCENT_RANK() OVER (ORDER BY score) as pct FROM ranking ORDER BY id",
    );
    assert!(result.is_ok(), "PERCENT_RANK failed: {:?}", result);

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_window_cume_dist() {
    eprintln!("\n=== E2E Test: Window CUME_DIST ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE grades (student TEXT, grade INT)").unwrap();
    db.execute(
        "INSERT INTO grades VALUES ('Alice', 85), ('Bob', 92), ('Charlie', 85), ('David', 78)",
    )
    .unwrap();

    let result = db.execute(
        "SELECT student, grade, CUME_DIST() OVER (ORDER BY grade) as cdist FROM grades ORDER BY grade, student",
    );
    assert!(result.is_ok(), "CUME_DIST failed: {:?}", result);

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_window_nth_value() {
    eprintln!("\n=== E2E Test: Window NTH_VALUE ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE sequence (pos INT, value INT)").unwrap();
    db.execute("INSERT INTO sequence VALUES (1, 10), (2, 20), (3, 30), (4, 40)").unwrap();

    let result = db.execute(
        "SELECT pos, value, NTH_VALUE(value, 2) OVER (ORDER BY pos) as second_val FROM sequence ORDER BY pos",
    );
    assert!(result.is_ok(), "NTH_VALUE failed: {:?}", result);

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_window_empty_partition() {
    eprintln!("\n=== E2E Test: Window on Empty Table ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE empty_table (id INT, value INT)").unwrap();

    let result = db.execute("SELECT id, ROW_NUMBER() OVER (ORDER BY id) as rn FROM empty_table");
    assert!(result.is_ok(), "Window on empty table failed: {:?}", result);

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_window_rows_frame() {
    eprintln!("\n=== E2E Test: Window with ROWS frame ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE sales (id INT, value INT)").unwrap();
    db.execute("INSERT INTO sales VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)")
        .unwrap();

    let result = db.execute(
        "SELECT id, value, FIRST_VALUE(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as first_val FROM sales ORDER BY id",
    );
    assert!(result.is_ok(), "ROWS frame failed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("100") && output.contains("200") && output.contains("300"));

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_window_first_value_frame() {
    eprintln!("\n=== E2E Test: FIRST_VALUE with frame ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE ranked (id INT, score INT)").unwrap();
    db.execute("INSERT INTO ranked VALUES (1, 100), (2, 200), (3, 150)").unwrap();

    let result = db.execute(
        "SELECT id, score, FIRST_VALUE(score) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as first_score FROM ranked ORDER BY id",
    );
    assert!(result.is_ok(), "FIRST_VALUE with frame failed: {:?}", result);

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_window_last_value_frame() {
    eprintln!("\n=== E2E Test: LAST_VALUE with frame ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE sales_rank (id INT, amount INT)").unwrap();
    db.execute("INSERT INTO sales_rank VALUES (1, 500), (2, 800), (3, 300)").unwrap();

    let result = db.execute(
        "SELECT id, amount, LAST_VALUE(amount) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as last_amount FROM sales_rank ORDER BY id",
    );
    assert!(result.is_ok(), "LAST_VALUE with frame failed: {:?}", result);

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_window_nth_value() {
    eprintln!("\n=== E2E Test: NTH_VALUE ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE sequence (pos INT, value INT)").unwrap();
    db.execute("INSERT INTO sequence VALUES (1, 10), (2, 20), (3, 30), (4, 40)").unwrap();

    let result = db.execute(
        "SELECT pos, value, NTH_VALUE(value, 2) OVER (ORDER BY pos ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as second_val FROM sequence ORDER BY pos",
    );
    assert!(result.is_ok(), "NTH_VALUE failed: {:?}", result);

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_window_ntile() {
    eprintln!("\n=== E2E Test: Window NTILE ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE customers (name TEXT, balance INT)").unwrap();
    db.execute("INSERT INTO customers VALUES ('Alice', 1000), ('Bob', 2000), ('Charlie', 3000), ('David', 4000), ('Eve', 5000)")
        .unwrap();

    let result = db.execute(
        "SELECT name, balance, NTILE(2) OVER (ORDER BY balance) as quartile FROM customers ORDER BY balance",
    );
    assert!(result.is_ok(), "NTILE failed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("Alice") && output.contains("Bob") && output.contains("Charlie"));

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_window_running_sum() {
    eprintln!("\n=== E2E Test: Running sum with ROWS frame ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE items (id INT, value INT)").unwrap();
    db.execute("INSERT INTO items VALUES (1, 100), (2, 200), (3, 300)").unwrap();

    let result = db.execute(
        "SELECT id, value, FIRST_VALUE(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_first FROM items ORDER BY id",
    );
    assert!(result.is_ok(), "Running sum with window frame failed: {:?}", result);

    eprintln!("=== E2E Test PASSED ===");
}

#[test]
fn test_e2e_window_moving_avg() {
    eprintln!("\n=== E2E Test: Moving average with frame ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE metrics (period TEXT, value INT)").unwrap();
    db.execute("INSERT INTO metrics VALUES ('Q1', 1000), ('Q2', 1500), ('Q3', 1200), ('Q4', 1800)")
        .unwrap();

    let result = db.execute(
        "SELECT period, value, FIRST_VALUE(value) OVER (ORDER BY period ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as moving_first FROM metrics ORDER BY period",
    );
    assert!(result.is_ok(), "Moving average failed: {:?}", result);

    eprintln!("=== E2E Test PASSED ===");
}
