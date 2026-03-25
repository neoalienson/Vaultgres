use e2e::*;

#[test]
fn test_sql_function_create_and_call() {
    eprintln!("\n=== Test: Create and call SQL function ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute(
        "CREATE FUNCTION add_int(a INT, b INT) RETURNS INT LANGUAGE SQL AS 'SELECT $1 + $2'",
    );
    assert!(result.is_ok(), "CREATE FUNCTION failed: {:?}", result);
    eprintln!("CREATE FUNCTION succeeded");

    let result = db.execute("SELECT add_int(5, 3)");
    assert!(result.is_ok(), "SELECT function failed: {:?}", result);
    eprintln!("SELECT function succeeded: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_sql_function_with_text_concat() {
    eprintln!("\n=== Test: SQL function with text concat ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute("CREATE FUNCTION greet(name TEXT) RETURNS TEXT LANGUAGE SQL AS 'SELECT CONCAT(''Hello, '', $1)'");
    assert!(result.is_ok(), "CREATE FUNCTION failed: {:?}", result);

    let result = db.execute("SELECT greet('World')");
    assert!(result.is_ok(), "SELECT function failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_sql_function_with_null() {
    eprintln!("\n=== Test: SQL function with NULL ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute(
        "CREATE FUNCTION add_int(a INT, b INT) RETURNS INT LANGUAGE SQL AS 'SELECT $1 + $2'",
    );
    assert!(result.is_ok(), "CREATE FUNCTION failed: {:?}", result);

    let result = db.execute("SELECT add_int(5, NULL)");
    assert!(result.is_ok(), "SELECT function failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_sql_function_multiple_params() {
    eprintln!("\n=== Test: SQL function with multiple params ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute("CREATE FUNCTION calculate(a INT, b INT, c INT) RETURNS INT LANGUAGE SQL AS 'SELECT ($1 + $2) * $3'");
    assert!(result.is_ok(), "CREATE FUNCTION failed: {:?}", result);

    let result = db.execute("SELECT calculate(1, 2, 3)");
    assert!(result.is_ok(), "SELECT function failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_sql_function_bool_return() {
    eprintln!("\n=== Test: SQL function returning BOOL ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db
        .execute("CREATE FUNCTION is_positive(n INT) RETURNS BOOL LANGUAGE SQL AS 'SELECT $1 > 0'");
    assert!(result.is_ok(), "CREATE FUNCTION failed: {:?}", result);

    let result = db.execute("SELECT is_positive(5)");
    assert!(result.is_ok(), "SELECT function failed: {:?}", result);

    let result = db.execute("SELECT is_positive(-5)");
    assert!(result.is_ok(), "SELECT function failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_sql_function_constant_return() {
    eprintln!("\n=== Test: SQL function returning constant ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute("CREATE FUNCTION get_value() RETURNS INT LANGUAGE SQL AS 'SELECT 42'");
    assert!(result.is_ok(), "CREATE FUNCTION failed: {:?}", result);

    let result = db.execute("SELECT get_value()");
    assert!(result.is_ok(), "SELECT function failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_drop_function() {
    eprintln!("\n=== Test: DROP FUNCTION ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute(
        "CREATE FUNCTION add_int(a INT, b INT) RETURNS INT LANGUAGE SQL AS 'SELECT $1 + $2'",
    );
    assert!(result.is_ok(), "CREATE FUNCTION failed: {:?}", result);

    let result = db.execute("DROP FUNCTION add_int");
    assert!(result.is_ok(), "DROP FUNCTION failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_drop_function_if_exists() {
    eprintln!("\n=== Test: DROP FUNCTION IF EXISTS ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute("DROP FUNCTION IF EXISTS nonexistent_func");
    assert!(result.is_ok(), "DROP FUNCTION IF EXISTS failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_sql_function_in_query() {
    eprintln!("\n=== Test: SQL function in query ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE test_table (id INT, value INT)").unwrap();
    db.execute("INSERT INTO test_table VALUES (1, 10), (2, 20), (3, 30)").unwrap();

    let result = db
        .execute("CREATE FUNCTION double_value(n INT) RETURNS INT LANGUAGE SQL AS 'SELECT $1 * 2'");
    assert!(result.is_ok(), "CREATE FUNCTION failed: {:?}", result);

    let result = db.execute("SELECT id, double_value(value) FROM test_table ORDER BY id");
    assert!(result.is_ok(), "SELECT with function failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_sql_function_subquery() {
    eprintln!("\n=== Test: SQL function with subquery ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE items (id INT, name TEXT)").unwrap();
    db.execute("INSERT INTO items VALUES (1, 'Apple'), (2, 'Banana'), (3, 'Cherry')").unwrap();

    let result = db.execute(
        "CREATE FUNCTION count_items() RETURNS INT LANGUAGE SQL AS 'SELECT COUNT(*) FROM items'",
    );
    assert!(result.is_ok(), "CREATE FUNCTION failed: {:?}", result);

    let result = db.execute("SELECT count_items()");
    assert!(result.is_ok(), "SELECT function failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_sql_function_volatility() {
    eprintln!("\n=== Test: SQL function with volatility ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute(
        "CREATE FUNCTION immutable_func() RETURNS INT LANGUAGE SQL IMMUTABLE AS 'SELECT 1'",
    );
    assert!(result.is_ok(), "CREATE FUNCTION IMMUTABLE failed: {:?}", result);

    let result =
        db.execute("CREATE FUNCTION stable_func() RETURNS INT LANGUAGE SQL STABLE AS 'SELECT 1'");
    assert!(result.is_ok(), "CREATE FUNCTION STABLE failed: {:?}", result);

    let result = db
        .execute("CREATE FUNCTION volatile_func() RETURNS INT LANGUAGE SQL VOLATILE AS 'SELECT 1'");
    assert!(result.is_ok(), "CREATE FUNCTION VOLATILE failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_sql_function_cost_hint() {
    eprintln!("\n=== Test: SQL function with COST hint ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute(
        "CREATE FUNCTION expensive_func() RETURNS INT LANGUAGE SQL COST 1000 AS 'SELECT 1'",
    );
    assert!(result.is_ok(), "CREATE FUNCTION with COST failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}
