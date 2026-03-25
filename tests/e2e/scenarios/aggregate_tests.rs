use e2e::*;

#[test]
fn test_custom_aggregate_execution() {
    eprintln!("\n=== Test: Custom Aggregate Execution (sfunc/finalfunc) ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE test_data (value INT)").unwrap();
    db.execute("INSERT INTO test_data VALUES (10), (20), (30)").unwrap();

    db.execute("CREATE AGGREGATE my_sum (INT) (SFUNC = int8pl, STYPE = INT8, INITCOND = '0')")
        .unwrap();

    let result = db.execute("SELECT my_sum(value) FROM test_data");
    assert!(result.is_ok(), "SELECT with custom aggregate failed: {:?}", result);

    eprintln!("Custom aggregate execution succeeded");
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_custom_aggregate_with_group_by() {
    eprintln!("\n=== Test: Custom Aggregate with GROUP BY ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE sales (category TEXT, amount INT)").unwrap();
    db.execute("INSERT INTO sales VALUES ('A', 100), ('B', 200), ('A', 150), ('B', 50)").unwrap();

    db.execute("CREATE AGGREGATE my_sum (INT) (SFUNC = int8pl, STYPE = INT8, INITCOND = '0')")
        .unwrap();

    let result = db.execute("SELECT category, my_sum(amount) FROM sales GROUP BY category");
    assert!(result.is_ok(), "SELECT with custom aggregate GROUP BY failed: {:?}", result);

    eprintln!("Custom aggregate with GROUP BY succeeded");
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_custom_aggregate_with_finalfunc() {
    eprintln!("\n=== Test: Custom Aggregate with FINALFUNC ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE test_data (value INT)").unwrap();
    db.execute("INSERT INTO test_data VALUES (10), (20)").unwrap();

    db.execute(
        "CREATE AGGREGATE my_avg (INT) (SFUNC = int8_avg_accum, STYPE = INT8, FINALFUNC = int8_avg)",
    )
    .unwrap();

    let result = db.execute("SELECT my_avg(value) FROM test_data");
    assert!(result.is_ok(), "SELECT with custom aggregate FINALFUNC failed: {:?}", result);

    eprintln!("Custom aggregate with FINALFUNC succeeded");
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_custom_aggregate_empty_input() {
    eprintln!("\n=== Test: Custom Aggregate with Empty Input ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE empty_data (value INT)").unwrap();

    db.execute("CREATE AGGREGATE my_sum (INT) (SFUNC = int8pl, STYPE = INT8, INITCOND = '0')")
        .unwrap();

    let result = db.execute("SELECT my_sum(value) FROM empty_data");
    assert!(result.is_ok(), "SELECT with custom aggregate on empty table failed: {:?}", result);

    eprintln!("Custom aggregate on empty input succeeded");
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_custom_aggregate_multiple() {
    eprintln!("\n=== Test: Multiple Custom Aggregates ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE test_data (value INT)").unwrap();
    db.execute("INSERT INTO test_data VALUES (10), (20)").unwrap();

    db.execute("CREATE AGGREGATE my_count (INT) (SFUNC = int8pl, STYPE = INT8, INITCOND = '0')")
        .unwrap();
    db.execute("CREATE AGGREGATE my_sum (INT) (SFUNC = int8pl, STYPE = INT8, INITCOND = '0')")
        .unwrap();

    let result = db.execute("SELECT my_count(value), my_sum(value) FROM test_data");
    assert!(result.is_ok(), "SELECT with multiple custom aggregates failed: {:?}", result);

    eprintln!("Multiple custom aggregates succeeded");
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_custom_aggregate_mixed_with_builtin() {
    eprintln!("\n=== Test: Custom Aggregate Mixed with Built-in ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE test_data (value INT)").unwrap();
    db.execute("INSERT INTO test_data VALUES (10), (20), (30)").unwrap();

    db.execute("CREATE AGGREGATE my_sum (INT) (SFUNC = int8pl, STYPE = INT8, INITCOND = '0')")
        .unwrap();

    let result = db.execute("SELECT COUNT(*), SUM(value), my_sum(value) FROM test_data");
    assert!(result.is_ok(), "SELECT with custom and built-in aggregate failed: {:?}", result);

    eprintln!("Custom and built-in aggregate mixed succeeded");
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_custom_aggregate_with_nulls() {
    eprintln!("\n=== Test: Custom Aggregate with NULL Values ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE test_data (value INT)").unwrap();
    db.execute("INSERT INTO test_data VALUES (10), (NULL), (20)").unwrap();

    db.execute("CREATE AGGREGATE my_sum (INT) (SFUNC = int8pl, STYPE = INT8, INITCOND = '0')")
        .unwrap();

    let result = db.execute("SELECT my_sum(value) FROM test_data");
    assert!(result.is_ok(), "SELECT with custom aggregate and NULLs failed: {:?}", result);

    eprintln!("Custom aggregate with NULL values succeeded");
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_create_aggregate() {
    eprintln!("\n=== Test: CREATE AGGREGATE ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute(
        "CREATE AGGREGATE my_avg (INT) (SFUNC = int8_avg_accum, STYPE = INT8, FINALFUNC = int8_avg)",
    );
    assert!(result.is_ok(), "CREATE AGGREGATE failed: {:?}", result);
    eprintln!("CREATE AGGREGATE succeeded");
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_create_aggregate_with_initcond() {
    eprintln!("\n=== Test: CREATE AGGREGATE with INITCOND ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute(
        "CREATE AGGREGATE my_count (INT) (SFUNC = int8_avg_accum, STYPE = INT8, INITCOND = 0)",
    );
    assert!(result.is_ok(), "CREATE AGGREGATE failed: {:?}", result);
    eprintln!("CREATE AGGREGATE with INITCOND succeeded");
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_drop_aggregate() {
    eprintln!("\n=== Test: DROP AGGREGATE ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute(
        "CREATE AGGREGATE my_avg (INT) (SFUNC = int8_avg_accum, STYPE = INT8, FINALFUNC = int8_avg)",
    );
    assert!(result.is_ok(), "CREATE AGGREGATE failed: {:?}", result);

    let result = db.execute("DROP AGGREGATE my_avg");
    assert!(result.is_ok(), "DROP AGGREGATE failed: {:?}", result);
    eprintln!("DROP AGGREGATE succeeded");
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_drop_aggregate_if_exists() {
    eprintln!("\n=== Test: DROP AGGREGATE IF EXISTS ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute("DROP AGGREGATE IF EXISTS nonexistent");
    assert!(result.is_ok(), "DROP AGGREGATE IF EXISTS failed: {:?}", result);
    eprintln!("DROP AGGREGATE IF EXISTS succeeded");
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_drop_aggregate_if_exists_false() {
    eprintln!("\n=== Test: DROP AGGREGATE (without IF EXISTS, should fail) ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute("DROP AGGREGATE nonexistent");
    assert!(result.is_err(), "DROP AGGREGATE should have failed");
    eprintln!("DROP AGGREGATE correctly failed for nonexistent aggregate");
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_multiple_aggregates() {
    eprintln!("\n=== Test: Multiple aggregates ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute(
        "CREATE AGGREGATE my_avg (INT) (SFUNC = int8_avg_accum, STYPE = INT8, FINALFUNC = int8_avg)",
    );
    assert!(result.is_ok(), "CREATE AGGREGATE my_avg failed: {:?}", result);

    let result = db.execute("CREATE AGGREGATE my_sum (INT) (SFUNC = int8_sum, STYPE = INT8)");
    assert!(result.is_ok(), "CREATE AGGREGATE my_sum failed: {:?}", result);

    eprintln!("Multiple aggregates created successfully");
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_aggregate_overwrite() {
    eprintln!("\n=== Test: Aggregate overwrite ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute("CREATE AGGREGATE my_agg (INT) (SFUNC = func_v1, STYPE = INT8)");
    assert!(result.is_ok(), "CREATE AGGREGATE failed: {:?}", result);

    let result = db.execute("CREATE AGGREGATE my_agg (INT) (SFUNC = func_v2, STYPE = INT8)");
    assert!(result.is_ok(), "CREATE AGGREGATE overwrite failed: {:?}", result);

    eprintln!("Aggregate overwrite succeeded");
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_aggregate_case_insensitive() {
    eprintln!("\n=== Test: Aggregate case insensitive ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute("create aggregate my_avg (INT) (SFUNC = int8_avg_accum, STYPE = INT8)");
    assert!(result.is_ok(), "create aggregate (lowercase) failed: {:?}", result);

    let result = db.execute("DROP AGGREGATE MY_AVG");
    assert!(result.is_ok(), "DROP AGGREGATE (uppercase) failed: {:?}", result);

    eprintln!("Aggregate case insensitive succeeded");
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_aggregate_with_volatility() {
    eprintln!("\n=== Test: Aggregate with volatility ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db
        .execute("CREATE AGGREGATE my_avg (INT) (SFUNC = int8_avg_accum, STYPE = INT8) IMMUTABLE");
    assert!(result.is_ok(), "CREATE AGGREGATE with IMMUTABLE failed: {:?}", result);

    let result =
        db.execute("CREATE AGGREGATE my_sum (INT) (SFUNC = int8_sum, STYPE = INT8) STABLE");
    assert!(result.is_ok(), "CREATE AGGREGATE with STABLE failed: {:?}", result);

    let result =
        db.execute("CREATE AGGREGATE my_count (INT) (SFUNC = int8_count, STYPE = INT8) VOLATILE");
    assert!(result.is_ok(), "CREATE AGGREGATE with VOLATILE failed: {:?}", result);

    eprintln!("Aggregate with volatility succeeded");
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_aggregate_with_cost() {
    eprintln!("\n=== Test: Aggregate with COST ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result =
        db.execute("CREATE AGGREGATE my_avg (INT) (SFUNC = int8_avg_accum, STYPE = INT8) COST 500");
    assert!(result.is_ok(), "CREATE AGGREGATE with COST failed: {:?}", result);

    eprintln!("Aggregate with COST succeeded");
    eprintln!("=== Test PASSED ===");
}
