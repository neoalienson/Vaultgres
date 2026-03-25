use e2e::*;

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
