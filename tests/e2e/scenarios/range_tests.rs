use e2e::*;

#[test]
fn test_create_table_with_int4range() {
    eprintln!("\n=== Test: CREATE TABLE with INT4RANGE ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute("CREATE TABLE inventory (id INT, qty_range INT4RANGE)");
    assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_create_table_with_numrange() {
    eprintln!("\n=== Test: CREATE TABLE with NUMRANGE ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute("CREATE TABLE products (id INT, price_range NUMRANGE)");
    assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_create_table_with_daterange() {
    eprintln!("\n=== Test: CREATE TABLE with DATERANGE ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute("CREATE TABLE events (id INT, date_range DATERANGE)");
    assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_create_table_with_tsrange() {
    eprintln!("\n=== Test: CREATE TABLE with TSRANGE ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute("CREATE TABLE reservations (id INT, time_slot TSRANGE)");
    assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_create_table_with_tstzrange() {
    eprintln!("\n=== Test: CREATE TABLE with TSTZRANGE ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute("CREATE TABLE sessions (id INT, time_range TSTZRANGE)");
    assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_create_table_with_int8range() {
    eprintln!("\n=== Test: CREATE TABLE with INT8RANGE ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute("CREATE TABLE big_inventory (id INT, qty_range INT8RANGE)");
    assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_insert_into_int4range_table() {
    eprintln!("\n=== Test: INSERT into INT4RANGE table ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE inventory (id INT, qty_range INT4RANGE)").unwrap();

    let result = db.execute("INSERT INTO inventory VALUES (1, '[10,50)')");
    assert!(result.is_ok(), "INSERT failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_insert_into_numrange_table() {
    eprintln!("\n=== Test: INSERT into NUMRANGE table ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE products (id INT, price_range NUMRANGE)").unwrap();

    let result = db.execute("INSERT INTO products VALUES (1, '[10.5,50.5)')");
    assert!(result.is_ok(), "INSERT failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_select_from_int4range_table() {
    eprintln!("\n=== Test: SELECT from INT4RANGE table ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE inventory (id INT, qty_range INT4RANGE)").unwrap();
    db.execute("INSERT INTO inventory VALUES (1, '[10,50)')").unwrap();

    let result = db.execute("SELECT * FROM inventory");
    assert!(result.is_ok(), "SELECT failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_multiple_range_types_in_same_table() {
    eprintln!("\n=== Test: Multiple range types in same table ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute("CREATE TABLE analytics (id INT, qty_range INT4RANGE, price_range NUMRANGE, date_range DATERANGE)");
    assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}
