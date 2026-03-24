use e2e::*;

#[test]
fn test_create_composite_type() {
    eprintln!("\n=== Test: CREATE TYPE AS (composite type) ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute(
        "CREATE TYPE address AS (street VARCHAR(100), city VARCHAR(50), state VARCHAR(50))",
    );
    assert!(result.is_ok(), "CREATE TYPE failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_create_table_with_composite_column() {
    eprintln!("\n=== Test: CREATE TABLE with composite type column ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TYPE address AS (street VARCHAR(100), city VARCHAR(50), state VARCHAR(50))")
        .unwrap();

    let result = db.execute("CREATE TABLE people (id INT, home_address address)");
    assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_insert_into_composite_table() {
    eprintln!("\n=== Test: INSERT into table with composite type ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TYPE address AS (street VARCHAR(100), city VARCHAR(50), state VARCHAR(50))")
        .unwrap();
    db.execute("CREATE TABLE people (id INT, home_address address)").unwrap();

    let result = db.execute("INSERT INTO people VALUES (1, '(123 Main St, NYC, NY)')");
    assert!(result.is_ok(), "INSERT failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_select_from_composite_table() {
    eprintln!("\n=== Test: SELECT from table with composite type ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TYPE address AS (street VARCHAR(100), city VARCHAR(50), state VARCHAR(50))")
        .unwrap();
    db.execute("CREATE TABLE people (id INT, home_address address)").unwrap();
    db.execute("INSERT INTO people VALUES (1, '(123 Main St, NYC, NY)')").unwrap();

    let result = db.execute("SELECT * FROM people");
    assert!(result.is_ok(), "SELECT failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_composite_type_with_enum() {
    eprintln!("\n=== Test: Composite type with enum field ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TYPE priority AS ENUM ('low', 'medium', 'high')").unwrap();
    db.execute("CREATE TYPE request AS (title VARCHAR(100), priority priority)").unwrap();

    let result = db.execute("CREATE TABLE requests (id INT, req request)");
    assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_drop_composite_type() {
    eprintln!("\n=== Test: DROP composite type ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TYPE address AS (street VARCHAR(100))").unwrap();

    let result = db.execute("DROP TYPE address");
    assert!(result.is_ok(), "DROP TYPE failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_drop_composite_type_cascade() {
    eprintln!("\n=== Test: DROP composite type with CASCADE ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TYPE address AS (street VARCHAR(100))").unwrap();
    db.execute("CREATE TABLE people (id INT, addr address)").unwrap();

    let result = db.execute("DROP TYPE address CASCADE");
    assert!(result.is_ok(), "DROP TYPE CASCADE failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_multiple_composite_types() {
    eprintln!("\n=== Test: Multiple composite types ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TYPE address AS (street VARCHAR(100), city VARCHAR(50))").unwrap();
    db.execute("CREATE TYPE contact AS (email VARCHAR(100), phone VARCHAR(20))").unwrap();

    let result = db.execute("CREATE TABLE companies (id INT, addr address, contact contact)");
    assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}
