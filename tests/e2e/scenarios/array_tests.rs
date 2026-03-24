use e2e::*;

#[test]
fn test_create_table_with_int_array() {
    eprintln!("\n=== Test: CREATE TABLE with INT ARRAY ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute("CREATE TABLE scores (id INT, name TEXT, grades INT[])");
    assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_create_table_with_text_array() {
    eprintln!("\n=== Test: CREATE TABLE with TEXT ARRAY ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    let result = db.execute("CREATE TABLE tags (id INT, name TEXT, labels TEXT[])");
    assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_insert_array_bracket_syntax() {
    eprintln!("\n=== Test: INSERT with ARRAY[...] syntax ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE scores (id INT, name TEXT, grades INT[])").unwrap();

    let result = db.execute("INSERT INTO scores VALUES (1, 'Alice', ARRAY[85, 90, 78])");
    assert!(result.is_ok(), "INSERT failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_insert_array_literal_syntax() {
    eprintln!("\n=== Test: INSERT with {{...}} literal syntax ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE scores (id INT, name TEXT, grades INT[])").unwrap();

    let result = db.execute("INSERT INTO scores VALUES (1, 'Alice', '{85, 90, 78}')");
    assert!(result.is_ok(), "INSERT failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_insert_empty_array() {
    eprintln!("\n=== Test: INSERT empty array ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE scores (id INT, name TEXT, grades INT[])").unwrap();

    let result = db.execute("INSERT INTO scores VALUES (1, 'Bob', '{}')");
    assert!(result.is_ok(), "INSERT failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_select_array_element_access() {
    eprintln!("\n=== Test: SELECT array[1] element access ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE scores (id INT, name TEXT, grades INT[])").unwrap();
    db.execute("INSERT INTO scores VALUES (1, 'Alice', ARRAY[85, 90, 78])").unwrap();

    let result = db.execute("SELECT grades[1] FROM scores WHERE id = 1");
    assert!(result.is_ok(), "SELECT failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_select_array_with_contains_operator() {
    eprintln!("\n=== Test: SELECT with @> containment operator ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE scores (id INT, name TEXT, grades INT[])").unwrap();
    db.execute("INSERT INTO scores VALUES (1, 'Alice', ARRAY[85, 90, 78])").unwrap();
    db.execute("INSERT INTO scores VALUES (2, 'Bob', ARRAY[70, 75])").unwrap();

    let result = db.execute("SELECT name FROM scores WHERE grades @> ARRAY[90]");
    assert!(result.is_ok(), "SELECT failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_select_array_with_overlaps_operator() {
    eprintln!("\n=== Test: SELECT with && overlaps operator ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE scores (id INT, name TEXT, grades INT[])").unwrap();
    db.execute("INSERT INTO scores VALUES (1, 'Alice', ARRAY[85, 90, 78])").unwrap();
    db.execute("INSERT INTO scores VALUES (2, 'Bob', ARRAY[70, 75, 80])").unwrap();

    let result = db.execute("SELECT name FROM scores WHERE grades && ARRAY[75, 80]");
    assert!(result.is_ok(), "SELECT failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_array_concat_operator() {
    eprintln!("\n=== Test: Array concat with || operator ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE scores (id INT, name TEXT, grades INT[])").unwrap();
    db.execute("INSERT INTO scores VALUES (1, 'Alice', ARRAY[85, 90])").unwrap();

    let result = db.execute("SELECT grades || ARRAY[100] FROM scores WHERE id = 1");
    assert!(result.is_ok(), "SELECT failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_array_length_function() {
    eprintln!("\n=== Test: array_length() function ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE scores (id INT, name TEXT, grades INT[])").unwrap();
    db.execute("INSERT INTO scores VALUES (1, 'Alice', ARRAY[85, 90, 78])").unwrap();

    let result = db.execute("SELECT array_length(grades, 1) FROM scores WHERE id = 1");
    assert!(result.is_ok(), "SELECT failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_array_append_function() {
    eprintln!("\n=== Test: array_append() function ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE scores (id INT, name TEXT, grades INT[])").unwrap();
    db.execute("INSERT INTO scores VALUES (1, 'Alice', ARRAY[85, 90])").unwrap();

    let result = db.execute("SELECT array_append(grades, 100) FROM scores WHERE id = 1");
    assert!(result.is_ok(), "SELECT failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_array_with_null_elements() {
    eprintln!("\n=== Test: Array with NULL elements ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE scores (id INT, name TEXT, grades INT[])").unwrap();

    let result = db.execute("INSERT INTO scores VALUES (1, 'Charlie', ARRAY[85, NULL, 90])");
    assert!(result.is_ok(), "INSERT failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_nested_array_access() {
    eprintln!("\n=== Test: Nested array access arr[1][2] ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE matrix (id INT, data INT[][])").unwrap();
    db.execute("INSERT INTO matrix VALUES (1, ARRAY[ARRAY[1, 2], ARRAY[3, 4]])").unwrap();

    let result = db.execute("SELECT data[1][2] FROM matrix WHERE id = 1");
    assert!(result.is_ok(), "SELECT failed: {:?}", result);
    eprintln!("=== Test PASSED ===");
}
