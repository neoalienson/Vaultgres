// Pet Store Enum Tests - Testing PostgreSQL-compatible Enum types
use e2e::*;

pub fn run_enum_tests(db: &DbConnection) {
    eprintln!("[PetStore] Testing Enum Types...");

    test_create_enum_type(db);
    test_enum_in_table_creation(db);
    test_enum_insert_and_select(db);
    test_enum_comparison(db);
    test_enum_in_expressions(db);

    eprintln!("[PetStore] Enum tests completed.");
}

fn test_create_enum_type(db: &DbConnection) {
    eprintln!("[PetStore] Testing CREATE TYPE AS ENUM...");

    // Create an enum type for order status
    let result = db.execute(
        "CREATE TYPE order_status AS ENUM ('pending', 'processing', 'shipped', 'delivered', 'cancelled')"
    );
    assert!(result.is_ok(), "Failed to create enum type: {:?}", result);
    eprintln!("[PetStore]   Created order_status enum");
}

fn test_enum_in_table_creation(db: &DbConnection) {
    eprintln!("[PetStore] Testing ENUM in table creation...");

    // Create a table that uses the enum type
    let result = db.execute(
        "CREATE TABLE orders_with_status (id INT, customer_id INT, status order_status, total INT)",
    );
    assert!(result.is_ok(), "Failed to create table with enum column: {:?}", result);
    eprintln!("[PetStore]   Created orders_with_status table with enum column");
}

fn test_enum_insert_and_select(db: &DbConnection) {
    eprintln!("[PetStore] Testing ENUM insert and select...");

    // Insert with enum values
    let result = db.execute(
        "INSERT INTO orders_with_status VALUES (1, 1, 'pending', 4500), (2, 1, 'processing', 899), (3, 2, 'shipped', 1599)"
    );
    assert!(result.is_ok(), "Failed to insert enum values: {:?}", result);
    eprintln!("[PetStore]   Inserted rows with enum values");

    // Select and verify enum values are returned correctly
    let result = db.execute("SELECT id, status FROM orders_with_status WHERE customer_id = 1");
    assert!(result.is_ok(), "Failed to select enum values: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("pending"), "Expected 'pending' in result: {}", output);
    assert!(output.contains("processing"), "Expected 'processing' in result: {}", output);
    eprintln!("[PetStore]   Verified enum values in select result");
}

fn test_enum_comparison(db: &DbConnection) {
    eprintln!("[PetStore] Testing ENUM comparison...");

    // Test enum comparison in WHERE clause - use SELECT * to see the status column
    let result =
        db.execute("SELECT id, status, total FROM orders_with_status WHERE status = 'shipped'");
    assert!(result.is_ok(), "Failed to compare enum: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("shipped"), "Expected 'shipped' in result: {}", output);
    eprintln!("[PetStore]   Verified enum comparison works");

    // Test enum comparison with !=
    let result = db.execute("SELECT COUNT(*) FROM orders_with_status WHERE status != 'cancelled'");
    assert!(result.is_ok(), "Failed to compare enum with !=: {:?}", result);
    let output = result.unwrap();
    // Should return 3 rows (pending, processing, shipped) since we have 3 non-cancelled orders
    assert!(output.contains("3"), "Expected count of 3 for != 'cancelled': {}", output);
    eprintln!("[PetStore]   Verified enum not-equal comparison works");
}

fn test_enum_in_expressions(db: &DbConnection) {
    eprintln!("[PetStore] Testing ENUM in expressions...");

    // Test enum with IS NULL
    let result = db.execute("SELECT id FROM orders_with_status WHERE status IS NOT NULL");
    assert!(result.is_ok(), "Failed to check enum IS NOT NULL: {:?}", result);
    eprintln!("[PetStore]   Verified enum IS NOT NULL works");

    // Test enum with IN clause
    let result = db.execute(
        "SELECT id, status FROM orders_with_status WHERE status IN ('pending', 'processing')",
    );
    assert!(result.is_ok(), "Failed to check enum IN clause: {:?}", result);
    let output = result.unwrap();
    assert!(
        output.contains("pending") || output.contains("processing"),
        "Expected enum values in IN result: {}",
        output
    );
    eprintln!("[PetStore]   Verified enum IN clause works");
}

pub fn cleanup_enum_types(db: &DbConnection) {
    eprintln!("[PetStore] Cleaning up enum types...");
    let _ = db.execute("DROP TABLE IF EXISTS orders_with_status");
    let _ = db.execute("DROP TYPE IF EXISTS order_status");
}
