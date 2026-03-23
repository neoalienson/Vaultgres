// Pet Store Test Modules
pub mod cleanup;
pub mod edge_case_tests;
pub mod enum_tests;
pub mod insert_tests;
pub mod persistence_tests;
pub mod select_tests;
pub mod setup;
pub mod update_tests;

// Main test function that orchestrates all test modules
#[test]
pub fn test_pet_store_comprehensive() {
    use e2e::*;

    env_logger::builder().filter_level(log::LevelFilter::Debug).is_test(true).try_init().ok();
    eprintln!("\n=== Test: Pet Accessories Store - Comprehensive Features ===");

    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    // Verify database connection before proceeding
    db.verify_connection();

    // Setup phase
    setup::setup_schema(&db);
    setup::insert_initial_data(&db);
    setup::setup_transactions(&db);
    setup::setup_scd_data(&db);
    setup::setup_views(&db);

    // Basic functionality tests
    eprintln!("[PetStore] Testing INNER JOIN...");
    let result = db.execute(
        "SELECT c.name, o.total FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
    );
    assert!(result.is_ok());
    assert!(result.unwrap().contains("Alice"));

    // Enum type tests
    eprintln!("[PetStore] Testing ENUM types...");
    enum_tests::run_enum_tests(&db);
    enum_tests::cleanup_enum_types(&db);

    eprintln!("[PetStore] Testing LEFT JOIN...");
    let result = db
        .execute("SELECT c.name, o.id FROM customers c LEFT JOIN orders o ON c.id = o.customer_id");
    assert!(result.is_ok());

    eprintln!("[PetStore] Testing SCD current version query...");
    let result = db.execute("SELECT name, price FROM items WHERE is_current = 1");
    assert!(result.is_ok());

    eprintln!("[PetStore] Testing SCD historical query...");
    let result = db.execute("SELECT name, price, effective_from, effective_to, modified_by FROM items WHERE item_id = 1");
    assert!(result.is_ok());

    eprintln!("[PetStore] Testing subquery with AVG...");
    let result = db.execute("SELECT name, price FROM items WHERE price > (SELECT AVG(price) FROM items WHERE is_current = 1) AND is_current = 1");
    assert!(result.is_ok());

    eprintln!("[PetStore] Testing IN subquery...");
    let result =
        db.execute("SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders)");
    assert!(result.is_ok());

    eprintln!("[PetStore] Testing string functions...");
    let result = db.execute(
        "SELECT UPPER(name), LOWER(category), LENGTH(sku) FROM items WHERE is_current = 1",
    );
    assert!(result.is_ok());
    let result = db.execute("SELECT CONCAT(sku, ' - ', name) FROM items WHERE is_current = 1");
    assert!(result.is_ok());
    let result = db.execute("SELECT SUBSTRING(email, 1, 5) FROM customers");
    assert!(result.is_ok());

    eprintln!("[PetStore] Querying views...");
    let result = db.execute("SELECT * FROM current_items");
    assert!(result.is_ok());
    let result = db.execute("SELECT * FROM customer_orders");
    assert!(result.is_ok());

    eprintln!("[PetStore] Querying materialized view...");
    let result = db.execute("SELECT * FROM category_sales");
    assert!(result.is_ok());

    eprintln!("[PetStore] Testing 3-way JOIN...");
    // Test multi-table join with column prefixing - columns with same name should be preserved
    let result = db.execute("SELECT c.name, o.total, i.name as item_name FROM customers c JOIN orders o ON c.id = o.customer_id JOIN order_items oi ON o.id = oi.order_id JOIN items i ON oi.item_id = i.item_id WHERE i.is_current = 1");
    assert!(result.is_ok());

    // Edge case tests
    select_tests::run_select_tests(&db);
    insert_tests::run_insert_tests(&db);
    update_tests::run_update_tests(&db);

    // Persistence tests
    persistence_tests::run_persistence_tests(&env);

    // Additional edge case tests (after persistence tests)
    edge_case_tests::run_edge_case_tests(&env);

    // Final verification
    eprintln!("\n[PetStore] === Final Data Verification ===");
    let db = env.vaultgres();

    let result = db.execute("SELECT COUNT(*) FROM items");
    assert!(result.is_ok());

    let result = db.execute("SELECT COUNT(*) FROM customers");
    assert!(result.is_ok());

    let result = db.execute("SELECT COUNT(*) FROM orders");
    assert!(result.is_ok());

    let result = db.execute("SELECT COUNT(*) FROM order_items");
    assert!(result.is_ok());

    // Cleanup
    cleanup::cleanup(&db);

    eprintln!("[PetStore] === All Tests PASSED ===");
}
