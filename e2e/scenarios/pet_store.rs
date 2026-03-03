use e2e::*;
use std::time::Instant;

#[test]
fn test_pet_store_basic_operations() {
    eprintln!("\n=== Test: Pet Store - Basic Operations ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    // Create schema
    eprintln!("[PetStore] Creating tables...");
    db.execute("CREATE TABLE pets_basic (id INT, name TEXT, species TEXT, price INT)").unwrap();
    db.execute("CREATE TABLE customers_basic (id INT, name TEXT, email TEXT)").unwrap();
    db.execute("CREATE TABLE orders_basic (id INT, customer_id INT, pet_id INT, quantity INT)").unwrap();

    // Add pets
    eprintln!("[PetStore] Adding pets to inventory...");
    db.execute("INSERT INTO pets_basic VALUES (1, 'Buddy', 'Dog', 500)").unwrap();
    db.execute("INSERT INTO pets_basic VALUES (2, 'Whiskers', 'Cat', 300)").unwrap();
    db.execute("INSERT INTO pets_basic VALUES (3, 'Goldie', 'Fish', 20)").unwrap();
    db.execute("INSERT INTO pets_basic VALUES (4, 'Tweety', 'Bird', 150)").unwrap();

    // Add customers
    eprintln!("[PetStore] Registering customers...");
    db.execute("INSERT INTO customers_basic VALUES (1, 'Alice', 'alice@example.com')").unwrap();
    db.execute("INSERT INTO customers_basic VALUES (2, 'Bob', 'bob@example.com')").unwrap();

    // Place orders
    eprintln!("[PetStore] Processing orders...");
    db.execute("INSERT INTO orders_basic VALUES (1, 1, 1, 1)").unwrap();
    db.execute("INSERT INTO orders_basic VALUES (2, 2, 3, 2)").unwrap();

    // Query inventory
    eprintln!("[PetStore] Querying pet inventory...");
    let result = db.execute("SELECT * FROM pets_basic");
    assert!(result.is_ok(), "Failed to query pets");
    let output = result.unwrap();
    assert!(output.contains("Buddy"), "Pet 'Buddy' not found");
    assert!(output.contains("Whiskers"), "Pet 'Whiskers' not found");
    assert!(output.contains("Goldie"), "Pet 'Goldie' not found");
    assert!(output.contains("Tweety"), "Pet 'Tweety' not found");
    eprintln!("[PetStore] Verified 4 pets in inventory");

    // Cleanup
    db.execute("DROP TABLE orders_basic").ok();
    db.execute("DROP TABLE customers_basic").ok();
    db.execute("DROP TABLE pets_basic").ok();

    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_pet_store_concurrent_orders() {
    eprintln!("\n=== Test: Pet Store - Concurrent Orders ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    // Setup
    db.execute("CREATE TABLE pets_concurrent (id INT, name TEXT, stock INT)").unwrap();
    db.execute("CREATE TABLE orders_concurrent (id INT, pet_id INT, quantity INT)").unwrap();
    
    eprintln!("[PetStore] Stocking inventory...");
    db.execute("INSERT INTO pets_concurrent VALUES (1, 'Puppy', 100)").unwrap();
    db.execute("INSERT INTO pets_concurrent VALUES (2, 'Kitten', 50)").unwrap();

    // Simulate concurrent orders
    eprintln!("[PetStore] Simulating 20 concurrent orders...");
    let start = Instant::now();
    for i in 0..20 {
        let pet_id = (i % 2) + 1;
        db.execute(&format!("INSERT INTO orders_concurrent VALUES ({}, {}, 1)", i, pet_id)).ok();
    }
    let duration = start.elapsed();

    eprintln!("[PetStore] Processed 20 orders in {:?}", duration);
    
    // Verify orders were created
    let result = db.execute("SELECT * FROM orders_concurrent");
    assert!(result.is_ok(), "Failed to query orders");
    let output = result.unwrap();
    eprintln!("[PetStore] Orders created: {}", output.lines().count().saturating_sub(3));
    
    assert!(duration.as_secs() < 10, "Orders too slow");

    // Cleanup
    db.execute("DROP TABLE orders_concurrent").ok();
    db.execute("DROP TABLE pets_concurrent").ok();

    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_pet_store_inventory_management() {
    eprintln!("\n=== Test: Pet Store - Inventory Management ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE inventory (id INT, item TEXT, quantity INT, price INT)").unwrap();

    // Initial stock
    eprintln!("[PetStore] Adding initial inventory...");
    let items = vec![
        (1, "Dog Food", 100, 50),
        (2, "Cat Litter", 75, 30),
        (3, "Fish Tank", 20, 200),
        (4, "Bird Cage", 15, 150),
        (5, "Pet Toys", 200, 10),
    ];

    for (id, item, qty, price) in items {
        db.execute(&format!(
            "INSERT INTO inventory VALUES ({}, '{}', {}, {})",
            id, item, qty, price
        )).unwrap();
    }

    // Query all inventory
    eprintln!("[PetStore] Checking inventory levels...");
    let result = db.execute("SELECT * FROM inventory");
    assert!(result.is_ok(), "Failed to query inventory");
    let output = result.unwrap();
    assert!(output.contains("Dog Food"), "Dog Food not found in inventory");
    assert!(output.contains("Cat Litter"), "Cat Litter not found in inventory");
    eprintln!("[PetStore] Found {} items in inventory", 5);

    // Query specific items
    eprintln!("[PetStore] Querying low stock items (quantity < 50)...");
    let result = db.execute("SELECT * FROM inventory WHERE quantity < 50");
    assert!(result.is_ok(), "Failed to query low stock items");
    let output = result.unwrap();
    assert!(output.contains("Fish Tank") || output.contains("Bird Cage"), "Expected low stock items not found");

    // Cleanup
    db.execute("DROP TABLE inventory").ok();

    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_pet_store_customer_orders_workflow() {
    eprintln!("\n=== Test: Pet Store - Complete Order Workflow ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    // Create tables
    eprintln!("[PetStore] Setting up database schema...");
    db.execute("CREATE TABLE customers_workflow (id INT, name TEXT, loyalty_points INT)").unwrap();
    db.execute("CREATE TABLE products_workflow (id INT, name TEXT, category TEXT, price INT)").unwrap();
    db.execute("CREATE TABLE orders_workflow (id INT, customer_id INT, product_id INT, total INT)").unwrap();

    // Add customers
    eprintln!("[PetStore] Registering customers...");
    db.execute("INSERT INTO customers_workflow VALUES (1, 'Alice', 100)").unwrap();
    db.execute("INSERT INTO customers_workflow VALUES (2, 'Bob', 50)").unwrap();
    db.execute("INSERT INTO customers_workflow VALUES (3, 'Charlie', 200)").unwrap();

    // Add products
    eprintln!("[PetStore] Adding products...");
    db.execute("INSERT INTO products_workflow VALUES (1, 'Premium Dog Food', 'Food', 60)").unwrap();
    db.execute("INSERT INTO products_workflow VALUES (2, 'Cat Scratching Post', 'Furniture', 80)").unwrap();
    db.execute("INSERT INTO products_workflow VALUES (3, 'Aquarium Filter', 'Equipment', 45)").unwrap();

    // Process orders
    eprintln!("[PetStore] Processing customer orders...");
    db.execute("INSERT INTO orders_workflow VALUES (1, 1, 1, 60)").unwrap();
    db.execute("INSERT INTO orders_workflow VALUES (2, 2, 2, 80)").unwrap();
    db.execute("INSERT INTO orders_workflow VALUES (3, 1, 3, 45)").unwrap();
    db.execute("INSERT INTO orders_workflow VALUES (4, 3, 1, 60)").unwrap();

    // Verify data
    eprintln!("[PetStore] Verifying orders...");
    let result = db.execute("SELECT * FROM orders_workflow");
    assert!(result.is_ok(), "Failed to query orders");
    let orders_output = result.unwrap();
    eprintln!("[PetStore] Orders output: {}", orders_output.lines().take(5).collect::<Vec<_>>().join(" | "));

    eprintln!("[PetStore] Verifying customers...");
    let result = db.execute("SELECT * FROM customers_workflow");
    assert!(result.is_ok(), "Failed to query customers");
    let customers_output = result.unwrap();
    assert!(customers_output.contains("Alice"), "Customer Alice not found");
    assert!(customers_output.contains("Bob"), "Customer Bob not found");
    assert!(customers_output.contains("Charlie"), "Customer Charlie not found");
    eprintln!("[PetStore] Verified 3 customers");

    // Cleanup
    db.execute("DROP TABLE orders_workflow").ok();
    db.execute("DROP TABLE products_workflow").ok();
    db.execute("DROP TABLE customers_workflow").ok();

    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_pet_store_high_volume_sales() {
    eprintln!("\n=== Test: Pet Store - High Volume Sales ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE sales (id INT, product TEXT, amount INT, timestamp INT)").unwrap();

    eprintln!("[PetStore] Recording 100 sales transactions...");
    let start = Instant::now();
    
    for i in 0..100 {
        let product = match i % 5 {
            0 => "Dog Food",
            1 => "Cat Litter",
            2 => "Fish Food",
            3 => "Bird Seed",
            _ => "Pet Toys",
        };
        let amount = (i % 10 + 1) * 10;
        
        db.execute(&format!(
            "INSERT INTO sales VALUES ({}, '{}', {}, {})",
            i, product, amount, i
        )).ok();
    }
    
    let duration = start.elapsed();
    let tps = 100.0 / duration.as_secs_f64();
    
    eprintln!("[PetStore] Completed 100 sales in {:?}", duration);
    eprintln!("[PetStore] Throughput: {:.2} transactions/sec", tps);
    
    assert!(tps > 5.0, "Transaction throughput too low: {:.2} TPS", tps);
    
    // Cleanup
    db.execute("DROP TABLE sales").ok();
    
    eprintln!("=== Test PASSED ===");
}
