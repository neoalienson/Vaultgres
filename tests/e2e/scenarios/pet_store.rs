use e2e::*;
use std::time::Instant;

#[test]
fn test_pet_store_comprehensive() {
    env_logger::builder().filter_level(log::LevelFilter::Debug).is_test(true).try_init().ok();
    eprintln!("\n=== Test: Pet Accessories Store - Comprehensive Features ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    // Create SCD Type 2 tables with indexes
    eprintln!("[PetStore] Creating SCD Type 2 schema with indexes...");
    db.execute("CREATE TABLE items (item_id INT, sku TEXT, name TEXT, category TEXT, price INT, supplier TEXT, effective_from INT, effective_to INT, is_current INT, modified_by TEXT)").unwrap();
    db.execute("CREATE INDEX idx_items_sku ON items(sku)").unwrap();
    db.execute("CREATE INDEX idx_items_current ON items(is_current)").unwrap();
    db.execute("CREATE INDEX idx_items_category ON items(category)").unwrap();
    
    db.execute("CREATE TABLE customers (id INT, name TEXT, email TEXT, loyalty_points INT)").unwrap();
    db.execute("CREATE INDEX idx_customers_email ON customers(email)").unwrap();
    
    db.execute("CREATE TABLE orders (id INT, customer_id INT, order_timestamp INT, total INT)").unwrap();
    db.execute("CREATE INDEX idx_orders_customer ON orders(customer_id)").unwrap();
    
    db.execute("CREATE TABLE order_items (id INT, order_id INT, item_id INT, quantity INT, price INT)").unwrap();
    db.execute("CREATE INDEX idx_order_items_order ON order_items(order_id)").unwrap();
    
    db.execute("CREATE TABLE inventory (item_id INT, stock INT, last_updated INT)").unwrap();

    // Batch insert items (SCD Type 2 - initial versions)
    eprintln!("[PetStore] Batch inserting items (pet food, toys, accessories)...");
    db.execute("INSERT INTO items VALUES (1, 'DF001', 'Premium Dog Food 10kg', 'Food', 4500, 'PetNutrition Co', 1000, 9999999999, 1, 'admin'), (2, 'CT001', 'Catnip Toy Mouse', 'Toy', 899, 'FunPets Inc', 1000, 9999999999, 1, 'admin'), (3, 'FB001', 'Fish Food Flakes 200g', 'Food', 1299, 'AquaLife Ltd', 1000, 9999999999, 1, 'admin'), (4, 'BC001', 'Bird Cage Large', 'Accessory', 8900, 'CageWorld', 1000, 9999999999, 1, 'admin'), (5, 'DT001', 'Dog Chew Toy Rope', 'Toy', 1599, 'FunPets Inc', 1000, 9999999999, 1, 'admin')").unwrap();

    // Batch insert customers
    eprintln!("[PetStore] Batch inserting customers...");
    db.execute("INSERT INTO customers VALUES (1, 'Alice', 'alice@example.com', 100), (2, 'Bob', 'bob@example.com', 50), (3, 'Charlie', 'charlie@example.com', 200)").unwrap();

    // Batch insert inventory
    eprintln!("[PetStore] Batch inserting inventory...");
    db.execute("INSERT INTO inventory VALUES (1, 50, 1000), (2, 120, 1000), (3, 200, 1000), (4, 15, 1000), (5, 80, 1000)").unwrap();

    // Transaction with savepoint
    eprintln!("[PetStore] Testing transaction with savepoint...");
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO orders VALUES (1, 1, 1000, 4500)").unwrap();
    db.execute("SAVEPOINT sp1").unwrap();
    db.execute("INSERT INTO order_items VALUES (1, 1, 1, 1, 4500)").unwrap();
    db.execute("ROLLBACK TO sp1").unwrap();
    db.execute("INSERT INTO order_items VALUES (1, 1, 1, 1, 4500), (2, 1, 3, 2, 2598)").unwrap();
    db.execute("COMMIT").unwrap();

    // More orders
    eprintln!("[PetStore] Adding more orders...");
    db.execute("INSERT INTO orders VALUES (2, 2, 1001, 899), (3, 3, 1002, 1599)").unwrap();
    db.execute("INSERT INTO order_items VALUES (3, 2, 2, 1, 899), (4, 3, 5, 1, 1599)").unwrap();

    // SCD Type 2: Update item price (create new version)
    eprintln!("[PetStore] SCD Type 2: Updating item price (creating new version)...");
    db.execute("UPDATE items SET effective_to = 2000, is_current = 0 WHERE item_id = 1 AND is_current = 1").unwrap();
    db.execute("INSERT INTO items VALUES (1, 'DF001', 'Premium Dog Food 10kg', 'Food', 4799, 'PetNutrition Co', 2001, 9999999999, 1, 'manager')").unwrap();
    
    // SCD Type 2: Change supplier
    eprintln!("[PetStore] SCD Type 2: Changing supplier (creating new version)...");
    db.execute("UPDATE items SET effective_to = 3000, is_current = 0 WHERE item_id = 2 AND is_current = 1").unwrap();
    db.execute("INSERT INTO items VALUES (2, 'CT001', 'Catnip Toy Mouse', 'Toy', 899, 'ToyMakers Ltd', 3001, 9999999999, 1, 'buyer')").unwrap();

    // Create views
    eprintln!("[PetStore] Creating views...");
    db.execute("CREATE VIEW current_items AS SELECT item_id, sku, name, category, price, supplier FROM items WHERE is_current = 1").unwrap();
    db.execute("CREATE VIEW customer_orders AS SELECT c.name, o.id AS order_id, o.total FROM customers c JOIN orders o ON c.id = o.customer_id").unwrap();

    // Create materialized view
    eprintln!("[PetStore] Creating materialized view...");
    db.execute("CREATE MATERIALIZED VIEW category_sales AS SELECT i.category, COUNT(*) AS sold_count FROM items i JOIN order_items oi ON i.item_id = oi.item_id WHERE i.is_current = 1 GROUP BY i.category").unwrap();

    // Test INNER JOIN
    eprintln!("[PetStore] Testing INNER JOIN...");
    let result = db.execute("SELECT c.name, o.total FROM customers c INNER JOIN orders o ON c.id = o.customer_id");
    assert!(result.is_ok());
    assert!(result.unwrap().contains("Alice"));

    // Test LEFT JOIN
    eprintln!("[PetStore] Testing LEFT JOIN...");
    let result = db.execute("SELECT c.name, o.id FROM customers c LEFT JOIN orders o ON c.id = o.customer_id");
    assert!(result.is_ok());

    // Test SCD queries
    eprintln!("[PetStore] Testing SCD current version query...");
    let result = db.execute("SELECT name, price FROM items WHERE is_current = 1");
    assert!(result.is_ok());

    eprintln!("[PetStore] Testing SCD historical query...");
    let result = db.execute("SELECT name, price, effective_from, effective_to, modified_by FROM items WHERE item_id = 1");
    assert!(result.is_ok());

    // Test subquery with aggregate
    eprintln!("[PetStore] Testing subquery with AVG...");
    let result = db.execute("SELECT name, price FROM items WHERE price > (SELECT AVG(price) FROM items WHERE is_current = 1) AND is_current = 1");
    assert!(result.is_ok());

    // Test IN subquery
    eprintln!("[PetStore] Testing IN subquery...");
    let result = db.execute("SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders)");
    assert!(result.is_ok());

    // Test string functions
    eprintln!("[PetStore] Testing string functions...");
    let result = db.execute("SELECT UPPER(name), LOWER(category), LENGTH(sku) FROM items WHERE is_current = 1");
    assert!(result.is_ok());
    let result = db.execute("SELECT CONCAT(sku, ' - ', name) FROM items WHERE is_current = 1");
    assert!(result.is_ok());
    let result = db.execute("SELECT SUBSTRING(email, 1, 5) FROM customers");
    assert!(result.is_ok());

    // Test views
    eprintln!("[PetStore] Querying views...");
    let result = db.execute("SELECT * FROM current_items");
    assert!(result.is_ok());
    let result = db.execute("SELECT * FROM customer_orders");
    assert!(result.is_ok());

    // Test materialized view
    eprintln!("[PetStore] Querying materialized view...");
    let result = db.execute("SELECT * FROM category_sales");
    assert!(result.is_ok());

    // Test complex join with multiple tables
    eprintln!("[PetStore] Testing 3-way JOIN...");
    let result = db.execute("SELECT c.name, i.name, oi.quantity FROM customers c JOIN orders o ON c.id = o.customer_id JOIN order_items oi ON o.id = oi.order_id JOIN items i ON oi.item_id = i.item_id WHERE i.is_current = 1");
    assert!(result.is_ok());

    eprintln!("[PetStore] Stopping server for persistence test...");
    env.restart();
    let db = env.vaultgres();

    eprintln!("[PetStore] Verifying data persistence...");
    let result = db.execute("SELECT * FROM items");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("Dog Food"));
    assert!(output.contains("Catnip"));

    eprintln!("[PetStore] Verifying SCD Type 2 persistence...");
    let result = db.execute("SELECT * FROM items WHERE item_id = 1");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("4500"));
    assert!(output.contains("4799"));

    eprintln!("[PetStore] Verifying index persistence...");
    let result = db.execute("SELECT * FROM items WHERE category = 'Food'");
    assert!(result.is_ok());

    eprintln!("[PetStore] Verifying view persistence...");
    let result = db.execute("SELECT * FROM current_items");
    assert!(result.is_ok());

    eprintln!("[PetStore] Verifying materialized view persistence...");
    let result = db.execute("SELECT * FROM category_sales");
    assert!(result.is_ok());

    eprintln!("[PetStore] Verifying orders persistence...");
    let result = db.execute("SELECT * FROM orders");
    assert!(result.is_ok());

    eprintln!("[PetStore] Verifying inventory persistence...");
    let result = db.execute("SELECT * FROM inventory");
    assert!(result.is_ok());

    // Cleanup
    db.execute("DROP MATERIALIZED VIEW category_sales").ok();
    db.execute("DROP VIEW customer_orders").ok();
    db.execute("DROP VIEW current_items").ok();
    db.execute("DROP TABLE order_items").ok();
    db.execute("DROP TABLE orders").ok();
    db.execute("DROP TABLE inventory").ok();
    db.execute("DROP TABLE customers").ok();
    db.execute("DROP TABLE items").ok();

    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_pet_store_concurrent_orders() {
    eprintln!("\n=== Test: Pet Store - Concurrent Orders ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE items_concurrent (id INT, name TEXT, stock INT)").unwrap();
    db.execute("CREATE TABLE orders_concurrent (id INT, item_id INT, quantity INT)").unwrap();
    
    eprintln!("[PetStore] Stocking inventory...");
    db.execute("INSERT INTO items_concurrent VALUES (1, 'Dog Food', 100)").unwrap();
    db.execute("INSERT INTO items_concurrent VALUES (2, 'Cat Toy', 50)").unwrap();

    eprintln!("[PetStore] Simulating 20 concurrent orders...");
    let start = Instant::now();
    for i in 0..20 {
        let item_id = (i % 2) + 1;
        db.execute(&format!("INSERT INTO orders_concurrent VALUES ({}, {}, 1)", i, item_id)).ok();
    }
    let duration = start.elapsed();

    eprintln!("[PetStore] Processed 20 orders in {:?}", duration);
    
    let result = db.execute("SELECT * FROM orders_concurrent");
    assert!(result.is_ok());
    
    assert!(duration.as_secs() < 10);

    db.execute("DROP TABLE orders_concurrent").ok();
    db.execute("DROP TABLE items_concurrent").ok();

    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_pet_store_inventory_management() {
    eprintln!("\n=== Test: Pet Store - Inventory Management ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE inventory (id INT, item TEXT, quantity INT, price INT)").unwrap();

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

    eprintln!("[PetStore] Checking inventory levels...");
    let result = db.execute("SELECT * FROM inventory");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("Dog Food"));
    assert!(output.contains("Cat Litter"));

    eprintln!("[PetStore] Querying low stock items (quantity < 50)...");
    let result = db.execute("SELECT * FROM inventory WHERE quantity < 50");
    assert!(result.is_ok());

    db.execute("DROP TABLE inventory").ok();

    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_pet_store_customer_orders_workflow() {
    eprintln!("\n=== Test: Pet Store - Complete Order Workflow ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    eprintln!("[PetStore] Setting up database schema...");
    db.execute("CREATE TABLE customers_workflow (id INT, name TEXT, loyalty_points INT)").unwrap();
    db.execute("CREATE TABLE products_workflow (id INT, name TEXT, category TEXT, price INT)").unwrap();
    db.execute("CREATE TABLE orders_workflow (id INT, customer_id INT, product_id INT, total INT)").unwrap();

    eprintln!("[PetStore] Registering customers...");
    db.execute("INSERT INTO customers_workflow VALUES (1, 'Alice', 100)").unwrap();
    db.execute("INSERT INTO customers_workflow VALUES (2, 'Bob', 50)").unwrap();
    db.execute("INSERT INTO customers_workflow VALUES (3, 'Charlie', 200)").unwrap();

    eprintln!("[PetStore] Adding products...");
    db.execute("INSERT INTO products_workflow VALUES (1, 'Premium Dog Food', 'Food', 60)").unwrap();
    db.execute("INSERT INTO products_workflow VALUES (2, 'Cat Scratching Post', 'Furniture', 80)").unwrap();
    db.execute("INSERT INTO products_workflow VALUES (3, 'Aquarium Filter', 'Equipment', 45)").unwrap();

    eprintln!("[PetStore] Processing customer orders...");
    db.execute("INSERT INTO orders_workflow VALUES (1, 1, 1, 60)").unwrap();
    db.execute("INSERT INTO orders_workflow VALUES (2, 2, 2, 80)").unwrap();
    db.execute("INSERT INTO orders_workflow VALUES (3, 1, 3, 45)").unwrap();
    db.execute("INSERT INTO orders_workflow VALUES (4, 3, 1, 60)").unwrap();

    eprintln!("[PetStore] Verifying orders...");
    let result = db.execute("SELECT * FROM orders_workflow");
    assert!(result.is_ok());

    eprintln!("[PetStore] Verifying customers...");
    let result = db.execute("SELECT * FROM customers_workflow");
    assert!(result.is_ok());
    let customers_output = result.unwrap();
    assert!(customers_output.contains("Alice"));
    assert!(customers_output.contains("Bob"));
    assert!(customers_output.contains("Charlie"));

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

    db.execute("CREATE TABLE sales (id INT, product TEXT, amount INT, ts INT)").unwrap();

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

    db.execute("DROP TABLE sales").ok();

    eprintln!("=== Test PASSED ===");
}
