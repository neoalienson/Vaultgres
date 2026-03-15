// Pet Store Test Setup - Schema creation and initial data
use e2e::*;

pub fn setup_schema(db: &DbConnection) {
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
}

pub fn insert_initial_data(db: &DbConnection) {
    // Batch insert items (SCD Type 2 - initial versions)
    eprintln!("[PetStore] Batch inserting items (pet food, toys, accessories)...");
    db.execute("INSERT INTO items VALUES (1, 'DF001', 'Premium Dog Food 10kg', 'Food', 4500, 'PetNutrition Co', 1000, 9999999999, 1, 'admin'), (2, 'CT001', 'Catnip Toy Mouse', 'Toy', 899, 'FunPets Inc', 1000, 9999999999, 1, 'admin'), (3, 'FB001', 'Fish Food Flakes 200g', 'Food', 1299, 'AquaLife Ltd', 1000, 9999999999, 1, 'admin'), (4, 'BC001', 'Bird Cage Large', 'Accessory', 8900, 'CageWorld', 1000, 9999999999, 1, 'admin'), (5, 'DT001', 'Dog Chew Toy Rope', 'Toy', 1599, 'FunPets Inc', 1000, 9999999999, 1, 'admin')").unwrap();

    // Batch insert customers
    eprintln!("[PetStore] Batch inserting customers...");
    db.execute("INSERT INTO customers VALUES (1, 'Alice', 'alice@example.com', 100), (2, 'Bob', 'bob@example.com', 50), (3, 'Charlie', 'charlie@example.com', 200)").unwrap();

    // Batch insert inventory
    eprintln!("[PetStore] Batch inserting inventory...");
    db.execute("INSERT INTO inventory VALUES (1, 50, 1000), (2, 120, 1000), (3, 200, 1000), (4, 15, 1000), (5, 80, 1000)").unwrap();
}

pub fn setup_transactions(db: &DbConnection) {
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
}

pub fn setup_scd_data(db: &DbConnection) {
    // SCD Type 2: Update item price (create new version)
    eprintln!("[PetStore] SCD Type 2: Updating item price (creating new version)...");
    db.execute("UPDATE items SET effective_to = 2000, is_current = 0 WHERE item_id = 1 AND is_current = 1").unwrap();
    db.execute("INSERT INTO items VALUES (1, 'DF001', 'Premium Dog Food 10kg', 'Food', 4799, 'PetNutrition Co', 2001, 9999999999, 1, 'manager')").unwrap();

    // SCD Type 2: Change supplier
    eprintln!("[PetStore] SCD Type 2: Changing supplier (creating new version)...");
    db.execute("UPDATE items SET effective_to = 3000, is_current = 0 WHERE item_id = 2 AND is_current = 1").unwrap();
    db.execute("INSERT INTO items VALUES (2, 'CT001', 'Catnip Toy Mouse', 'Toy', 899, 'ToyMakers Ltd', 3001, 9999999999, 1, 'buyer')").unwrap();
}

pub fn setup_views(db: &DbConnection) {
    // Create views
    eprintln!("[PetStore] Creating views...");
    db.execute("CREATE VIEW current_items AS SELECT item_id, sku, name, category, price, supplier FROM items WHERE is_current = 1").unwrap();
    db.execute("CREATE VIEW customer_orders AS SELECT c.name, o.id AS order_id, o.total FROM customers c JOIN orders o ON c.id = o.customer_id").unwrap();

    // Create materialized view
    eprintln!("[PetStore] Creating materialized view...");
    db.execute("CREATE MATERIALIZED VIEW category_sales AS SELECT i.category, COUNT(*) AS sold_count FROM items i JOIN order_items oi ON i.item_id = oi.item_id WHERE i.is_current = 1 GROUP BY i.category").unwrap();
}
