// Pet Store SELECT Edge Case Tests
use e2e::*;

pub fn run_select_tests(db: &DbConnection) {
    eprintln!("\n[PetStore] === Testing SELECT Edge Cases ===");
    
    test_select_with_nulls(db);
    test_select_distinct(db);
    test_select_order_by(db);
    test_select_limit_offset(db);
    test_select_like(db);
    test_select_conditions(db);
    test_select_between(db);
    test_select_aggregates(db);
    test_select_group_by(db);
    test_select_having(db);
    test_select_case(db);
    test_select_coalesce(db);
    test_select_nested_subquery(db);
    test_select_exists(db);
    test_select_multiple_joins(db);
    test_select_expressions(db);
    test_select_aliases(db);
}

fn test_select_with_nulls(db: &DbConnection) {
    eprintln!("[PetStore] Testing SELECT with NULL values...");
    db.execute("CREATE TABLE nullable_select (id INT, value INT, text TEXT)").unwrap();
    db.execute("INSERT INTO nullable_select VALUES (1, 10, 'hello'), (2, NULL, 'world'), (3, 30, NULL)").unwrap();
    
    let result = db.execute("SELECT * FROM nullable_select WHERE value IS NULL");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("world"), "Should find row with NULL value");
    
    let result = db.execute("SELECT * FROM nullable_select WHERE text IS NULL");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("30"), "Should find row with NULL text");
}

fn test_select_distinct(db: &DbConnection) {
    eprintln!("[PetStore] Testing SELECT with DISTINCT...");
    let result = db.execute("SELECT DISTINCT category FROM items");
    assert!(result.is_ok());
}

fn test_select_order_by(db: &DbConnection) {
    eprintln!("[PetStore] Testing SELECT with ORDER BY...");
    let result = db.execute("SELECT name, price FROM items WHERE is_current = 1 ORDER BY price DESC");
    assert!(result.is_ok());
    let output = result.unwrap();
    let dog_food_pos = output.find("Premium Dog Food").unwrap_or(0);
    let catnip_pos = output.find("Catnip Toy Mouse").unwrap_or(0);
    assert!(dog_food_pos < catnip_pos, "Higher price should come first with DESC");
    
    eprintln!("[PetStore] Testing SELECT with ORDER BY ASC...");
    let result = db.execute("SELECT name, price FROM items WHERE is_current = 1 ORDER BY price ASC");
    assert!(result.is_ok());
    let output = result.unwrap();
    let catnip_pos = output.find("Catnip Toy Mouse").unwrap_or(0);
    let dog_food_pos = output.find("Premium Dog Food").unwrap_or(0);
    assert!(catnip_pos < dog_food_pos, "Lower price should come first with ASC");
}

fn test_select_limit_offset(db: &DbConnection) {
    eprintln!("[PetStore] Testing SELECT with LIMIT...");
    let result = db.execute("SELECT name FROM items LIMIT 2");
    assert!(result.is_ok());
    let output = result.unwrap();
    let line_count = output.lines().filter(|l| l.contains("Premium") || l.contains("Catnip") || l.contains("Fish") || l.contains("Bird") || l.contains("Dog Chew")).count();
    assert!(line_count <= 2, "Should return at most 2 rows");
    
    eprintln!("[PetStore] Testing SELECT with OFFSET...");
    let result = db.execute("SELECT name FROM items ORDER BY price ASC LIMIT 1 OFFSET 1");
    assert!(result.is_ok());
}

fn test_select_like(db: &DbConnection) {
    eprintln!("[PetStore] Testing SELECT with LIKE...");
    let result = db.execute("SELECT name FROM items WHERE name LIKE '%Dog%'");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("Dog"), "Should find items with 'Dog' in name");
}

fn test_select_conditions(db: &DbConnection) {
    eprintln!("[PetStore] Testing SELECT with AND/OR conditions...");
    let result = db.execute("SELECT name FROM items WHERE (category = 'Food' OR category = 'Toy') AND is_current = 1");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("Food") || output.contains("Toy"), "Should find Food or Toy items");
}

fn test_select_between(db: &DbConnection) {
    eprintln!("[PetStore] Testing SELECT with BETWEEN...");
    let result = db.execute("SELECT name, price FROM items WHERE price BETWEEN 1000 AND 5000 AND is_current = 1");
    assert!(result.is_ok());
}

fn test_select_aggregates(db: &DbConnection) {
    eprintln!("[PetStore] Testing SELECT with aggregates...");
    let result = db.execute("SELECT COUNT(*), SUM(price), AVG(price), MIN(price), MAX(price) FROM items WHERE is_current = 1");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("5"), "Should have 5 current items");
}

fn test_select_group_by(db: &DbConnection) {
    eprintln!("[PetStore] Testing SELECT with GROUP BY...");
    let result = db.execute("SELECT category, COUNT(*) FROM items GROUP BY category");
    assert!(result.is_ok());
}

fn test_select_having(db: &DbConnection) {
    eprintln!("[PetStore] Testing SELECT with HAVING...");
    let result = db.execute("SELECT category, COUNT(*) as cnt FROM items GROUP BY category HAVING COUNT(*) >= 1");
    assert!(result.is_ok());
}

fn test_select_case(db: &DbConnection) {
    eprintln!("[PetStore] Testing SELECT with CASE...");
    let result = db.execute("SELECT name, CASE WHEN price > 1000 THEN 'expensive' ELSE 'cheap' END FROM items WHERE is_current = 1");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("expensive") || output.contains("cheap"), "Should have price categories");
}

fn test_select_coalesce(db: &DbConnection) {
    eprintln!("[PetStore] Testing SELECT with COALESCE...");
    let result = db.execute("SELECT COALESCE(NULL, NULL, 'default_value')");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("default_value"), "COALESCE should return first non-NULL");
}

fn test_select_nested_subquery(db: &DbConnection) {
    eprintln!("[PetStore] Testing SELECT with nested subquery...");
    let result = db.execute("SELECT name FROM items WHERE price > (SELECT AVG(price) FROM (SELECT price FROM items WHERE is_current = 1) AS sub)");
    assert!(result.is_ok());
}

fn test_select_exists(db: &DbConnection) {
    eprintln!("[PetStore] Testing SELECT with EXISTS...");
    let result = db.execute("SELECT name FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)");
    assert!(result.is_ok());
}

fn test_select_multiple_joins(db: &DbConnection) {
    eprintln!("[PetStore] Testing SELECT with multiple JOINs...");
    let result = db.execute("SELECT c.name, o.total, i.name as item_name FROM customers c JOIN orders o ON c.id = o.customer_id JOIN order_items oi ON o.id = oi.order_id JOIN items i ON oi.item_id = i.item_id");
    assert!(result.is_ok());
}

fn test_select_expressions(db: &DbConnection) {
    eprintln!("[PetStore] Testing SELECT with complex expressions...");
    let result = db.execute("SELECT name, price, price * 2 as double_price, price + 100 as increased_price FROM items WHERE is_current = 1");
    assert!(result.is_ok());
}

fn test_select_aliases(db: &DbConnection) {
    eprintln!("[PetStore] Testing SELECT with column aliases...");
    let result = db.execute("SELECT name as item_name, price as item_price FROM items WHERE is_current = 1");
    assert!(result.is_ok());
    
    eprintln!("[PetStore] Testing SELECT with table aliases...");
    let result = db.execute("SELECT c.name as customer_name, o.total as order_total FROM customers c, orders o WHERE c.id = o.customer_id");
    assert!(result.is_ok());
}
