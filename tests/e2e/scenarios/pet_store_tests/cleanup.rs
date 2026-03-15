// Pet Store Test Cleanup
use e2e::*;

pub fn cleanup(db: &DbConnection) {
    eprintln!("[PetStore] Cleaning up test data...");
    db.execute("DROP MATERIALIZED VIEW IF EXISTS category_sales").ok();
    db.execute("DROP VIEW IF EXISTS customer_orders").ok();
    db.execute("DROP VIEW IF EXISTS current_items").ok();
    db.execute("DROP TABLE IF EXISTS order_items").ok();
    db.execute("DROP TABLE IF EXISTS orders").ok();
    db.execute("DROP TABLE IF EXISTS inventory").ok();
    db.execute("DROP TABLE IF EXISTS customers").ok();
    db.execute("DROP TABLE IF EXISTS items").ok();
    db.execute("DROP TABLE IF EXISTS accounts").ok();
    db.execute("DROP TABLE IF EXISTS accounts_copy").ok();
    db.execute("DROP TABLE IF EXISTS nullable_select").ok();
    db.execute("DROP TABLE IF EXISTS insert_null_test").ok();
    db.execute("DROP TABLE IF EXISTS nullable_test").ok();
}
