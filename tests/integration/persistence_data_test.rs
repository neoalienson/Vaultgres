// Integration tests for data persistence across restarts
// These tests verify that data is properly saved to disk and loaded on restart

use std::sync::Arc;
use tempfile::TempDir;
use vaultgres::catalog::{Catalog, DataType};
use vaultgres::parser::ast::{AggregateFunc, BinaryOperator, ColumnDef, Expr, SelectStmt};

fn create_test_catalog_with_data() -> (Arc<Catalog>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap().to_string();

    let catalog = Catalog::new_with_data_dir(&data_dir);

    // Create a table
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("name".to_string(), DataType::Text),
    ];
    catalog.create_table("test_table".to_string(), columns).unwrap();

    // Insert data
    catalog
        .insert("test_table", &[], vec![Expr::Number(1), Expr::String("Alice".to_string())])
        .unwrap();

    catalog
        .insert("test_table", &[], vec![Expr::Number(2), Expr::String("Bob".to_string())])
        .unwrap();

    // Force a save by waiting for background thread
    std::thread::sleep(std::time::Duration::from_millis(300));

    (catalog, temp_dir)
}

#[test]
fn test_data_persistence_basic() {
    let (catalog, _temp_dir) = create_test_catalog_with_data();

    // Verify data is in memory
    assert_eq!(catalog.row_count("test_table"), 2);

    // Create a new catalog instance pointing to the same data directory
    let data_dir = _temp_dir.path().to_str().unwrap();
    let new_catalog = Catalog::new_with_data_dir(data_dir);

    // Wait for load to complete
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Verify data was loaded
    assert_eq!(new_catalog.row_count("test_table"), 2, "Data should be persisted and loaded");
}

#[test]
fn test_table_schema_persistence() {
    let (catalog, _temp_dir) = create_test_catalog_with_data();

    // Verify schema is in memory
    let original_schema = catalog.get_table("test_table").unwrap();
    assert_eq!(original_schema.columns.len(), 2);
    assert_eq!(original_schema.columns[0].name, "id");
    assert_eq!(original_schema.columns[1].name, "name");

    // Create a new catalog instance
    let data_dir = _temp_dir.path().to_str().unwrap();
    let new_catalog = Catalog::new_with_data_dir(data_dir);

    // Wait for load to complete
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Verify schema was loaded
    let loaded_schema =
        new_catalog.get_table("test_table").expect("Table schema should be persisted");
    assert_eq!(loaded_schema.columns.len(), 2);
    assert_eq!(loaded_schema.columns[0].name, "id");
    assert_eq!(loaded_schema.columns[1].name, "name");
}

#[test]
fn test_multiple_tables_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap().to_string();

    let catalog = Catalog::new_with_data_dir(&data_dir);

    // Create multiple tables
    catalog
        .create_table(
            "users".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("email".to_string(), DataType::Text),
            ],
        )
        .unwrap();

    catalog
        .create_table(
            "products".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("name".to_string(), DataType::Text),
                ColumnDef::new("price".to_string(), DataType::Int),
            ],
        )
        .unwrap();

    // Insert data into both tables
    catalog
        .insert("users", &[], vec![Expr::Number(1), Expr::String("user@example.com".to_string())])
        .unwrap();

    catalog
        .insert(
            "products",
            &[],
            vec![Expr::Number(1), Expr::String("Widget".to_string()), Expr::Number(999)],
        )
        .unwrap();

    // Force save
    std::thread::sleep(std::time::Duration::from_millis(300));

    // Create new catalog
    let new_catalog = Catalog::new_with_data_dir(&data_dir);
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Verify both tables exist
    assert!(new_catalog.get_table("users").is_some(), "users table should be persisted");
    assert!(new_catalog.get_table("products").is_some(), "products table should be persisted");

    // Verify row counts
    assert_eq!(new_catalog.row_count("users"), 1);
    assert_eq!(new_catalog.row_count("products"), 1);
}

#[test]
fn test_large_dataset_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap().to_string();

    let catalog = Catalog::new_with_data_dir(&data_dir);

    // Create table
    catalog
        .create_table(
            "large_table".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("value".to_string(), DataType::Text),
            ],
        )
        .unwrap();

    // Insert 100 rows
    for i in 0..100 {
        catalog
            .insert("large_table", &[], vec![Expr::Number(i), Expr::String(format!("value_{}", i))])
            .unwrap();
    }

    // Force save
    std::thread::sleep(std::time::Duration::from_millis(300));

    // Verify in-memory count
    assert_eq!(catalog.row_count("large_table"), 100);

    // Create new catalog
    let new_catalog = Catalog::new_with_data_dir(&data_dir);
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Verify persisted count
    assert_eq!(new_catalog.row_count("large_table"), 100, "All 100 rows should be persisted");
}

#[test]
fn test_persistence_empty_table() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap().to_string();

    let catalog = Catalog::new_with_data_dir(&data_dir);

    // Create table but don't insert any data
    catalog
        .create_table(
            "empty_table".to_string(),
            vec![ColumnDef::new("id".to_string(), DataType::Int)],
        )
        .unwrap();

    // Force save
    std::thread::sleep(std::time::Duration::from_millis(300));

    // Create new catalog
    let new_catalog = Catalog::new_with_data_dir(&data_dir);
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Table should exist but be empty
    assert!(new_catalog.get_table("empty_table").is_some());
    assert_eq!(new_catalog.row_count("empty_table"), 0);
}

#[test]
fn test_persistence_with_joins() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap().to_string();

    let catalog = Catalog::new_with_data_dir(&data_dir);

    // Create customers table
    catalog
        .create_table(
            "customers".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("name".to_string(), DataType::Text),
            ],
        )
        .unwrap();

    // Create orders table
    catalog
        .create_table(
            "orders".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("customer_id".to_string(), DataType::Int),
                ColumnDef::new("total".to_string(), DataType::Int),
            ],
        )
        .unwrap();

    // Insert customers
    catalog
        .insert("customers", &[], vec![Expr::Number(1), Expr::String("Alice".to_string())])
        .unwrap();

    catalog
        .insert("customers", &[], vec![Expr::Number(2), Expr::String("Bob".to_string())])
        .unwrap();

    // Insert orders
    catalog
        .insert("orders", &[], vec![Expr::Number(1), Expr::Number(1), Expr::Number(100)])
        .unwrap();

    catalog
        .insert("orders", &[], vec![Expr::Number(2), Expr::Number(2), Expr::Number(200)])
        .unwrap();

    // Force save
    std::thread::sleep(std::time::Duration::from_millis(300));

    // Verify in-memory counts
    assert_eq!(catalog.row_count("customers"), 2);
    assert_eq!(catalog.row_count("orders"), 2);

    // Create new catalog
    let new_catalog = Catalog::new_with_data_dir(&data_dir);
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Verify persisted counts
    assert_eq!(new_catalog.row_count("customers"), 2, "Customers should persist");
    assert_eq!(new_catalog.row_count("orders"), 2, "Orders should persist");

    // Verify schemas persisted correctly
    assert!(new_catalog.get_table("customers").is_some());
    assert!(new_catalog.get_table("orders").is_some());
}

#[test]
fn test_persistence_with_views() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap().to_string();

    let catalog = Arc::new(Catalog::new_with_data_dir(&data_dir));

    // Create table
    catalog
        .create_table(
            "items".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("name".to_string(), DataType::Text),
                ColumnDef::new("is_current".to_string(), DataType::Int),
            ],
        )
        .unwrap();

    // Insert data
    catalog
        .insert(
            "items",
            &[],
            vec![Expr::Number(1), Expr::String("Item 1".to_string()), Expr::Number(1)],
        )
        .unwrap();

    catalog
        .insert(
            "items",
            &[],
            vec![Expr::Number(2), Expr::String("Item 2".to_string()), Expr::Number(0)],
        )
        .unwrap();

    // Create view using catalog API directly
    let select_stmt = SelectStmt {
        distinct: false,
        columns: vec![Expr::Column("id".to_string()), Expr::Column("name".to_string())],
        from: "items".to_string(),
        table_alias: None,
        joins: vec![],
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("is_current".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(1)),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };
    catalog.create_view("items_view".to_string(), select_stmt).unwrap();

    // Force save
    std::thread::sleep(std::time::Duration::from_millis(300));

    // Verify view exists in memory
    assert!(catalog.get_view("items_view").is_some());

    // Create new catalog
    let new_catalog = Catalog::new_with_data_dir(&data_dir);
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Verify view persisted
    assert!(new_catalog.get_view("items_view").is_some(), "View should persist");
}

#[test]
fn test_persistence_with_materialized_views() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap().to_string();

    let catalog = Arc::new(Catalog::new_with_data_dir(&data_dir));

    // Create table
    catalog
        .create_table(
            "sales".to_string(),
            vec![
                ColumnDef::new("category".to_string(), DataType::Text),
                ColumnDef::new("amount".to_string(), DataType::Int),
            ],
        )
        .unwrap();

    // Insert data
    catalog.insert("sales", &[], vec![Expr::String("cat1".to_string()), Expr::Number(10)]).unwrap();

    catalog.insert("sales", &[], vec![Expr::String("cat2".to_string()), Expr::Number(20)]).unwrap();

    catalog.insert("sales", &[], vec![Expr::String("cat1".to_string()), Expr::Number(15)]).unwrap();

    // Create materialized view using catalog API directly
    let select_stmt = SelectStmt {
        distinct: false,
        columns: vec![
            Expr::Column("category".to_string()),
            Expr::Aggregate {
                func: AggregateFunc::Sum,
                arg: Box::new(Expr::Column("amount".to_string())),
            },
        ],
        from: "sales".to_string(),
        table_alias: None,
        joins: vec![],
        where_clause: None,
        group_by: Some(vec![Expr::Column("category".to_string())]),
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };
    catalog.create_materialized_view("sales_view".to_string(), select_stmt).unwrap();

    // Force save
    std::thread::sleep(std::time::Duration::from_millis(300));

    // Verify materialized view exists in memory
    assert!(catalog.get_materialized_view("sales_view").is_some());

    // Create new catalog
    let new_catalog = Catalog::new_with_data_dir(&data_dir);
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Verify materialized view persisted
    assert!(
        new_catalog.get_materialized_view("sales_view").is_some(),
        "Materialized view should persist"
    );

    let mv_data = new_catalog.get_materialized_view("sales_view").unwrap();
    assert_eq!(mv_data.len(), 2, "Materialized view should have 2 rows");
}
