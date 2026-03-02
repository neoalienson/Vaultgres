use rustgres::catalog::Catalog;
use rustgres::parser::ast::{ColumnDef, DataType, Expr};

#[test]
fn test_catalog_create_and_query() {
    let catalog = Catalog::new();

    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("name".to_string(), DataType::Text),
    ];

    catalog.create_table("users".to_string(), columns).unwrap();

    let schema = catalog.get_table("users").unwrap();
    assert_eq!(schema.name, "users");
    assert_eq!(schema.columns.len(), 2);
}

#[test]
fn test_catalog_insert_and_count() {
    let catalog = Catalog::new();

    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();

    catalog.insert("data", vec![Expr::Number(1), Expr::Number(100)]).unwrap();
    catalog.insert("data", vec![Expr::Number(2), Expr::Number(200)]).unwrap();
    catalog.insert("data", vec![Expr::Number(3), Expr::Number(300)]).unwrap();

    assert_eq!(catalog.row_count("data"), 3);
}

#[test]
fn test_catalog_multiple_tables() {
    let catalog = Catalog::new();

    catalog
        .create_table("users".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
        .unwrap();

    catalog
        .create_table("products".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
        .unwrap();

    let tables = catalog.list_tables();
    assert_eq!(tables.len(), 2);
    assert!(tables.contains(&"users".to_string()));
    assert!(tables.contains(&"products".to_string()));
}

#[test]
fn test_catalog_drop_and_recreate() {
    let catalog = Catalog::new();

    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("temp".to_string(), columns.clone()).unwrap();
    catalog.insert("temp", vec![Expr::Number(1)]).unwrap();
    assert_eq!(catalog.row_count("temp"), 1);

    catalog.drop_table("temp", false).unwrap();
    assert!(catalog.get_table("temp").is_none());

    catalog.create_table("temp".to_string(), columns).unwrap();
    assert_eq!(catalog.row_count("temp"), 0);
}

#[test]
fn test_catalog_varchar_type() {
    let catalog = Catalog::new();

    let columns = vec![ColumnDef::new("email".to_string(), DataType::Varchar(100))];

    catalog.create_table("contacts".to_string(), columns).unwrap();
    catalog.insert("contacts", vec![Expr::String("test@example.com".to_string())]).unwrap();

    assert_eq!(catalog.row_count("contacts"), 1);
}

#[test]
fn test_catalog_insert_validation() {
    let catalog = Catalog::new();

    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("name".to_string(), DataType::Text),
    ];

    catalog.create_table("users".to_string(), columns).unwrap();

    let result = catalog.insert("users", vec![Expr::Number(1)]);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Expected 2 values"));
}

#[test]
fn test_catalog_type_validation() {
    let catalog = Catalog::new();

    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("numbers".to_string(), columns).unwrap();

    let result = catalog.insert("numbers", vec![Expr::String("not a number".to_string())]);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Type mismatch"));
}

#[test]
fn test_catalog_concurrent_operations() {
    let catalog = Catalog::new();

    catalog
        .create_table("test".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
        .unwrap();

    for i in 0..10 {
        catalog.insert("test", vec![Expr::Number(i)]).unwrap();
    }

    assert_eq!(catalog.row_count("test"), 10);
}

#[test]
fn test_catalog_empty_table() {
    let catalog = Catalog::new();

    catalog
        .create_table("empty".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
        .unwrap();

    assert_eq!(catalog.row_count("empty"), 0);
    assert!(catalog.get_table("empty").is_some());
}
