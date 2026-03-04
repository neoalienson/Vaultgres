use std::sync::Arc;
use vaultgres::catalog::*;
use vaultgres::parser::ast::{ColumnDef, DataType, Expr};

#[test]
fn test_insert() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("name".to_string(), DataType::Text),
    ];

    catalog.create_table("users".to_string(), columns).unwrap();

    let values = vec![Expr::Number(1), Expr::String("Alice".to_string())];
    assert!(catalog.insert("users", values).is_ok());
    assert_eq!(catalog.row_count("users"), 1);
}

#[test]
fn test_insert_wrong_column_count() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("users".to_string(), columns).unwrap();

    let values = vec![Expr::Number(1), Expr::String("Alice".to_string())];
    assert!(catalog.insert("users", values).is_err());
}

#[test]
fn test_insert_type_mismatch() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("users".to_string(), columns).unwrap();

    let values = vec![Expr::String("not a number".to_string())];
    assert!(catalog.insert("users", values).is_err());
}

#[test]
fn test_insert_multiple_rows() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("name".to_string(), DataType::Text),
    ];

    catalog.create_table("users".to_string(), columns).unwrap();

    catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
    catalog.insert("users", vec![Expr::Number(2), Expr::String("Bob".to_string())]).unwrap();
    catalog.insert("users", vec![Expr::Number(3), Expr::String("Charlie".to_string())]).unwrap();

    assert_eq!(catalog.row_count("users"), 3);
}
