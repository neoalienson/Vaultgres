use std::sync::Arc;
use vaultgres::catalog::*;
use vaultgres::parser::ast::{ColumnDef, DataType};

#[test]
fn test_create_table() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("name".to_string(), DataType::Text),
    ];

    assert!(catalog.create_table("users".to_string(), columns).is_ok());
    assert!(catalog.get_table("users").is_some());
}

#[test]
fn test_create_duplicate_table() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("users".to_string(), columns.clone()).unwrap();
    assert!(catalog.create_table("users".to_string(), columns).is_err());
}

#[test]
fn test_drop_table() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("users".to_string(), columns).unwrap();
    assert!(catalog.drop_table("users", false).is_ok());
    assert!(catalog.get_table("users").is_none());
}

#[test]
fn test_drop_nonexistent_table() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    assert!(catalog.drop_table("users", false).is_err());
    assert!(catalog.drop_table("users", true).is_ok());
}
