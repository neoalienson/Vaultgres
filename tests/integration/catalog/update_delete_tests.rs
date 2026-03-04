use std::sync::Arc;
use vaultgres::catalog::*;
use vaultgres::parser::ast::{BinaryOperator, ColumnDef, DataType, Expr};

#[test]
fn test_update() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1), Expr::Number(100)]).unwrap();
    catalog.insert("data", vec![Expr::Number(2), Expr::Number(200)]).unwrap();

    let updated =
        catalog.update("data", vec![("value".to_string(), Expr::Number(999))], None).unwrap();
    assert_eq!(updated, 2);
}

#[test]
fn test_update_nonexistent_table() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let result = catalog.update("nonexistent", vec![("col".to_string(), Expr::Number(1))], None);
    assert!(result.is_err());
}

#[test]
fn test_update_with_where() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1), Expr::Number(100)]).unwrap();
    catalog.insert("data", vec![Expr::Number(2), Expr::Number(200)]).unwrap();

    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: BinaryOperator::Equals,
        right: Box::new(Expr::Number(1)),
    });

    let updated = catalog
        .update("data", vec![("value".to_string(), Expr::Number(999))], where_clause)
        .unwrap();
    assert_eq!(updated, 1);
}

#[test]
fn test_delete() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();
    catalog.insert("data", vec![Expr::Number(2)]).unwrap();
    catalog.insert("data", vec![Expr::Number(3)]).unwrap();

    let deleted = catalog.delete("data", None).unwrap();
    assert_eq!(deleted, 3);
}

#[test]
fn test_delete_empty_table() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("empty".to_string(), columns).unwrap();
    let deleted = catalog.delete("empty", None).unwrap();
    assert_eq!(deleted, 0);
}

#[test]
fn test_delete_with_where() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();
    catalog.insert("data", vec![Expr::Number(2)]).unwrap();
    catalog.insert("data", vec![Expr::Number(3)]).unwrap();

    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: BinaryOperator::Equals,
        right: Box::new(Expr::Number(2)),
    });

    let deleted = catalog.delete("data", where_clause).unwrap();
    assert_eq!(deleted, 1);
}
