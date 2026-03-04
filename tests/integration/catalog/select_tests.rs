use std::sync::Arc;
use vaultgres::catalog::*;
use vaultgres::parser::ast::{ColumnDef, DataType, Expr, OrderByExpr};

#[test]
fn test_select_all() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("name".to_string(), DataType::Text),
    ];

    catalog.create_table("users".to_string(), columns).unwrap();
    catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
    catalog.insert("users", vec![Expr::Number(2), Expr::String("Bob".to_string())]).unwrap();

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "users",
        false,
        vec![Expr::Star],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].len(), 2);
}

#[test]
fn test_select_specific_columns() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("name".to_string(), DataType::Text),
    ];

    catalog.create_table("users".to_string(), columns).unwrap();
    catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "users",
        false,
        vec![Expr::Column("id".to_string())],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 1);
}

#[test]
fn test_select_nonexistent_table() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let result = Catalog::select_with_catalog(
        &catalog_arc,
        "nonexistent",
        false,
        vec![Expr::Star],
        None,
        None,
        None,
        None,
        None,
        None,
    );
    assert!(result.is_err());
}

#[test]
fn test_select_empty_table() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("empty".to_string(), columns).unwrap();
    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "empty",
        false,
        vec![Expr::Star],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(rows.len(), 0);
}

#[test]
fn test_select_with_order_by_asc() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(3), Expr::Number(300)]).unwrap();
    catalog.insert("data", vec![Expr::Number(1), Expr::Number(100)]).unwrap();
    catalog.insert("data", vec![Expr::Number(2), Expr::Number(200)]).unwrap();

    let order_by = Some(vec![OrderByExpr { column: "id".to_string(), ascending: true }]);
    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![Expr::Star],
        None,
        None,
        None,
        order_by,
        None,
        None,
    )
    .unwrap();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], Value::Int(1));
    assert_eq!(rows[1][0], Value::Int(2));
    assert_eq!(rows[2][0], Value::Int(3));
}

#[test]
fn test_select_with_order_by_desc() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();
    catalog.insert("data", vec![Expr::Number(3)]).unwrap();
    catalog.insert("data", vec![Expr::Number(2)]).unwrap();

    let order_by = Some(vec![OrderByExpr { column: "id".to_string(), ascending: false }]);
    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![Expr::Star],
        None,
        None,
        None,
        order_by,
        None,
        None,
    )
    .unwrap();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], Value::Int(3));
    assert_eq!(rows[1][0], Value::Int(2));
    assert_eq!(rows[2][0], Value::Int(1));
}

#[test]
fn test_select_with_limit() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();
    catalog.insert("data", vec![Expr::Number(2)]).unwrap();
    catalog.insert("data", vec![Expr::Number(3)]).unwrap();
    catalog.insert("data", vec![Expr::Number(4)]).unwrap();

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![Expr::Star],
        None,
        None,
        None,
        None,
        Some(2),
        None,
    )
    .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_select_with_offset() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();
    catalog.insert("data", vec![Expr::Number(2)]).unwrap();
    catalog.insert("data", vec![Expr::Number(3)]).unwrap();
    catalog.insert("data", vec![Expr::Number(4)]).unwrap();

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![Expr::Star],
        None,
        None,
        None,
        None,
        None,
        Some(2),
    )
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Int(3));
}

#[test]
fn test_select_with_limit_and_offset() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();
    catalog.insert("data", vec![Expr::Number(2)]).unwrap();
    catalog.insert("data", vec![Expr::Number(3)]).unwrap();
    catalog.insert("data", vec![Expr::Number(4)]).unwrap();
    catalog.insert("data", vec![Expr::Number(5)]).unwrap();

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![Expr::Star],
        None,
        None,
        None,
        None,
        Some(2),
        Some(1),
    )
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Int(2));
    assert_eq!(rows[1][0], Value::Int(3));
}

#[test]
fn test_distinct() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("category".to_string(), DataType::Text)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::String("A".to_string())]).unwrap();
    catalog.insert("data", vec![Expr::String("B".to_string())]).unwrap();
    catalog.insert("data", vec![Expr::String("A".to_string())]).unwrap();
    catalog.insert("data", vec![Expr::String("B".to_string())]).unwrap();

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        true,
        vec![Expr::Column("category".to_string())],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(rows.len(), 2);
}
