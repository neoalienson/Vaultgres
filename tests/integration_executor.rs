// Integration tests for executor operators using the high-level catalog API
// These tests verify the new Volcano-style executor model works correctly

use vaultgres::catalog::{Catalog, Value};
use vaultgres::parser::ast::{
    AggregateFunc, BinaryOperator, ColumnDef, DataType, Expr, OrderByExpr,
};

#[test]
fn test_filter_through_catalog() {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];
    catalog.create_table("test".to_string(), columns).unwrap();
    catalog.insert("test", vec![Expr::Number(1), Expr::Number(10)]).unwrap();
    catalog.insert("test", vec![Expr::Number(2), Expr::Number(20)]).unwrap();
    catalog.insert("test", vec![Expr::Number(3), Expr::Number(30)]).unwrap();

    let catalog_arc = std::sync::Arc::new(catalog.clone());

    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: BinaryOperator::GreaterThan,
        right: Box::new(Expr::Number(1)),
    });

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "test",
        false,
        vec![Expr::Star],
        where_clause,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();

    assert_eq!(rows.len(), 2); // id > 1 should return 2 rows
}

#[test]
fn test_limit_through_catalog() {
    let catalog = Catalog::new();
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];
    catalog.create_table("test".to_string(), columns).unwrap();
    catalog.insert("test", vec![Expr::Number(1)]).unwrap();
    catalog.insert("test", vec![Expr::Number(2)]).unwrap();
    catalog.insert("test", vec![Expr::Number(3)]).unwrap();

    let catalog_arc = std::sync::Arc::new(catalog.clone());

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "test",
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
fn test_project_through_catalog() {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("name".to_string(), DataType::Text),
    ];
    catalog.create_table("test".to_string(), columns).unwrap();
    catalog.insert("test", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();

    let catalog_arc = std::sync::Arc::new(catalog.clone());

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "test",
        false,
        vec![Expr::Column("name".to_string())],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 1); // Only name column
}

#[test]
fn test_distinct_through_catalog() {
    let catalog = Catalog::new();
    let columns = vec![ColumnDef::new("value".to_string(), DataType::Int)];
    catalog.create_table("test".to_string(), columns).unwrap();
    catalog.insert("test", vec![Expr::Number(1)]).unwrap();
    catalog.insert("test", vec![Expr::Number(1)]).unwrap();
    catalog.insert("test", vec![Expr::Number(2)]).unwrap();

    let catalog_arc = std::sync::Arc::new(catalog.clone());

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "test",
        true, // distinct
        vec![Expr::Star],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();

    assert_eq!(rows.len(), 2); // Should have 2 distinct values
}

#[test]
fn test_sort_through_catalog() {
    let catalog = Catalog::new();
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];
    catalog.create_table("test".to_string(), columns).unwrap();
    catalog.insert("test", vec![Expr::Number(3)]).unwrap();
    catalog.insert("test", vec![Expr::Number(1)]).unwrap();
    catalog.insert("test", vec![Expr::Number(2)]).unwrap();

    let catalog_arc = std::sync::Arc::new(catalog.clone());

    let order_by = Some(vec![OrderByExpr { column: "id".to_string(), ascending: true }]);

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "test",
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
fn test_aggregate_through_catalog() {
    let catalog = Catalog::new();
    let columns = vec![ColumnDef::new("value".to_string(), DataType::Int)];
    catalog.create_table("test".to_string(), columns).unwrap();
    catalog.insert("test", vec![Expr::Number(10)]).unwrap();
    catalog.insert("test", vec![Expr::Number(20)]).unwrap();
    catalog.insert("test", vec![Expr::Number(30)]).unwrap();

    let catalog_arc = std::sync::Arc::new(catalog.clone());

    let agg_exprs = vec![Expr::Aggregate {
        func: AggregateFunc::Sum,
        arg: Box::new(Expr::Column("value".to_string())),
    }];

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "test",
        false,
        agg_exprs,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int(60));
}

#[test]
fn test_seq_scan_through_catalog() {
    let catalog = Catalog::new();
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];
    catalog.create_table("test".to_string(), columns).unwrap();
    catalog.insert("test", vec![Expr::Number(1)]).unwrap();
    catalog.insert("test", vec![Expr::Number(2)]).unwrap();

    let catalog_arc = std::sync::Arc::new(catalog.clone());

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "test",
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
}

#[test]
fn test_combined_operators() {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];
    catalog.create_table("test".to_string(), columns).unwrap();
    catalog.insert("test", vec![Expr::Number(1), Expr::Number(10)]).unwrap();
    catalog.insert("test", vec![Expr::Number(2), Expr::Number(20)]).unwrap();
    catalog.insert("test", vec![Expr::Number(3), Expr::Number(30)]).unwrap();
    catalog.insert("test", vec![Expr::Number(4), Expr::Number(40)]).unwrap();

    let catalog_arc = std::sync::Arc::new(catalog.clone());

    // Filter: value > 15, Order by id DESC, Limit 2
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("value".to_string())),
        op: BinaryOperator::GreaterThan,
        right: Box::new(Expr::Number(15)),
    });

    let order_by = Some(vec![OrderByExpr { column: "id".to_string(), ascending: false }]);

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "test",
        false,
        vec![Expr::Star],
        where_clause,
        None,
        None,
        order_by,
        Some(2),
        None,
    )
    .unwrap();

    // Should return id=4 and id=3 (value > 15, ordered by id DESC, limited to 2)
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Int(4));
    assert_eq!(rows[1][0], Value::Int(3));
}
