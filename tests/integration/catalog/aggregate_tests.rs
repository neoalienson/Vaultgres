use std::sync::Arc;
use vaultgres::catalog::*;
use vaultgres::parser::ast::{AggregateFunc, ColumnDef, DataType, Expr};

#[test]
fn test_aggregate_count() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();
    catalog.insert("data", vec![Expr::Number(2)]).unwrap();
    catalog.insert("data", vec![Expr::Number(3)]).unwrap();

    let agg_expr = Expr::Aggregate {
        func: vaultgres::parser::ast::AggregateFunc::Count,
        arg: Box::new(Expr::Number(1)),
    };
    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![agg_expr],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int(3));
}

#[test]
fn test_aggregate_sum() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("value".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(10)]).unwrap();
    catalog.insert("data", vec![Expr::Number(20)]).unwrap();
    catalog.insert("data", vec![Expr::Number(30)]).unwrap();

    let agg_expr = Expr::Aggregate {
        func: vaultgres::parser::ast::AggregateFunc::Sum,
        arg: Box::new(Expr::Column("value".to_string())),
    };
    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![agg_expr],
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
fn test_aggregate_avg() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("value".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(10)]).unwrap();
    catalog.insert("data", vec![Expr::Number(20)]).unwrap();
    catalog.insert("data", vec![Expr::Number(30)]).unwrap();

    let agg_expr = Expr::Aggregate {
        func: vaultgres::parser::ast::AggregateFunc::Avg,
        arg: Box::new(Expr::Column("value".to_string())),
    };
    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![agg_expr],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int(20));
}

#[test]
fn test_aggregate_min_max() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("value".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(10)]).unwrap();
    catalog.insert("data", vec![Expr::Number(50)]).unwrap();
    catalog.insert("data", vec![Expr::Number(30)]).unwrap();

    let agg_expr_min = Expr::Aggregate {
        func: vaultgres::parser::ast::AggregateFunc::Min,
        arg: Box::new(Expr::Column("value".to_string())),
    };
    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![agg_expr_min],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(rows[0][0], Value::Int(10));

    let agg_expr_max = Expr::Aggregate {
        func: vaultgres::parser::ast::AggregateFunc::Max,
        arg: Box::new(Expr::Column("value".to_string())),
    };
    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![agg_expr_max],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(rows[0][0], Value::Int(50));
}

#[test]
fn test_group_by() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("category".to_string(), DataType::Text),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::String("A".to_string()), Expr::Number(10)]).unwrap();
    catalog.insert("data", vec![Expr::String("B".to_string()), Expr::Number(20)]).unwrap();
    catalog.insert("data", vec![Expr::String("A".to_string()), Expr::Number(30)]).unwrap();
    catalog.insert("data", vec![Expr::String("B".to_string()), Expr::Number(40)]).unwrap();

    let group_by = Some(vec![Expr::Column("category".to_string())]);
    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![Expr::Column("category".to_string())],
        None,
        group_by,
        None,
        None,
        None,
        None,
    )
    .unwrap();

    assert_eq!(rows.len(), 2);
}

#[test]
fn test_having_clause() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("category".to_string(), DataType::Text),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::String("A".to_string()), Expr::Number(10)]).unwrap();
    catalog.insert("data", vec![Expr::String("B".to_string()), Expr::Number(20)]).unwrap();
    catalog.insert("data", vec![Expr::String("A".to_string()), Expr::Number(30)]).unwrap();
    catalog.insert("data", vec![Expr::String("C".to_string()), Expr::Number(5)]).unwrap();

    let group_by = Some(vec![Expr::Column("category".to_string())]);
    let having = Some(Expr::BinaryOp {
        left: Box::new(Expr::Number(2)),
        op: vaultgres::parser::ast::BinaryOperator::GreaterThan,
        right: Box::new(Expr::Number(1)),
    });

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![Expr::Column("category".to_string())],
        None,
        group_by,
        having,
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(rows.len(), 3);
}
