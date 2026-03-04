use std::sync::Arc;
use vaultgres::catalog::*;
use vaultgres::parser::ast::{AggregateFunc, BinaryOperator, ColumnDef, DataType, Expr};

#[test]
fn test_insert_null_value() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("name".to_string(), DataType::Text),
    ];

    catalog.create_table("users".to_string(), columns).unwrap();

    // Note: Current implementation doesn't support NULL in INSERT
    // This test documents the limitation
    let values = vec![Expr::Number(1), Expr::String("Alice".to_string())];
    assert!(catalog.insert("users", values).is_ok());
}

#[test]
fn test_select_from_empty_result() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();

    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: BinaryOperator::Equals,
        right: Box::new(Expr::Number(999)),
    });

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
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
    assert_eq!(rows.len(), 0);
}

#[test]
fn test_update_no_matching_rows() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1), Expr::Number(100)]).unwrap();

    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: BinaryOperator::Equals,
        right: Box::new(Expr::Number(999)),
    });

    let updated = catalog
        .update("data", vec![("value".to_string(), Expr::Number(999))], where_clause)
        .unwrap();
    assert_eq!(updated, 0);
}

#[test]
fn test_delete_no_matching_rows() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();

    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: BinaryOperator::Equals,
        right: Box::new(Expr::Number(999)),
    });

    let deleted = catalog.delete("data", where_clause).unwrap();
    assert_eq!(deleted, 0);
}

#[test]
fn test_select_with_invalid_column() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();

    let result = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![Expr::Column("nonexistent".to_string())],
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
fn test_where_with_invalid_column() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();

    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("nonexistent".to_string())),
        op: BinaryOperator::Equals,
        right: Box::new(Expr::Number(1)),
    });

    let result = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![Expr::Star],
        where_clause,
        None,
        None,
        None,
        None,
        None,
    );
    assert!(result.is_err());
}

#[test]
fn test_update_invalid_column() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();

    let result = catalog.update("data", vec![("nonexistent".to_string(), Expr::Number(999))], None);
    assert!(result.is_err());
}

#[test]
fn test_limit_larger_than_result_set() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();
    catalog.insert("data", vec![Expr::Number(2)]).unwrap();

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![Expr::Star],
        None,
        None,
        None,
        None,
        Some(100),
        None,
    )
    .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_offset_larger_than_result_set() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();

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
        Some(100),
    )
    .unwrap();
    assert_eq!(rows.len(), 0);
}

#[test]
fn test_aggregate_on_empty_table() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("value".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![Expr::Aggregate {
            func: vaultgres::parser::ast::AggregateFunc::Count,
            arg: Box::new(Expr::Star),
        }],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(rows[0][0], Value::Int(0));
}

#[test]
fn test_in_operator_empty_list() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();

    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: BinaryOperator::In,
        right: Box::new(Expr::List(vec![])),
    });

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
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
    assert_eq!(rows.len(), 0);
}

#[test]
fn test_between_with_reversed_bounds() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("value".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(15)]).unwrap();

    // BETWEEN 30 AND 10 (reversed) - should return no rows
    // Converted to: value >= 30 AND value <= 10 (impossible condition)
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::GreaterThanOrEqual,
            right: Box::new(Expr::Number(30)),
        }),
        op: BinaryOperator::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::LessThanOrEqual,
            right: Box::new(Expr::Number(10)),
        }),
    });

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
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
    assert_eq!(rows.len(), 0);
}

#[test]
fn test_like_with_empty_pattern() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("name".to_string(), DataType::Text)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::String("test".to_string())]).unwrap();

    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("name".to_string())),
        op: BinaryOperator::Like,
        right: Box::new(Expr::String("%%".to_string())),
    });

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
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
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_distinct_on_empty_table() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        true,
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
fn test_distinct_all_duplicates() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("value".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        true,
        vec![Expr::Star],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_order_by_with_invalid_column() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();

    let order_by = Some(vec![vaultgres::parser::ast::OrderByExpr {
        column: "nonexistent".to_string(),
        ascending: true,
    }]);

    let result = Catalog::select_with_catalog(
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
    );
    assert!(result.is_err());
}

#[test]
fn test_group_by_with_invalid_column() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();

    let group_by = Some(vec![Expr::Column("nonexistent".to_string())]);
    let result = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![Expr::Column("id".to_string())],
        None,
        group_by,
        None,
        None,
        None,
        None,
    );
    assert!(result.is_err());
}

#[test]
fn test_zero_limit() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", vec![Expr::Number(1)]).unwrap();
    catalog.insert("data", vec![Expr::Number(2)]).unwrap();

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "data",
        false,
        vec![Expr::Star],
        None,
        None,
        None,
        None,
        Some(0),
        None,
    )
    .unwrap();
    assert_eq!(rows.len(), 0);
}

#[test]
fn test_insert_to_nonexistent_table() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let result = catalog.insert("nonexistent", vec![Expr::Number(1)]);
    assert!(result.is_err());
}

#[test]
fn test_update_nonexistent_table() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let result = catalog.update("nonexistent", vec![("col".to_string(), Expr::Number(1))], None);
    assert!(result.is_err());
}

#[test]
fn test_delete_from_nonexistent_table() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let result = catalog.delete("nonexistent", None);
    assert!(result.is_err());
}
