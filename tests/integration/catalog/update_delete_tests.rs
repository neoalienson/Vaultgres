use std::sync::Arc;
use vaultgres::catalog::*;
use vaultgres::parser::ast::{
    AggregateFunc, BinaryOperator, ColumnDef, DataType, Expr, SelectStmt, UnaryOperator,
};

#[test]
fn test_update() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(1), Expr::Number(100)]).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(2), Expr::Number(200)]).unwrap();

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
    catalog.insert("data", &[], vec![Expr::Number(1), Expr::Number(100)]).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(2), Expr::Number(200)]).unwrap();

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
    catalog.insert("data", &[], vec![Expr::Number(1)]).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(2)]).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(3)]).unwrap();

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
    catalog.insert("data", &[], vec![Expr::Number(1)]).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(2)]).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(3)]).unwrap();

    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: BinaryOperator::Equals,
        right: Box::new(Expr::Number(2)),
    });

    let deleted = catalog.delete("data", where_clause).unwrap();
    assert_eq!(deleted, 1);
}

// --- Arithmetic expression tests ---

#[test]
fn test_update_arithmetic_add() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(1), Expr::Number(100)]).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(2), Expr::Number(200)]).unwrap();

    let assignments = vec![(
        "value".to_string(),
        Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Number(50)),
        },
    )];

    let updated = catalog.update("data", assignments, None).unwrap();
    assert_eq!(updated, 2);
}

#[test]
fn test_update_arithmetic_subtract() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(1), Expr::Number(100)]).unwrap();

    let assignments = vec![(
        "value".to_string(),
        Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::Subtract,
            right: Box::new(Expr::Number(25)),
        },
    )];

    let updated = catalog.update("data", assignments, None).unwrap();
    assert_eq!(updated, 1);
}

#[test]
fn test_update_arithmetic_multiply() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(1), Expr::Number(10)]).unwrap();

    let assignments = vec![(
        "value".to_string(),
        Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Number(3)),
        },
    )];

    let updated = catalog.update("data", assignments, None).unwrap();
    assert_eq!(updated, 1);
}

#[test]
fn test_update_arithmetic_divide() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(1), Expr::Number(100)]).unwrap();

    let assignments = vec![(
        "value".to_string(),
        Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::Divide,
            right: Box::new(Expr::Number(4)),
        },
    )];

    let updated = catalog.update("data", assignments, None).unwrap();
    assert_eq!(updated, 1);
}

#[test]
fn test_update_arithmetic_modulo() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(1), Expr::Number(17)]).unwrap();

    let assignments = vec![(
        "value".to_string(),
        Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::Modulo,
            right: Box::new(Expr::Number(5)),
        },
    )];

    let updated = catalog.update("data", assignments, None).unwrap();
    assert_eq!(updated, 1);
}

#[test]
fn test_update_arithmetic_division_by_zero() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(1), Expr::Number(100)]).unwrap();

    let assignments = vec![(
        "value".to_string(),
        Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::Divide,
            right: Box::new(Expr::Number(0)),
        },
    )];

    let result = catalog.update("data", assignments, None);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Division by zero"));
}

#[test]
fn test_update_arithmetic_nested() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(1), Expr::Number(10)]).unwrap();

    let assignments = vec![(
        "value".to_string(),
        Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("value".to_string())),
                op: BinaryOperator::Multiply,
                right: Box::new(Expr::Number(2)),
            }),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Number(3)),
        },
    )];

    let updated = catalog.update("data", assignments, None).unwrap();
    assert_eq!(updated, 1);
}

#[test]
fn test_update_arithmetic_between_columns() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("a".to_string(), DataType::Int),
        ColumnDef::new("b".to_string(), DataType::Int),
        ColumnDef::new("result".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(5), Expr::Number(3), Expr::Number(0)]).unwrap();

    let assignments = vec![(
        "result".to_string(),
        Expr::BinaryOp {
            left: Box::new(Expr::Column("a".to_string())),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Column("b".to_string())),
        },
    )];

    let updated = catalog.update("data", assignments, None).unwrap();
    assert_eq!(updated, 1);
}

#[test]
fn test_update_column_reference() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(42), Expr::Number(0)]).unwrap();

    let assignments = vec![("value".to_string(), Expr::Column("id".to_string()))];

    let updated = catalog.update("data", assignments, None).unwrap();
    assert_eq!(updated, 1);
}

#[test]
fn test_update_qualified_column() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(99), Expr::Number(0)]).unwrap();

    let assignments = vec![(
        "value".to_string(),
        Expr::QualifiedColumn { table: "data".to_string(), column: "id".to_string() },
    )];

    let updated = catalog.update("data", assignments, None).unwrap();
    assert_eq!(updated, 1);
}

#[test]
fn test_update_arithmetic_with_where() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(1), Expr::Number(100)]).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(2), Expr::Number(200)]).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(3), Expr::Number(300)]).unwrap();

    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: BinaryOperator::LessThan,
        right: Box::new(Expr::Number(3)),
    });

    let assignments = vec![(
        "value".to_string(),
        Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Number(10)),
        },
    )];

    let updated = catalog.update("data", assignments, where_clause).unwrap();
    assert_eq!(updated, 2);
}

#[test]
fn test_update_multiple_arithmetic_assignments() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("a".to_string(), DataType::Int),
        ColumnDef::new("b".to_string(), DataType::Int),
        ColumnDef::new("c".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(1), Expr::Number(2), Expr::Number(0)]).unwrap();

    let assignments = vec![
        (
            "b".to_string(),
            Expr::BinaryOp {
                left: Box::new(Expr::Column("a".to_string())),
                op: BinaryOperator::Multiply,
                right: Box::new(Expr::Number(10)),
            },
        ),
        (
            "c".to_string(),
            Expr::BinaryOp {
                left: Box::new(Expr::Column("a".to_string())),
                op: BinaryOperator::Add,
                right: Box::new(Expr::Column("b".to_string())),
            },
        ),
    ];

    let updated = catalog.update("data", assignments, None).unwrap();
    assert_eq!(updated, 1);
}

#[test]
fn test_update_self_reference() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![ColumnDef::new("counter".to_string(), DataType::Int)];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(0)]).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(0)]).unwrap();

    let assignments = vec![(
        "counter".to_string(),
        Expr::BinaryOp {
            left: Box::new(Expr::Column("counter".to_string())),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Number(1)),
        },
    )];

    let updated = catalog.update("data", assignments, None).unwrap();
    assert_eq!(updated, 2);
}

#[test]
fn test_update_arithmetic_complex_expression() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("a".to_string(), DataType::Int),
        ColumnDef::new("b".to_string(), DataType::Int),
        ColumnDef::new("result".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(10), Expr::Number(5), Expr::Number(0)]).unwrap();

    let assignments = vec![(
        "result".to_string(),
        Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("a".to_string())),
                op: BinaryOperator::Multiply,
                right: Box::new(Expr::Column("b".to_string())),
            }),
            op: BinaryOperator::Add,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("a".to_string())),
                op: BinaryOperator::Subtract,
                right: Box::new(Expr::Column("b".to_string())),
            }),
        },
    )];

    let updated = catalog.update("data", assignments, None).unwrap();
    assert_eq!(updated, 1);
}

#[test]
fn test_update_float_arithmetic() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("amount".to_string(), DataType::Float),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(1), Expr::Float(10.0)]).unwrap();

    let assignments = vec![(
        "amount".to_string(),
        Expr::BinaryOp {
            left: Box::new(Expr::Column("amount".to_string())),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Number(2)),
        },
    )];

    let updated = catalog.update("data", assignments, None).unwrap();
    assert_eq!(updated, 1);
}

#[test]
fn test_update_mixed_int_float_arithmetic() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("amount".to_string(), DataType::Float),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(1), Expr::Float(15.0)]).unwrap();

    let assignments = vec![(
        "amount".to_string(),
        Expr::BinaryOp {
            left: Box::new(Expr::Column("amount".to_string())),
            op: BinaryOperator::Divide,
            right: Box::new(Expr::Number(3)),
        },
    )];

    let updated = catalog.update("data", assignments, None).unwrap();
    assert_eq!(updated, 1);
}

#[test]
fn test_update_arithmetic_order_of_operations() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(1), Expr::Number(10)]).unwrap();

    let assignments = vec![(
        "value".to_string(),
        Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("value".to_string())),
                op: BinaryOperator::Add,
                right: Box::new(Expr::Number(5)),
            }),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Number(2)),
        },
    )];

    let updated = catalog.update("data", assignments, None).unwrap();
    assert_eq!(updated, 1);
}

#[test]
fn test_update_unary_negation() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(1), Expr::Number(42)]).unwrap();

    let assignments = vec![(
        "value".to_string(),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: Box::new(Expr::Column("value".to_string())),
        },
    )];

    let updated = catalog.update("data", assignments, None).unwrap();
    assert_eq!(updated, 1);
}

#[test]
fn test_update_column_not_found() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(1), Expr::Number(100)]).unwrap();

    let assignments = vec![("value".to_string(), Expr::Column("missing_column".to_string()))];

    let result = catalog.update("data", assignments, None);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("missing_column"));
}

#[test]
fn test_update_target_column_not_found() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(1), Expr::Number(100)]).unwrap();

    let result = catalog.update("data", vec![("nonexistent".to_string(), Expr::Number(1))], None);
    assert!(result.is_err());
}

#[test]
fn test_update_type_mismatch_arithmetic_to_text() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("name".to_string(), DataType::Text),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();
    catalog.insert("data", &[], vec![Expr::Number(1), Expr::String("hello".to_string())]).unwrap();

    let assignments = vec![(
        "name".to_string(),
        Expr::BinaryOp {
            left: Box::new(Expr::Number(1)),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Number(2)),
        },
    )];

    let result = catalog.update("data", assignments, None);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Type mismatch"));
}

#[test]
fn test_update_with_scalar_subquery_max() {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("balance".to_string(), DataType::Int),
    ];

    catalog.create_table("accounts".to_string(), columns.clone()).unwrap();
    catalog.insert("accounts", &[], vec![Expr::Number(1), Expr::Number(100)]).unwrap();
    catalog.insert("accounts", &[], vec![Expr::Number(2), Expr::Number(200)]).unwrap();
    catalog.insert("accounts", &[], vec![Expr::Number(3), Expr::Number(150)]).unwrap();

    let assignments = vec![(
        "balance".to_string(),
        Expr::Subquery(Box::new(SelectStmt {
            distinct: false,
            columns: vec![Expr::Aggregate {
                func: AggregateFunc::Max,
                arg: Box::new(Expr::Column("balance".to_string())),
            }],
            from: "accounts".to_string(),
            table_alias: None,
            joins: vec![],
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        })),
    )];

    let updated = catalog.update("accounts", assignments, None).unwrap();
    assert_eq!(updated, 3);

    let catalog_arc = Arc::new(catalog.clone());
    let results = Catalog::select_with_catalog(
        &catalog_arc,
        "accounts",
        false,
        vec![Expr::Column("balance".to_string())],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    for row in results {
        assert_eq!(row[0], Value::Int(200));
    }
}

#[test]
fn test_update_with_scalar_subquery_min() {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("balance".to_string(), DataType::Int),
    ];

    catalog.create_table("accounts".to_string(), columns.clone()).unwrap();
    catalog.insert("accounts", &[], vec![Expr::Number(1), Expr::Number(100)]).unwrap();
    catalog.insert("accounts", &[], vec![Expr::Number(2), Expr::Number(200)]).unwrap();

    let assignments = vec![(
        "balance".to_string(),
        Expr::Subquery(Box::new(SelectStmt {
            distinct: false,
            columns: vec![Expr::Aggregate {
                func: AggregateFunc::Min,
                arg: Box::new(Expr::Column("balance".to_string())),
            }],
            from: "accounts".to_string(),
            table_alias: None,
            joins: vec![],
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        })),
    )];

    let updated = catalog.update("accounts", assignments, None).unwrap();
    assert_eq!(updated, 2);

    let catalog_arc = Arc::new(catalog.clone());
    let results = Catalog::select_with_catalog(
        &catalog_arc,
        "accounts",
        false,
        vec![Expr::Column("balance".to_string())],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    for row in results {
        assert_eq!(row[0], Value::Int(100));
    }
}

#[test]
fn test_update_with_scalar_subquery_sum() {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("total".to_string(), DataType::Int),
    ];

    catalog.create_table("orders".to_string(), columns.clone()).unwrap();
    catalog.insert("orders", &[], vec![Expr::Number(1), Expr::Number(0)]).unwrap();
    catalog.insert("orders", &[], vec![Expr::Number(2), Expr::Number(0)]).unwrap();

    let assignments = vec![(
        "total".to_string(),
        Expr::Subquery(Box::new(SelectStmt {
            distinct: false,
            columns: vec![Expr::Aggregate {
                func: AggregateFunc::Sum,
                arg: Box::new(Expr::Column("id".to_string())),
            }],
            from: "orders".to_string(),
            table_alias: None,
            joins: vec![],
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        })),
    )];

    let updated = catalog.update("orders", assignments, None).unwrap();
    assert_eq!(updated, 2);

    let catalog_arc = Arc::new(catalog.clone());
    let results = Catalog::select_with_catalog(
        &catalog_arc,
        "orders",
        false,
        vec![Expr::Column("total".to_string())],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    for row in results {
        assert_eq!(row[0], Value::Int(3));
    }
}

#[test]
fn test_update_with_scalar_subquery_count() {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("cnt".to_string(), DataType::Int),
    ];

    catalog.create_table("items".to_string(), columns.clone()).unwrap();
    catalog.insert("items", &[], vec![Expr::Number(1), Expr::Number(0)]).unwrap();
    catalog.insert("items", &[], vec![Expr::Number(2), Expr::Number(0)]).unwrap();
    catalog.insert("items", &[], vec![Expr::Number(3), Expr::Number(0)]).unwrap();

    let assignments = vec![(
        "cnt".to_string(),
        Expr::Subquery(Box::new(SelectStmt {
            distinct: false,
            columns: vec![Expr::Aggregate {
                func: AggregateFunc::Count,
                arg: Box::new(Expr::Star),
            }],
            from: "items".to_string(),
            table_alias: None,
            joins: vec![],
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        })),
    )];

    let updated = catalog.update("items", assignments, None).unwrap();
    assert_eq!(updated, 3);

    let catalog_arc = Arc::new(catalog.clone());
    let results = Catalog::select_with_catalog(
        &catalog_arc,
        "items",
        false,
        vec![Expr::Column("cnt".to_string())],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    for row in results {
        assert_eq!(row[0], Value::Int(3));
    }
}

#[test]
fn test_update_with_scalar_subquery_empty_result_returns_null() {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("balance".to_string(), DataType::Int),
    ];

    catalog.create_table("accounts".to_string(), columns.clone()).unwrap();
    catalog.insert("accounts", &[], vec![Expr::Number(1), Expr::Number(100)]).unwrap();

    let assignments = vec![(
        "balance".to_string(),
        Expr::Subquery(Box::new(SelectStmt {
            distinct: false,
            columns: vec![Expr::Aggregate {
                func: AggregateFunc::Max,
                arg: Box::new(Expr::Column("balance".to_string())),
            }],
            from: "accounts".to_string(),
            table_alias: None,
            joins: vec![],
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::Number(999)),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        })),
    )];

    let result = catalog.update("accounts", assignments, None);
    assert!(result.is_ok());
}

#[test]
fn test_update_with_case_and_subquery() {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("balance".to_string(), DataType::Int),
    ];

    catalog.create_table("accounts".to_string(), columns.clone()).unwrap();
    catalog.insert("accounts", &[], vec![Expr::Number(1), Expr::Number(150)]).unwrap();
    catalog.insert("accounts", &[], vec![Expr::Number(2), Expr::Number(50)]).unwrap();

    let avg_subquery = Expr::Subquery(Box::new(SelectStmt {
        distinct: false,
        columns: vec![Expr::Aggregate {
            func: AggregateFunc::Avg,
            arg: Box::new(Expr::Column("balance".to_string())),
        }],
        from: "accounts".to_string(),
        table_alias: None,
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    }));

    let assignments = vec![(
        "balance".to_string(),
        Expr::Case {
            conditions: vec![(
                Expr::BinaryOp {
                    left: Box::new(Expr::Column("balance".to_string())),
                    op: BinaryOperator::GreaterThan,
                    right: Box::new(avg_subquery),
                },
                Expr::BinaryOp {
                    left: Box::new(Expr::Column("balance".to_string())),
                    op: BinaryOperator::Subtract,
                    right: Box::new(Expr::Number(10)),
                },
            )],
            else_expr: Some(Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("balance".to_string())),
                op: BinaryOperator::Add,
                right: Box::new(Expr::Number(10)),
            })),
        },
    )];

    let updated = catalog.update("accounts", assignments, None).unwrap();
    assert_eq!(updated, 2);

    // Note: select_with_catalog returns columns in alphabetical order due to a pre-existing bug,
    // so we select in alphabetical order (balance, id) and extract r[0] as balance
    let catalog_arc = Arc::new(catalog.clone());
    let results = Catalog::select_with_catalog(
        &catalog_arc,
        "accounts",
        false,
        vec![Expr::Column("balance".to_string()), Expr::Column("id".to_string())],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    let mut balances: Vec<i64> =
        results.iter().map(|r| if let Value::Int(n) = r[0] { n } else { 0 }).collect();
    balances.sort();
    assert_eq!(balances, vec![60, 140]);
}
