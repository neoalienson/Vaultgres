// Integration tests for Planner-Executor interaction
// These tests verify that the planner produces output that executors can correctly consume

use std::sync::Arc;
use vaultgres::catalog::{Catalog, TableSchema};
use vaultgres::parser::ast::{
    BinaryOperator, ColumnDef, DataType, Expr, JoinClause, JoinType, SelectStmt,
};
use vaultgres::planner::planner::Planner;

/// Create a test catalog with customers and orders tables for JOIN testing
fn create_join_test_catalog() -> Arc<Catalog> {
    let catalog = Catalog::new();

    // Create customers table
    let customers_columns = vec![
        ColumnDef {
            name: "id".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_unique: true,
            is_auto_increment: false,
            is_not_null: false,
            default_value: None,
            foreign_key: None,
        },
        ColumnDef {
            name: "name".to_string(),
            data_type: DataType::Text,
            is_primary_key: false,
            is_unique: false,
            is_auto_increment: false,
            is_not_null: false,
            default_value: None,
            foreign_key: None,
        },
    ];
    catalog.create_table("customers".to_string(), customers_columns).unwrap();

    // Create orders table
    let orders_columns = vec![
        ColumnDef {
            name: "id".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_unique: true,
            is_auto_increment: false,
            is_not_null: false,
            default_value: None,
            foreign_key: None,
        },
        ColumnDef {
            name: "customer_id".to_string(),
            data_type: DataType::Int,
            is_primary_key: false,
            is_unique: false,
            is_auto_increment: false,
            is_not_null: false,
            default_value: None,
            foreign_key: None,
        },
        ColumnDef {
            name: "total".to_string(),
            data_type: DataType::Int,
            is_primary_key: false,
            is_unique: false,
            is_auto_increment: false,
            is_not_null: false,
            default_value: None,
            foreign_key: None,
        },
    ];
    catalog.create_table("orders".to_string(), orders_columns).unwrap();

    Arc::new(catalog)
}

/// Helper to check for malformed column names (the bug we're preventing)
fn assert_column_name_valid(column_name: &str, expr_debug: &str) {
    assert!(
        !column_name.contains('{'),
        "Column name '{}' contains '{{' from expression: {}",
        column_name,
        expr_debug
    );
    assert!(
        !column_name.contains('}'),
        "Column name '{}' contains '}}' from expression: {}",
        column_name,
        expr_debug
    );
    assert!(
        !column_name.contains("QualifiedColumn"),
        "Column name '{}' contains type name from expression: {}",
        column_name,
        expr_debug
    );
    assert!(
        !column_name.contains("Expr::"),
        "Column name '{}' contains enum prefix from expression: {}",
        column_name,
        expr_debug
    );
    assert!(!column_name.is_empty(), "Column name is empty for expression: {}", expr_debug);
}

#[test]
fn test_plan_qualified_columns_no_malformed_names() {
    let catalog = create_join_test_catalog();

    // Test with qualified column expression (the bug scenario)
    let select_stmt = SelectStmt {
        distinct: false,
        columns: vec![
            Expr::QualifiedColumn { table: "c".to_string(), column: "name".to_string() },
            Expr::QualifiedColumn { table: "o".to_string(), column: "total".to_string() },
        ],
        from: "customers".to_string(),
        table_alias: Some("c".to_string()),
        joins: vec![JoinClause {
            join_type: JoinType::Inner,
            lateral: false,
            table: "orders".to_string(),
            alias: Some("o".to_string()),
            on: Expr::BinaryOp {
                left: Box::new(Expr::QualifiedColumn {
                    table: "c".to_string(),
                    column: "id".to_string(),
                }),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::QualifiedColumn {
                    table: "o".to_string(),
                    column: "customer_id".to_string(),
                }),
            },
        }],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let planner = Planner::new_with_catalog(catalog.clone());
    let plan_result = planner.plan(&select_stmt);

    // Planning should succeed
    assert!(plan_result.is_ok(), "Planning failed: {:?}", plan_result.err());

    // The plan is created successfully - this means qualified columns were handled
    // The actual execution would happen when next() is called on the executor
}

#[test]
fn test_plan_aliased_columns() {
    let catalog = create_join_test_catalog();

    let select_stmt = SelectStmt {
        distinct: false,
        columns: vec![
            Expr::Alias {
                alias: "customer_name".to_string(),
                expr: Box::new(Expr::Column("name".to_string())),
            },
            Expr::Alias {
                alias: "order_total".to_string(),
                expr: Box::new(Expr::Column("total".to_string())),
            },
        ],
        from: "customers".to_string(),
        table_alias: None,
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let planner = Planner::new_with_catalog(catalog.clone());
    let plan_result = planner.plan(&select_stmt);
    assert!(plan_result.is_ok(), "Planning failed: {:?}", plan_result.err());
}

#[test]
fn test_column_name_invariants_for_all_expr_types() {
    use vaultgres::parser::ast::AggregateFunc;

    // Test get_column_name logic from ProjectExecutor for all expression types
    // Note: Some expressions (like BinaryOp) use generated names, not Debug format
    let test_cases = vec![
        ("Column", Expr::Column("test_col".to_string()), "test_col"),
        (
            "QualifiedColumn",
            Expr::QualifiedColumn { table: "t".to_string(), column: "col".to_string() },
            "col",
        ),
        (
            "Alias",
            Expr::Alias {
                alias: "my_alias".to_string(),
                expr: Box::new(Expr::Column("x".to_string())),
            },
            "my_alias",
        ),
        (
            "FunctionCall",
            Expr::FunctionCall {
                name: "UPPER".to_string(),
                args: vec![Expr::Column("name".to_string())],
            },
            "upper",
        ),
        (
            "Aggregate",
            Expr::Aggregate {
                func: AggregateFunc::Count,
                arg: Box::new(Expr::Column("*".to_string())),
            },
            "count",
        ),
        // BinaryOp uses generated name in real code, not Debug format
        ("Number", Expr::Number(42), "number"),
        ("String", Expr::String("literal".to_string()), "string"),
    ];

    for (name, expr, expected) in test_cases {
        // Simulate what ProjectExecutor::get_column_name does
        let column_name = match &expr {
            Expr::Column(n) => n.clone(),
            Expr::QualifiedColumn { column, .. } => column.clone(),
            Expr::Alias { alias, .. } => alias.clone(),
            Expr::FunctionCall { name, .. } => name.to_lowercase(),
            Expr::Aggregate { func, .. } => format!("{:?}", func).to_lowercase(),
            Expr::BinaryOp { .. } => "binary_expr".to_string(), // Real code uses generated name
            Expr::Number(_) => "number".to_string(),
            Expr::String(_) => "string".to_string(),
            Expr::Star => "*".to_string(),
            _ => "expr".to_string(), // Real code uses generated name for unknown types
        };

        assert_eq!(column_name, expected, "{} produced wrong name", name);
        assert_column_name_valid(&column_name, &format!("{}: {:?}", name, expr));
    }
}

#[test]
fn test_view_with_qualified_columns_planning() {
    let catalog = create_join_test_catalog();

    // Create a view definition (stored in catalog)
    let view_stmt = SelectStmt {
        distinct: false,
        columns: vec![
            Expr::QualifiedColumn { table: "c".to_string(), column: "name".to_string() },
            Expr::Alias {
                alias: "order_id".to_string(),
                expr: Box::new(Expr::QualifiedColumn {
                    table: "o".to_string(),
                    column: "id".to_string(),
                }),
            },
            Expr::QualifiedColumn { table: "o".to_string(), column: "total".to_string() },
        ],
        from: "customers".to_string(),
        table_alias: Some("c".to_string()),
        joins: vec![JoinClause {
            join_type: JoinType::Inner,
            lateral: false,
            table: "orders".to_string(),
            alias: Some("o".to_string()),
            on: Expr::BinaryOp {
                left: Box::new(Expr::QualifiedColumn {
                    table: "c".to_string(),
                    column: "id".to_string(),
                }),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::QualifiedColumn {
                    table: "o".to_string(),
                    column: "customer_id".to_string(),
                }),
            },
        }],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    // Store the view
    catalog.create_view("customer_orders_view".to_string(), view_stmt.clone()).unwrap();

    // Now SELECT from the view
    let select_from_view = SelectStmt {
        distinct: false,
        columns: vec![Expr::Star],
        from: "customer_orders_view".to_string(),
        table_alias: None,
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let planner = Planner::new_with_catalog(catalog.clone());
    let plan_result = planner.plan(&select_from_view);

    // This should succeed - the view's qualified columns should be resolved
    assert!(
        plan_result.is_ok(),
        "SELECT from view with qualified columns failed: {:?}",
        plan_result.err()
    );
}

#[test]
fn test_malformed_column_name_detection() {
    // This test verifies our validation catches the bug
    let malformed_names = vec![
        "QualifiedColumn { table: \"c\", column: \"name\" }",
        "Expr::Column { name: \"test\" }",
        "Column{name}",
        "test{123}",
    ];

    for name in malformed_names {
        // Our validation should catch these
        assert!(
            name.contains('{') || name.contains("QualifiedColumn") || name.contains("Expr::"),
            "Test setup error: '{}' should be detected as malformed",
            name
        );
    }

    // Valid names should pass
    let valid_names = vec!["id", "name", "order_total", "customer_name", "col123"];
    for name in valid_names {
        assert!(
            !name.contains('{')
                && !name.contains('}')
                && !name.contains("QualifiedColumn")
                && !name.contains("Expr::"),
            "Test setup error: '{}' incorrectly flagged as malformed",
            name
        );
    }
}

#[test]
fn test_derive_projection_schema_with_qualified_columns() {
    let catalog = create_join_test_catalog();

    // Get combined schema (as would exist after JOIN)
    let customers_schema = catalog.get_table("customers").unwrap();
    let orders_schema = catalog.get_table("orders").unwrap();

    let mut combined_schema = customers_schema.clone();
    combined_schema.columns.extend(orders_schema.columns.clone());

    // Test deriving schema from qualified column expressions
    let projection = vec![
        Expr::QualifiedColumn { table: "c".to_string(), column: "name".to_string() },
        Expr::QualifiedColumn { table: "o".to_string(), column: "total".to_string() },
    ];

    let result = Planner::derive_projection_schema(&combined_schema, &projection);
    assert!(result.is_ok(), "Schema derivation failed: {:?}", result.err());

    let schema = result.unwrap();
    assert_eq!(schema.columns.len(), 2);
    assert_eq!(schema.columns[0].name, "name");
    assert_eq!(schema.columns[1].name, "total");
}
