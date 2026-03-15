// Integration tests for VIEW functionality - Schema derivation with prefixed columns
use std::sync::Arc;
use vaultgres::catalog::{Catalog, TableSchema};
use vaultgres::parser::ast::{BinaryOperator, ColumnDef, DataType, Expr, JoinClause, JoinType};
use vaultgres::planner::planner::Planner;

fn create_test_catalog() -> Arc<Catalog> {
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
        ColumnDef {
            name: "email".to_string(),
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

    // Create items table
    let items_columns = vec![
        ColumnDef {
            name: "item_id".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_unique: true,
            is_auto_increment: false,
            is_not_null: false,
            default_value: None,
            foreign_key: None,
        },
        ColumnDef {
            name: "sku".to_string(),
            data_type: DataType::Text,
            is_primary_key: false,
            is_unique: false,
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
        ColumnDef {
            name: "price".to_string(),
            data_type: DataType::Int,
            is_primary_key: false,
            is_unique: false,
            is_auto_increment: false,
            is_not_null: false,
            default_value: None,
            foreign_key: None,
        },
        ColumnDef {
            name: "category".to_string(),
            data_type: DataType::Text,
            is_primary_key: false,
            is_unique: false,
            is_auto_increment: false,
            is_not_null: false,
            default_value: None,
            foreign_key: None,
        },
    ];
    catalog.create_table("items".to_string(), items_columns).unwrap();

    Arc::new(catalog)
}

#[test]
fn test_view_schema_derivation_with_prefixed_columns() {
    let catalog = create_test_catalog();

    // Get base schemas
    let orders_schema = catalog.get_table("orders").unwrap();
    let customers_schema = catalog.get_table("customers").unwrap();

    // Create a combined schema (as would be done for a JOIN view)
    let mut combined_schema = customers_schema.clone();
    combined_schema.columns.extend(orders_schema.columns.clone());

    // Test projection with table-prefixed columns
    let projection = vec![
        Expr::Column("c.name".to_string()),
        Expr::Alias {
            alias: "order_id".to_string(),
            expr: Box::new(Expr::Column("o.id".to_string())),
        },
        Expr::Column("o.total".to_string()),
    ];

    let result = Planner::derive_projection_schema(&combined_schema, &projection);
    assert!(
        result.is_ok(),
        "Schema derivation with prefixed columns should succeed: {:?}",
        result.err()
    );

    let schema = result.unwrap();
    assert_eq!(schema.columns.len(), 3);
    assert_eq!(schema.columns[0].name, "name");
    assert_eq!(schema.columns[1].name, "order_id");
    assert_eq!(schema.columns[2].name, "total");
}

#[test]
fn test_view_schema_derivation_simple_view() {
    let catalog = create_test_catalog();

    // Create a simple view (no JOIN)
    use vaultgres::parser::ast::SelectStmt;
    let view_stmt = SelectStmt {
        distinct: false,
        columns: vec![Expr::Column("id".to_string()), Expr::Column("name".to_string())],
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

    catalog.create_view("customer_names".to_string(), view_stmt).unwrap();

    // Query the view
    let select_from_view = SelectStmt {
        distinct: false,
        columns: vec![Expr::Star],
        from: "customer_names".to_string(),
        table_alias: None,
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let planner = Planner::new(Some(catalog));
    let result = planner.plan(&select_from_view);
    assert!(result.is_ok(), "Simple view should plan successfully: {:?}", result.err());
}

#[test]
fn test_view_schema_columns_match() {
    let catalog = create_test_catalog();

    use vaultgres::parser::ast::SelectStmt;

    // Create a view with specific column selection
    let view_stmt = SelectStmt {
        distinct: false,
        columns: vec![
            Expr::Column("name".to_string()),
            Expr::Alias {
                alias: "customer_id".to_string(),
                expr: Box::new(Expr::Column("id".to_string())),
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

    catalog.create_view("customer_info".to_string(), view_stmt.clone()).unwrap();

    // Verify the view was stored correctly
    let stored_view = catalog.get_view("customer_info");
    assert!(stored_view.is_some(), "View should be stored in catalog");

    let stored = stored_view.unwrap();
    assert_eq!(stored.columns.len(), 2);

    // Verify column names in view definition
    match &stored.columns[0] {
        Expr::Column(name) => assert_eq!(name, "name"),
        _ => panic!("Expected Column expression"),
    }
    match &stored.columns[1] {
        Expr::Alias { alias, .. } => assert_eq!(alias, "customer_id"),
        _ => panic!("Expected Alias expression"),
    }
}
