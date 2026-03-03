use vaultgres::catalog::Catalog;
use vaultgres::parser::ast::{ColumnDef, DataType, Expr, SelectStmt};

#[test]
fn test_create_materialized_view() {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("name".to_string(), DataType::Text),
    ];

    catalog.create_table("products".to_string(), columns).unwrap();

    let query = SelectStmt {
        distinct: false,
        columns: vec![Expr::Star],
        from: "products".to_string(),
        table_alias: None,
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    catalog.create_materialized_view("product_mv".to_string(), query).unwrap();
    assert!(catalog.get_materialized_view("product_mv").is_some());
}

#[test]
fn test_refresh_materialized_view() {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("value".to_string(), DataType::Int),
    ];

    catalog.create_table("data".to_string(), columns).unwrap();

    let query = SelectStmt {
        distinct: false,
        columns: vec![Expr::Star],
        from: "data".to_string(),
        table_alias: None,
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    catalog.create_materialized_view("data_mv".to_string(), query).unwrap();
    catalog.refresh_materialized_view("data_mv").unwrap();

    let result = catalog.get_materialized_view("data_mv");
    assert!(result.is_some());
}

#[test]
fn test_drop_materialized_view() {
    let catalog = Catalog::new();
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("items".to_string(), columns).unwrap();

    let query = SelectStmt {
        distinct: false,
        columns: vec![Expr::Star],
        from: "items".to_string(),
        table_alias: None,
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    catalog.create_materialized_view("items_mv".to_string(), query).unwrap();
    catalog.drop_materialized_view("items_mv", false).unwrap();
    assert!(catalog.get_materialized_view("items_mv").is_none());
}

#[test]
fn test_drop_materialized_view_if_exists() {
    let catalog = Catalog::new();

    // Should not error when materialized view doesn't exist
    catalog.drop_materialized_view("nonexistent_mv", true).unwrap();
}

#[test]
fn test_drop_materialized_view_not_exists_error() {
    let catalog = Catalog::new();

    // Should error when materialized view doesn't exist and if_exists is false
    let result = catalog.drop_materialized_view("nonexistent_mv", false);
    assert!(result.is_err());
}

#[test]
fn test_create_materialized_view_duplicate_error() {
    let catalog = Catalog::new();
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("orders".to_string(), columns).unwrap();

    let query = SelectStmt {
        distinct: false,
        columns: vec![Expr::Star],
        from: "orders".to_string(),
        table_alias: None,
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    catalog.create_materialized_view("orders_mv".to_string(), query.clone()).unwrap();

    // Should error when creating duplicate materialized view
    let result = catalog.create_materialized_view("orders_mv".to_string(), query);
    assert!(result.is_err());
}

#[test]
fn test_refresh_nonexistent_materialized_view() {
    let catalog = Catalog::new();

    // Should error when refreshing non-existent materialized view
    let result = catalog.refresh_materialized_view("nonexistent_mv");
    assert!(result.is_err());
}
