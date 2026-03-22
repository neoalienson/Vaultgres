use std::sync::Arc;
use vaultgres::catalog::Catalog;
use vaultgres::catalog::Value;
use vaultgres::parser::ast::{ColumnDef, DataType, Expr, SelectStmt};

#[test]
fn test_create_materialized_view() {
    let catalog = Arc::new(Catalog::new());
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
    let catalog = Arc::new(Catalog::new());
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
    let catalog = Arc::new(Catalog::new());
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
    let catalog = Arc::new(Catalog::new());

    // Should not error when materialized view doesn't exist
    catalog.drop_materialized_view("nonexistent_mv", true).unwrap();
}

#[test]
fn test_drop_materialized_view_not_exists_error() {
    let catalog = Arc::new(Catalog::new());

    // Should error when materialized view doesn't exist and if_exists is false
    let result = catalog.drop_materialized_view("nonexistent_mv", false);
    assert!(result.is_err());
}

#[test]
fn test_create_materialized_view_duplicate_error() {
    let catalog = Arc::new(Catalog::new());
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
    let catalog = Arc::new(Catalog::new());

    // Should error when refreshing non-existent materialized view
    let result = catalog.refresh_materialized_view("nonexistent_mv");
    assert!(result.is_err());
}

#[test]
fn test_materialized_view_with_group_by() {
    env_logger::builder().filter_level(log::LevelFilter::Debug).is_test(true).try_init().ok();
    let catalog = Arc::new(Catalog::new());
    let columns = vec![
        ColumnDef::new("category".to_string(), DataType::Text),
        ColumnDef::new("amount".to_string(), DataType::Int),
    ];
    catalog.create_table("sales".to_string(), columns).unwrap();

    catalog.insert("sales", &[], vec![Expr::String("cat1".to_string()), Expr::Number(10)]).unwrap();
    catalog.insert("sales", &[], vec![Expr::String("cat2".to_string()), Expr::Number(20)]).unwrap();
    catalog.insert("sales", &[], vec![Expr::String("cat1".to_string()), Expr::Number(15)]).unwrap();

    let query = vaultgres::parser::Parser::new("CREATE MATERIALIZED VIEW sales_summary AS SELECT s.category, SUM(s.amount) FROM sales s GROUP BY s.category").unwrap().parse().unwrap();
    if let vaultgres::parser::Statement::CreateMaterializedView(create) = query {
        catalog.create_materialized_view(create.name, *create.query).unwrap();
    } else {
        panic!("Expected CreateMaterializedView statement");
    }

    let result = catalog.get_materialized_view("sales_summary").unwrap();
    assert_eq!(result.len(), 2);

    let mut sorted_result = result
        .into_iter()
        .map(|row| (row[0].clone(), row[1].clone()))
        .collect::<Vec<(Value, Value)>>();
    sorted_result.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(sorted_result[0].0, Value::Text("cat1".to_string()));
    assert_eq!(sorted_result[0].1, Value::Int(25));
    assert_eq!(sorted_result[1].0, Value::Text("cat2".to_string()));
    assert_eq!(sorted_result[1].1, Value::Int(20));
}
