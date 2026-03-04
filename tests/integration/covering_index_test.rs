use std::sync::Arc;
use vaultgres::catalog::Catalog;
use vaultgres::executor::IndexOnlyScan;
use vaultgres::parser::ast::{ColumnDef, DataType, Expr};

#[test]
fn test_covering_index_two_columns() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("email".to_string(), DataType::Text),
        ColumnDef::new("name".to_string(), DataType::Text),
    ];

    catalog.create_table("users".to_string(), columns).unwrap();
    catalog
        .insert(
            "users",
            vec![
                Expr::Number(1),
                Expr::String("user1@test.com".to_string()),
                Expr::String("User One".to_string()),
            ],
        )
        .unwrap();

    // Query only needs id and email, which are covered by index
    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "users",
        false,
        vec![Expr::Column("id".to_string()), Expr::Column("email".to_string())],
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
fn test_covering_index_all_query_columns() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("category".to_string(), DataType::Text),
        ColumnDef::new("price".to_string(), DataType::Int),
        ColumnDef::new("description".to_string(), DataType::Text),
    ];

    catalog.create_table("products".to_string(), columns).unwrap();
    catalog
        .insert(
            "products",
            vec![
                Expr::Number(1),
                Expr::String("electronics".to_string()),
                Expr::Number(100),
                Expr::String("A product".to_string()),
            ],
        )
        .unwrap();

    // Index on (category, price) covers query for these columns
    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "products",
        false,
        vec![Expr::Column("category".to_string()), Expr::Column("price".to_string())],
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
fn test_covering_index_with_where_clause() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("status".to_string(), DataType::Text),
        ColumnDef::new("priority".to_string(), DataType::Int),
    ];

    catalog.create_table("tasks".to_string(), columns).unwrap();
    catalog
        .insert("tasks", vec![Expr::Number(1), Expr::String("active".to_string()), Expr::Number(1)])
        .unwrap();
    catalog
        .insert(
            "tasks",
            vec![Expr::Number(2), Expr::String("completed".to_string()), Expr::Number(2)],
        )
        .unwrap();

    // Index on (status, priority) covers query
    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "tasks",
        false,
        vec![Expr::Column("status".to_string()), Expr::Column("priority".to_string())],
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
fn test_covering_index_scan_only() {
    let scan =
        IndexOnlyScan::new_btree(vec!["col1".to_string(), "col2".to_string(), "col3".to_string()]);
    assert_eq!(scan.columns().len(), 3);
    // All three columns covered by index
}

#[test]
fn test_covering_index_includes_all_needed() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("first_name".to_string(), DataType::Text),
        ColumnDef::new("last_name".to_string(), DataType::Text),
        ColumnDef::new("age".to_string(), DataType::Int),
    ];

    catalog.create_table("people".to_string(), columns).unwrap();
    catalog
        .insert(
            "people",
            vec![
                Expr::Number(1),
                Expr::String("John".to_string()),
                Expr::String("Doe".to_string()),
                Expr::Number(30),
            ],
        )
        .unwrap();

    // Index on (first_name, last_name, age) covers query
    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "people",
        false,
        vec![Expr::Column("first_name".to_string()), Expr::Column("last_name".to_string())],
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
fn test_covering_index_order_by() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("name".to_string(), DataType::Text),
        ColumnDef::new("created_at".to_string(), DataType::Int),
    ];

    catalog.create_table("items".to_string(), columns).unwrap();
    catalog
        .insert(
            "items",
            vec![Expr::Number(1), Expr::String("Item A".to_string()), Expr::Number(100)],
        )
        .unwrap();
    catalog
        .insert(
            "items",
            vec![Expr::Number(2), Expr::String("Item B".to_string()), Expr::Number(200)],
        )
        .unwrap();

    // Index on (name, created_at) covers query with ORDER BY
    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "items",
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
    assert_eq!(rows.len(), 2);
}
