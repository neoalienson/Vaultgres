use std::sync::Arc;
use vaultgres::catalog::Catalog;
use vaultgres::parser::ast::{ColumnDef, DataType, Expr};

#[test]
fn test_multi_column_index_two_columns() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("first_name".to_string(), DataType::Text),
        ColumnDef::new("last_name".to_string(), DataType::Text),
    ];

    catalog.create_table("users".to_string(), columns).unwrap();
    catalog
        .insert(
            "users",
            vec![
                Expr::Number(1),
                Expr::String("John".to_string()),
                Expr::String("Doe".to_string()),
            ],
        )
        .unwrap();
    catalog
        .insert(
            "users",
            vec![
                Expr::Number(2),
                Expr::String("Jane".to_string()),
                Expr::String("Doe".to_string()),
            ],
        )
        .unwrap();

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
}

#[test]
fn test_multi_column_index_three_columns() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("country".to_string(), DataType::Text),
        ColumnDef::new("state".to_string(), DataType::Text),
        ColumnDef::new("city".to_string(), DataType::Text),
    ];

    catalog.create_table("locations".to_string(), columns).unwrap();
    catalog
        .insert(
            "locations",
            vec![
                Expr::Number(1),
                Expr::String("USA".to_string()),
                Expr::String("CA".to_string()),
                Expr::String("SF".to_string()),
            ],
        )
        .unwrap();

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "locations",
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
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_multi_column_index_ordering() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("category".to_string(), DataType::Text),
        ColumnDef::new("price".to_string(), DataType::Int),
    ];

    catalog.create_table("products".to_string(), columns).unwrap();
    catalog
        .insert(
            "products",
            vec![Expr::Number(1), Expr::String("electronics".to_string()), Expr::Number(100)],
        )
        .unwrap();
    catalog
        .insert(
            "products",
            vec![Expr::Number(2), Expr::String("electronics".to_string()), Expr::Number(200)],
        )
        .unwrap();
    catalog
        .insert(
            "products",
            vec![Expr::Number(3), Expr::String("books".to_string()), Expr::Number(50)],
        )
        .unwrap();

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "products",
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
    assert_eq!(rows.len(), 3);
}

#[test]
fn test_multi_column_unique_index() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("email".to_string(), DataType::Text),
        ColumnDef::new("username".to_string(), DataType::Text),
    ];

    catalog.create_table("accounts".to_string(), columns).unwrap();
    catalog
        .insert(
            "accounts",
            vec![
                Expr::Number(1),
                Expr::String("user1@test.com".to_string()),
                Expr::String("user1".to_string()),
            ],
        )
        .unwrap();

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "accounts",
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
    assert_eq!(rows.len(), 1);
}
