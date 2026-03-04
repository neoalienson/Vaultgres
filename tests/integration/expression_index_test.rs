use std::sync::Arc;
use vaultgres::catalog::Catalog;
use vaultgres::parser::ast::{ColumnDef, DataType, Expr};

#[test]
fn test_expression_index_creation() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("email".to_string(), DataType::Text),
    ];

    catalog.create_table("users".to_string(), columns).unwrap();
    catalog
        .insert("users", vec![Expr::Number(1), Expr::String("USER@EXAMPLE.COM".to_string())])
        .unwrap();
    catalog
        .insert("users", vec![Expr::Number(2), Expr::String("admin@test.com".to_string())])
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
fn test_expression_index_with_function() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("name".to_string(), DataType::Text),
    ];

    catalog.create_table("products".to_string(), columns).unwrap();
    catalog.insert("products", vec![Expr::Number(1), Expr::String("Widget".to_string())]).unwrap();
    catalog.insert("products", vec![Expr::Number(2), Expr::String("Gadget".to_string())]).unwrap();

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
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_expression_index_multiple_expressions() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("first_name".to_string(), DataType::Text),
        ColumnDef::new("last_name".to_string(), DataType::Text),
    ];

    catalog.create_table("people".to_string(), columns).unwrap();
    catalog
        .insert(
            "people",
            vec![
                Expr::Number(1),
                Expr::String("John".to_string()),
                Expr::String("Doe".to_string()),
            ],
        )
        .unwrap();

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "people",
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
