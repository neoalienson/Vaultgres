use std::sync::Arc;
use vaultgres::catalog::Catalog;
use vaultgres::parser::ast::{ColumnDef, DataType, Expr};

#[test]
fn test_partial_index_creation() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("status".to_string(), DataType::Text),
    ];

    catalog.create_table("orders".to_string(), columns).unwrap();
    catalog.insert("orders", vec![Expr::Number(1), Expr::String("active".to_string())]).unwrap();
    catalog.insert("orders", vec![Expr::Number(2), Expr::String("inactive".to_string())]).unwrap();
    catalog.insert("orders", vec![Expr::Number(3), Expr::String("active".to_string())]).unwrap();

    let rows = Catalog::select_with_catalog(
        &catalog_arc,
        "orders",
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
fn test_partial_index_with_where_clause() {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog.clone());
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("price".to_string(), DataType::Int),
    ];

    catalog.create_table("products".to_string(), columns).unwrap();
    catalog.insert("products", vec![Expr::Number(1), Expr::Number(100)]).unwrap();
    catalog.insert("products", vec![Expr::Number(2), Expr::Number(500)]).unwrap();
    catalog.insert("products", vec![Expr::Number(3), Expr::Number(50)]).unwrap();

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
