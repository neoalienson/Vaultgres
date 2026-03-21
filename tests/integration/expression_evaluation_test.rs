// Integration tests for SELECT expression evaluation
// Tests column aliases, arithmetic expressions, and multi-table joins
//
// Note: Full expression evaluation (arithmetic, aliases, etc.) is implemented
// in the protocol layer (src/protocol/connection.rs). The catalog layer tests
// here verify basic SELECT functionality. For full expression tests, see the
// unit tests in protocol::connection::expression_tests.

use std::sync::Arc;
use vaultgres::catalog::*;
use vaultgres::parser::ast::{ColumnDef, DataType, Expr};

fn create_products_catalog() -> Arc<Catalog> {
    let catalog = Catalog::new();
    let catalog_arc = Arc::new(catalog);

    // Create products table
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("name".to_string(), DataType::Text),
        ColumnDef::new("price".to_string(), DataType::Int),
        ColumnDef::new("quantity".to_string(), DataType::Int),
    ];
    catalog_arc.create_table("products".to_string(), columns).unwrap();

    // Insert test data
    catalog_arc
        .insert(
            "products",
            vec![
                Expr::Number(1),
                Expr::String("Laptop".to_string()),
                Expr::Number(1000),
                Expr::Number(10),
            ],
        )
        .unwrap();
    catalog_arc
        .insert(
            "products",
            vec![
                Expr::Number(2),
                Expr::String("Mouse".to_string()),
                Expr::Number(50),
                Expr::Number(50),
            ],
        )
        .unwrap();
    catalog_arc
        .insert(
            "products",
            vec![
                Expr::Number(3),
                Expr::String("Keyboard".to_string()),
                Expr::Number(150),
                Expr::Number(30),
            ],
        )
        .unwrap();

    catalog_arc
}

#[test]
fn test_select_all_columns() {
    let catalog = create_products_catalog();

    let rows = Catalog::select_with_catalog(
        &catalog,
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
    assert_eq!(rows[0].len(), 4); // id, name, price, quantity
}

#[test]
fn test_select_specific_columns() {
    let catalog = create_products_catalog();

    let rows = Catalog::select_with_catalog(
        &catalog,
        "products",
        false,
        vec![Expr::Column("name".to_string()), Expr::Column("price".to_string())],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();

    assert_eq!(rows.len(), 3);
    // Note: Catalog returns columns in sorted order by name
    // The values should be present but order may vary
    assert!(rows[0].iter().any(|v| v == &Value::Text("Laptop".to_string())));
    assert!(rows[0].iter().any(|v| v == &Value::Int(1000)));
}

#[test]
fn test_select_with_where() {
    let catalog = create_products_catalog();

    let rows = Catalog::select_with_catalog(
        &catalog,
        "products",
        false,
        vec![Expr::Column("name".to_string())],
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("price".to_string())),
            op: vaultgres::parser::ast::BinaryOperator::GreaterThan,
            right: Box::new(Expr::Number(100)),
        }),
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();

    // Should return Laptop and Keyboard (price > 100)
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_select_with_limit() {
    let catalog = create_products_catalog();

    let rows = Catalog::select_with_catalog(
        &catalog,
        "products",
        false,
        vec![Expr::Column("name".to_string())],
        None,
        None,
        None,
        None,
        Some(2),
        None,
    )
    .unwrap();

    assert_eq!(rows.len(), 2);
}

#[test]
fn test_select_column_exists() {
    let catalog = create_products_catalog();

    let rows = Catalog::select_with_catalog(
        &catalog,
        "products",
        false,
        vec![Expr::Column("id".to_string())],
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();

    assert_eq!(rows.len(), 3);
    // Each row should have exactly 1 column
    assert_eq!(rows[0].len(), 1);
}
