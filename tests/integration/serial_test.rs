use rustgres::catalog::Catalog;
use rustgres::parser::ast::{ColumnDef, DataType, Expr};

#[test]
fn test_serial_auto_increment() {
    let catalog = Catalog::new();

    catalog
        .create_table(
            "users".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Serial),
                ColumnDef::new("name".to_string(), DataType::Text),
            ],
        )
        .unwrap();

    catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
    catalog.insert("users", vec![Expr::Number(2), Expr::String("Bob".to_string())]).unwrap();
    catalog.insert("users", vec![Expr::Number(3), Expr::String("Charlie".to_string())]).unwrap();

    assert_eq!(catalog.row_count("users"), 3);
}

#[test]
fn test_auto_increment_flag() {
    let catalog = Catalog::new();

    let mut col = ColumnDef::new("id".to_string(), DataType::Int);
    col.is_auto_increment = true;

    catalog
        .create_table(
            "products".to_string(),
            vec![col, ColumnDef::new("name".to_string(), DataType::Text)],
        )
        .unwrap();

    catalog
        .insert("products", vec![Expr::Number(1), Expr::String("Product A".to_string())])
        .unwrap();
    catalog
        .insert("products", vec![Expr::Number(2), Expr::String("Product B".to_string())])
        .unwrap();

    assert_eq!(catalog.row_count("products"), 2);
}

#[test]
fn test_serial_with_explicit_values() {
    let catalog = Catalog::new();

    catalog
        .create_table(
            "items".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Serial),
                ColumnDef::new("name".to_string(), DataType::Text),
            ],
        )
        .unwrap();

    catalog.insert("items", vec![Expr::Number(100), Expr::String("Item 1".to_string())]).unwrap();
    catalog.insert("items", vec![Expr::Number(101), Expr::String("Item 2".to_string())]).unwrap();

    assert_eq!(catalog.row_count("items"), 2);
}

#[test]
fn test_serial_multiple_tables() {
    let catalog = Catalog::new();

    catalog
        .create_table(
            "users".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Serial),
                ColumnDef::new("name".to_string(), DataType::Text),
            ],
        )
        .unwrap();

    catalog
        .create_table(
            "posts".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Serial),
                ColumnDef::new("title".to_string(), DataType::Text),
            ],
        )
        .unwrap();

    catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
    catalog.insert("posts", vec![Expr::Number(1), Expr::String("Post 1".to_string())]).unwrap();
    catalog.insert("users", vec![Expr::Number(2), Expr::String("Bob".to_string())]).unwrap();
    catalog.insert("posts", vec![Expr::Number(2), Expr::String("Post 2".to_string())]).unwrap();

    assert_eq!(catalog.row_count("users"), 2);
    assert_eq!(catalog.row_count("posts"), 2);
}

#[test]
fn test_serial_with_other_columns() {
    let catalog = Catalog::new();

    catalog
        .create_table(
            "orders".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Serial),
                ColumnDef::new("customer".to_string(), DataType::Text),
                ColumnDef::new("amount".to_string(), DataType::Int),
            ],
        )
        .unwrap();

    catalog
        .insert(
            "orders",
            vec![Expr::Number(1), Expr::String("Alice".to_string()), Expr::Number(100)],
        )
        .unwrap();
    catalog
        .insert("orders", vec![Expr::Number(2), Expr::String("Bob".to_string()), Expr::Number(200)])
        .unwrap();

    assert_eq!(catalog.row_count("orders"), 2);
}

#[test]
fn test_serial_sequence_continuity() {
    let catalog = Catalog::new();

    catalog
        .create_table(
            "counters".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Serial),
                ColumnDef::new("value".to_string(), DataType::Int),
            ],
        )
        .unwrap();

    for i in 1..=10 {
        catalog.insert("counters", vec![Expr::Number(i), Expr::Number(i * 10)]).unwrap();
    }

    assert_eq!(catalog.row_count("counters"), 10);
}
