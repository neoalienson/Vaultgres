use vaultgres::catalog::Catalog;
use vaultgres::parser::ast::{ColumnDef, DataType, Expr};

#[test]
fn test_default_value_integer() {
    let catalog = Catalog::new();

    let mut col = ColumnDef::new("status".to_string(), DataType::Int);
    col.default_value = Some(Expr::Number(0));

    catalog
        .create_table(
            "users".to_string(),
            vec![ColumnDef::new("id".to_string(), DataType::Int), col],
        )
        .unwrap();

    catalog.insert("users", &[], vec![Expr::Number(1), Expr::Number(0)]).unwrap();
    assert_eq!(catalog.row_count("users"), 1);
}

#[test]
fn test_default_value_text() {
    let catalog = Catalog::new();

    let mut col = ColumnDef::new("role".to_string(), DataType::Text);
    col.default_value = Some(Expr::String("user".to_string()));

    catalog
        .create_table(
            "accounts".to_string(),
            vec![ColumnDef::new("id".to_string(), DataType::Int), col],
        )
        .unwrap();

    catalog.insert("accounts", &[], vec![Expr::Number(1), Expr::String("user".to_string())]).unwrap();
    assert_eq!(catalog.row_count("accounts"), 1);
}

#[test]
fn test_default_value_multiple_columns() {
    let catalog = Catalog::new();

    let mut col1 = ColumnDef::new("status".to_string(), DataType::Int);
    col1.default_value = Some(Expr::Number(1));

    let mut col2 = ColumnDef::new("role".to_string(), DataType::Text);
    col2.default_value = Some(Expr::String("guest".to_string()));

    catalog
        .create_table(
            "users".to_string(),
            vec![ColumnDef::new("id".to_string(), DataType::Int), col1, col2],
        )
        .unwrap();

    catalog
        .insert("users", &[], vec![Expr::Number(1), Expr::Number(1), Expr::String("guest".to_string())])
        .unwrap();
    assert_eq!(catalog.row_count("users"), 1);
}

#[test]
fn test_default_value_missing_no_default() {
    let catalog = Catalog::new();

    catalog
        .create_table(
            "users".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("name".to_string(), DataType::Text),
            ],
        )
        .unwrap();

    let result = catalog.insert("users", &[], vec![Expr::Number(1)]);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("default") || err.contains("Expected"));
}

#[test]
fn test_default_value_zero() {
    let catalog = Catalog::new();

    let mut col = ColumnDef::new("count".to_string(), DataType::Int);
    col.default_value = Some(Expr::Number(0));

    catalog
        .create_table(
            "counters".to_string(),
            vec![ColumnDef::new("id".to_string(), DataType::Int), col],
        )
        .unwrap();

    catalog.insert("counters", &[], vec![Expr::Number(1), Expr::Number(0)]).unwrap();
    assert_eq!(catalog.row_count("counters"), 1);
}

#[test]
fn test_default_value_empty_string() {
    let catalog = Catalog::new();

    let mut col = ColumnDef::new("description".to_string(), DataType::Text);
    col.default_value = Some(Expr::String("".to_string()));

    catalog
        .create_table(
            "items".to_string(),
            vec![ColumnDef::new("id".to_string(), DataType::Int), col],
        )
        .unwrap();

    catalog.insert("items", &[], vec![Expr::Number(1), Expr::String("".to_string())]).unwrap();
    assert_eq!(catalog.row_count("items"), 1);
}
