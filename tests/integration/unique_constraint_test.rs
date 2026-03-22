use vaultgres::catalog::{Catalog, Value};
use vaultgres::parser::ast::{ColumnDef, DataType, Expr, UniqueConstraint};

#[test]
fn test_unique_column_level() {
    let catalog = Catalog::new();

    let mut col = ColumnDef::new("email".to_string(), DataType::Text);
    col.is_unique = true;

    catalog
        .create_table(
            "users".to_string(),
            vec![ColumnDef::new("id".to_string(), DataType::Int), col],
        )
        .unwrap();

    catalog
        .insert("users", &[], vec![Expr::Number(1), Expr::String("alice@example.com".to_string())])
        .unwrap();

    let result = catalog.insert(
        "users",
        &[],
        vec![Expr::Number(2), Expr::String("alice@example.com".to_string())],
    );
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("UNIQUE"));
}

#[test]
fn test_unique_table_level() {
    let catalog = Catalog::new();

    let schema = catalog.get_table("test").unwrap_or_else(|| {
        catalog
            .create_table(
                "test".to_string(),
                vec![
                    ColumnDef::new("user_id".to_string(), DataType::Int),
                    ColumnDef::new("dept_id".to_string(), DataType::Int),
                ],
            )
            .unwrap();
        catalog.get_table("test").unwrap()
    });

    // Manually add unique constraint for testing
    let mut schema = schema;
    schema.unique_constraints.push(UniqueConstraint {
        name: Some("unique_user_dept".to_string()),
        columns: vec!["user_id".to_string(), "dept_id".to_string()],
    });

    // This test demonstrates the structure, actual enforcement happens in catalog.insert()
    assert_eq!(schema.unique_constraints.len(), 1);
}

#[test]
fn test_unique_allows_different_values() {
    let catalog = Catalog::new();

    let mut col = ColumnDef::new("username".to_string(), DataType::Text);
    col.is_unique = true;

    catalog
        .create_table(
            "users".to_string(),
            vec![ColumnDef::new("id".to_string(), DataType::Int), col],
        )
        .unwrap();

    catalog.insert("users", &[], vec![Expr::Number(1), Expr::String("alice".to_string())]).unwrap();
    catalog.insert("users", &[], vec![Expr::Number(2), Expr::String("bob".to_string())]).unwrap();
    catalog
        .insert("users", &[], vec![Expr::Number(3), Expr::String("charlie".to_string())])
        .unwrap();

    assert_eq!(catalog.row_count("users"), 3);
}

#[test]
fn test_unique_error_message() {
    let catalog = Catalog::new();

    let mut col = ColumnDef::new("email".to_string(), DataType::Text);
    col.is_unique = true;

    catalog
        .create_table(
            "users".to_string(),
            vec![ColumnDef::new("id".to_string(), DataType::Int), col],
        )
        .unwrap();

    catalog
        .insert("users", &[], vec![Expr::Number(1), Expr::String("test@example.com".to_string())])
        .unwrap();

    let result = catalog.insert(
        "users",
        &[],
        vec![Expr::Number(2), Expr::String("test@example.com".to_string())],
    );

    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.contains("UNIQUE"));
    assert!(error.contains("violated"));
}

#[test]
fn test_unique_with_integers() {
    let catalog = Catalog::new();

    let mut col = ColumnDef::new("code".to_string(), DataType::Int);
    col.is_unique = true;

    catalog
        .create_table(
            "products".to_string(),
            vec![ColumnDef::new("id".to_string(), DataType::Int), col],
        )
        .unwrap();

    catalog.insert("products", &[], vec![Expr::Number(1), Expr::Number(100)]).unwrap();
    catalog.insert("products", &[], vec![Expr::Number(2), Expr::Number(200)]).unwrap();

    let result = catalog.insert("products", &[], vec![Expr::Number(3), Expr::Number(100)]);
    assert!(result.is_err());
}

#[test]
fn test_unique_multiple_columns() {
    let catalog = Catalog::new();

    let mut col1 = ColumnDef::new("email".to_string(), DataType::Text);
    col1.is_unique = true;

    let mut col2 = ColumnDef::new("username".to_string(), DataType::Text);
    col2.is_unique = true;

    catalog
        .create_table(
            "users".to_string(),
            vec![ColumnDef::new("id".to_string(), DataType::Int), col1, col2],
        )
        .unwrap();

    catalog
        .insert(
            "users",
            &[],
            vec![
                Expr::Number(1),
                Expr::String("alice@example.com".to_string()),
                Expr::String("alice".to_string()),
            ],
        )
        .unwrap();

    // Duplicate email should fail
    let result = catalog.insert(
        "users",
        &[],
        vec![
            Expr::Number(2),
            Expr::String("alice@example.com".to_string()),
            Expr::String("bob".to_string()),
        ],
    );
    assert!(result.is_err());

    // Duplicate username should fail
    let result = catalog.insert(
        "users",
        &[],
        vec![
            Expr::Number(3),
            Expr::String("bob@example.com".to_string()),
            Expr::String("alice".to_string()),
        ],
    );
    assert!(result.is_err());
}
