use std::sync::Arc;
use vaultgres::catalog::{Catalog, Value};
use vaultgres::parser::ast::{ColumnDef, DataType, Expr};

fn setup_catalog_with_arrays() -> Catalog {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("name".to_string(), DataType::Text),
        ColumnDef::new("scores".to_string(), DataType::Array(Box::new(DataType::Int))),
    ];
    catalog.create_table("students".to_string(), columns).unwrap();
    catalog
}

#[test]
fn test_insert_array_column() {
    let catalog = setup_catalog_with_arrays();
    let _catalog_arc = Arc::new(catalog.clone());

    let values = vec![
        Expr::Number(1),
        Expr::String("Alice".to_string()),
        Expr::Array(vec![Expr::Number(85), Expr::Number(90), Expr::Number(78)]),
    ];
    assert!(catalog.insert("students", &[], values).is_ok());
    assert_eq!(catalog.row_count("students"), 1);
}

#[test]
fn test_insert_empty_array() {
    let catalog = setup_catalog_with_arrays();
    let _catalog_arc = Arc::new(catalog.clone());

    let values = vec![Expr::Number(2), Expr::String("Bob".to_string()), Expr::Array(vec![])];
    assert!(catalog.insert("students", &[], values).is_ok());
    assert_eq!(catalog.row_count("students"), 1);
}

#[test]
fn test_insert_multiple_rows_with_arrays() {
    let catalog = setup_catalog_with_arrays();
    let _catalog_arc = Arc::new(catalog.clone());

    let values1 = vec![
        Expr::Number(1),
        Expr::String("Alice".to_string()),
        Expr::Array(vec![Expr::Number(85), Expr::Number(90)]),
    ];
    catalog.insert("students", &[], values1).unwrap();

    let values2 = vec![
        Expr::Number(2),
        Expr::String("Bob".to_string()),
        Expr::Array(vec![Expr::Number(70), Expr::Number(75), Expr::Number(80)]),
    ];
    catalog.insert("students", &[], values2).unwrap();

    assert_eq!(catalog.row_count("students"), 2);
}

#[test]
fn test_insert_array_with_nulls() {
    let catalog = setup_catalog_with_arrays();
    let _catalog_arc = Arc::new(catalog.clone());

    let values = vec![
        Expr::Number(1),
        Expr::String("Charlie".to_string()),
        Expr::Array(vec![Expr::Number(85), Expr::Null, Expr::Number(90)]),
    ];
    assert!(catalog.insert("students", &[], values).is_ok());
    assert_eq!(catalog.row_count("students"), 1);
}

#[test]
fn test_nested_array_types() {
    let catalog = Catalog::new();
    let _catalog_arc = Arc::new(catalog.clone());

    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new(
            "matrix".to_string(),
            DataType::Array(Box::new(DataType::Array(Box::new(DataType::Int)))),
        ),
    ];

    let result = catalog.create_table("matrix_table".to_string(), columns);
    assert!(result.is_ok());

    let values = vec![
        Expr::Number(1),
        Expr::Array(vec![
            Expr::Array(vec![Expr::Number(1), Expr::Number(2)]),
            Expr::Array(vec![Expr::Number(3), Expr::Number(4)]),
        ]),
    ];
    assert!(catalog.insert("matrix_table", &[], values).is_ok());
}

#[test]
fn test_array_column_requires_array_value() {
    let catalog = setup_catalog_with_arrays();
    let _catalog_arc = Arc::new(catalog.clone());

    let values = vec![Expr::Number(1), Expr::String("Alice".to_string()), Expr::Number(100)];
    assert!(catalog.insert("students", &[], values).is_err());
}

#[test]
fn test_update_array_column() {
    let catalog = setup_catalog_with_arrays();
    let _catalog_arc = Arc::new(catalog.clone());

    let values = vec![
        Expr::Number(1),
        Expr::String("Alice".to_string()),
        Expr::Array(vec![Expr::Number(85), Expr::Number(90)]),
    ];
    catalog.insert("students", &[], values).unwrap();

    let assignment =
        (String::from("scores"), Expr::Array(vec![Expr::Number(95), Expr::Number(100)]));
    let where_clause = Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: vaultgres::parser::ast::BinaryOperator::Equals,
        right: Box::new(Expr::Number(1)),
    };

    let result = catalog.update("students", vec![assignment], Some(where_clause));
    assert!(result.is_ok());
    assert_eq!(catalog.row_count("students"), 1);
}

#[test]
fn test_delete_with_array_contains() {
    use vaultgres::parser::ast::BinaryOperator;

    let catalog = setup_catalog_with_arrays();
    let _catalog_arc = Arc::new(catalog.clone());

    let values1 = vec![
        Expr::Number(1),
        Expr::String("Alice".to_string()),
        Expr::Array(vec![Expr::Number(85), Expr::Number(90), Expr::Number(78)]),
    ];
    catalog.insert("students", &[], values1).unwrap();

    let values2 = vec![
        Expr::Number(2),
        Expr::String("Bob".to_string()),
        Expr::Array(vec![Expr::Number(70), Expr::Number(75)]),
    ];
    catalog.insert("students", &[], values2).unwrap();

    assert_eq!(catalog.row_count("students"), 2);

    let where_clause = Expr::BinaryOp {
        left: Box::new(Expr::Column("scores".to_string())),
        op: BinaryOperator::ArrayContains,
        right: Box::new(Expr::Number(90)),
    };

    let result = catalog.delete("students", Some(where_clause));
    assert!(result.is_ok());
}

#[test]
fn test_persistence_with_array_column() {
    use std::fs;

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_arrays.db");

    {
        let catalog = setup_catalog_with_arrays();

        let values = vec![
            Expr::Number(1),
            Expr::String("Alice".to_string()),
            Expr::Array(vec![Expr::Number(85), Expr::Number(90), Expr::Number(78)]),
        ];
        catalog.insert("students", &[], values).unwrap();

        let schema = catalog.get_table("students").unwrap();
        assert!(matches!(schema.columns[2].data_type, DataType::Array(_)));
    }
}
