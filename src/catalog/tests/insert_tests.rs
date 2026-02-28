use crate::catalog::*;
use crate::parser::ast::{ColumnDef, DataType, Expr};

#[test]
fn test_insert() {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ColumnDef { name: "name".to_string(), data_type: DataType::Text },
    ];
    
    catalog.create_table("users".to_string(), columns).unwrap();
    
    let values = vec![Expr::Number(1), Expr::String("Alice".to_string())];
    assert!(catalog.insert("users", values).is_ok());
    assert_eq!(catalog.row_count("users"), 1);
}

#[test]
fn test_insert_wrong_column_count() {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef { name: "id".to_string(), data_type: DataType::Int },
    ];
    
    catalog.create_table("users".to_string(), columns).unwrap();
    
    let values = vec![Expr::Number(1), Expr::String("Alice".to_string())];
    assert!(catalog.insert("users", values).is_err());
}

#[test]
fn test_insert_type_mismatch() {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef { name: "id".to_string(), data_type: DataType::Int },
    ];
    
    catalog.create_table("users".to_string(), columns).unwrap();
    
    let values = vec![Expr::String("not a number".to_string())];
    assert!(catalog.insert("users", values).is_err());
}

#[test]
fn test_insert_multiple_rows() {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ColumnDef { name: "name".to_string(), data_type: DataType::Text },
    ];
    
    catalog.create_table("users".to_string(), columns).unwrap();
    
    catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
    catalog.insert("users", vec![Expr::Number(2), Expr::String("Bob".to_string())]).unwrap();
    catalog.insert("users", vec![Expr::Number(3), Expr::String("Charlie".to_string())]).unwrap();
    
    assert_eq!(catalog.row_count("users"), 3);
}
