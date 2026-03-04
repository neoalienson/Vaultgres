use vaultgres::catalog::Catalog;
use vaultgres::executor::IndexOnlyScan;
use vaultgres::parser::ast::{ColumnDef, DataType, Expr};

#[test]
fn test_index_only_scan_creation() {
    let mut scan = IndexOnlyScan::new_btree(vec!["id".to_string()]);
}

#[test]
fn test_index_only_scan_with_catalog() {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("email".to_string(), DataType::Text),
    ];

    catalog.create_table("users".to_string(), columns).unwrap();
    catalog
        .insert("users", vec![Expr::Number(1), Expr::String("test@example.com".to_string())])
        .unwrap();

    let rows = catalog
        .select(
            "users",
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
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_index_only_scan_multiple_columns() {
    let scan = IndexOnlyScan::new_btree(vec!["col1".to_string(), "col2".to_string()]);
    assert_eq!(scan.columns(), &["col1", "col2"]);
}

#[test]
fn test_index_only_scan_hash_index() {
    let scan = IndexOnlyScan::new_hash(vec!["id".to_string()]);
    assert_eq!(scan.columns().len(), 1);
}
#[test]
fn test_index_only_scan_covering_index() {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("name".to_string(), DataType::Text),
        ColumnDef::new("email".to_string(), DataType::Text),
    ];

    catalog.create_table("accounts".to_string(), columns).unwrap();
    catalog
        .insert(
            "accounts",
            vec![
                Expr::Number(1),
                Expr::String("Alice".to_string()),
                Expr::String("alice@test.com".to_string()),
            ],
        )
        .unwrap();

    let rows = catalog
        .select(
            "accounts",
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
