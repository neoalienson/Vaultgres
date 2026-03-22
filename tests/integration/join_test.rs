use vaultgres::catalog::Catalog;
use vaultgres::parser::ast::{ColumnDef, DataType, Expr};
use vaultgres::parser::{Parser, Statement};

#[test]
fn test_join_execution() {
    let catalog = Catalog::new();

    // Create customers table
    catalog
        .create_table(
            "customers".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("name".to_string(), DataType::Text),
            ],
        )
        .unwrap();

    // Create orders table
    catalog
        .create_table(
            "orders".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("customer_id".to_string(), DataType::Int),
                ColumnDef::new("total".to_string(), DataType::Int),
            ],
        )
        .unwrap();

    // Insert data
    catalog.insert("customers", &[], vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
    catalog.insert("orders", &[], vec![Expr::Number(1), Expr::Number(1), Expr::Number(100)]).unwrap();

    // Parse JOIN query
    let sql = "SELECT c.name, o.total FROM customers c INNER JOIN orders o ON c.id = o.customer_id";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    eprintln!("Parsed statement: {:#?}", stmt);

    if let Statement::Select(select) = stmt {
        eprintln!("Joins: {:?}", select.joins);
        eprintln!("From: {}", select.from);
        eprintln!("Table alias: {:?}", select.table_alias);
        assert!(!select.joins.is_empty(), "JOIN should be parsed");
        assert_eq!(select.joins.len(), 1);
        assert_eq!(select.from, "customers");
        assert_eq!(select.table_alias, Some("c".to_string()));
        assert_eq!(select.joins[0].table, "orders");
        assert_eq!(select.joins[0].alias, Some("o".to_string()));
    } else {
        panic!("Expected SELECT statement");
    }
}
