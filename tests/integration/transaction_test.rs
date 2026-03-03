#[cfg(test)]
mod tests {
    use vaultgres::catalog::Catalog;
    use vaultgres::parser::ast::*;
    use vaultgres::parser::Parser;

    #[test]
    fn test_begin_transaction() {
        let catalog = Catalog::new();
        catalog.begin_transaction().unwrap();
    }

    #[test]
    fn test_commit_transaction() {
        let catalog = Catalog::new();
        catalog.begin_transaction().unwrap();
        catalog.commit_transaction().unwrap();
    }

    #[test]
    fn test_rollback_transaction() {
        let catalog = Catalog::new();
        catalog.begin_transaction().unwrap();
        catalog.rollback_transaction().unwrap();
    }

    #[test]
    fn test_commit_without_begin() {
        let catalog = Catalog::new();
        let result = catalog.commit_transaction();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "No active transaction");
    }

    #[test]
    fn test_rollback_without_begin() {
        let catalog = Catalog::new();
        let result = catalog.rollback_transaction();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "No active transaction");
    }

    #[test]
    fn test_nested_begin() {
        let catalog = Catalog::new();
        catalog.begin_transaction().unwrap();
        let result = catalog.begin_transaction();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Transaction already in progress");
    }

    #[test]
    fn test_parse_begin() {
        let mut parser = Parser::new("BEGIN").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::Begin));
    }

    #[test]
    fn test_parse_commit() {
        let mut parser = Parser::new("COMMIT").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::Commit));
    }

    #[test]
    fn test_parse_rollback() {
        let mut parser = Parser::new("ROLLBACK").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::Rollback));
    }

    #[test]
    fn test_transaction_with_insert() {
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

        catalog.begin_transaction().unwrap();
        catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
        catalog.commit_transaction().unwrap();

        assert_eq!(catalog.row_count("users"), 1);
    }

    #[test]
    fn test_transaction_rollback_insert() {
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

        catalog.begin_transaction().unwrap();
        catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
        catalog.rollback_transaction().unwrap();

        assert_eq!(catalog.row_count("users"), 1);
    }

    #[test]
    fn test_multiple_transactions() {
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

        catalog.begin_transaction().unwrap();
        catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
        catalog.commit_transaction().unwrap();

        catalog.begin_transaction().unwrap();
        catalog.insert("users", vec![Expr::Number(2), Expr::String("Bob".to_string())]).unwrap();
        catalog.commit_transaction().unwrap();

        assert_eq!(catalog.row_count("users"), 2);
    }
}
