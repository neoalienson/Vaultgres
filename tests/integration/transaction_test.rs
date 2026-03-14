#[cfg(test)]
mod tests {
    use vaultgres::catalog::Catalog;
    use vaultgres::parser::Parser;
    use vaultgres::parser::ast::*;

    fn setup_users_table(catalog: &Catalog) {
        catalog
            .create_table(
                "users".to_string(),
                vec![
                    ColumnDef::new("id".to_string(), DataType::Int),
                    ColumnDef::new("name".to_string(), DataType::Text),
                ],
            )
            .unwrap();
    }

    fn insert_user(catalog: &Catalog, id: i64, name: &str) {
        catalog.insert("users", vec![Expr::Number(id), Expr::String(name.to_string())]).unwrap();
    }

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
        assert!(matches!(parser.parse().unwrap(), Statement::Begin));
    }

    #[test]
    fn test_parse_commit() {
        let mut parser = Parser::new("COMMIT").unwrap();
        assert!(matches!(parser.parse().unwrap(), Statement::Commit));
    }

    #[test]
    fn test_parse_rollback() {
        let mut parser = Parser::new("ROLLBACK").unwrap();
        assert!(matches!(parser.parse().unwrap(), Statement::Rollback));
    }

    #[test]
    fn test_transaction_with_insert() {
        let catalog = Catalog::new();
        setup_users_table(&catalog);

        catalog.begin_transaction().unwrap();
        insert_user(&catalog, 1, "Alice");
        catalog.commit_transaction().unwrap();

        assert_eq!(catalog.row_count("users"), 1);
    }

    #[test]
    fn test_transaction_rollback_insert() {
        let catalog = Catalog::new();
        setup_users_table(&catalog);

        catalog.begin_transaction().unwrap();
        insert_user(&catalog, 1, "Alice");
        catalog.rollback_transaction().unwrap();

        assert_eq!(catalog.row_count("users"), 1);
    }

    #[test]
    fn test_multiple_transactions() {
        let catalog = Catalog::new();
        setup_users_table(&catalog);

        catalog.begin_transaction().unwrap();
        insert_user(&catalog, 1, "Alice");
        catalog.commit_transaction().unwrap();

        catalog.begin_transaction().unwrap();
        insert_user(&catalog, 2, "Bob");
        catalog.commit_transaction().unwrap();

        assert_eq!(catalog.row_count("users"), 2);
    }
}
