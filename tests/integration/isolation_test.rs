#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use vaultgres::catalog::Catalog;
    use vaultgres::parser::Parser;
    use vaultgres::parser::ast::*;

    #[test]
    fn test_parse_set_transaction_read_committed() {
        let mut parser = Parser::new("SET TRANSACTION ISOLATION LEVEL READ COMMITTED").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::SetTransaction(IsolationLevel::ReadCommitted)));
    }

    #[test]
    fn test_parse_set_transaction_repeatable_read() {
        let mut parser = Parser::new("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::SetTransaction(IsolationLevel::RepeatableRead)));
    }

    #[test]
    fn test_parse_set_transaction_serializable() {
        let mut parser = Parser::new("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::SetTransaction(IsolationLevel::Serializable)));
    }

    #[test]
    fn test_begin_with_read_committed() {
        let catalog = Catalog::new();
        let catalog_arc = Arc::new(catalog.clone());
        catalog.begin_transaction_with_isolation(IsolationLevel::ReadCommitted.into()).unwrap();
        catalog.commit_transaction().unwrap();
    }

    #[test]
    fn test_begin_with_repeatable_read() {
        let catalog = Catalog::new();
        let catalog_arc = Arc::new(catalog.clone());
        catalog.begin_transaction_with_isolation(IsolationLevel::RepeatableRead.into()).unwrap();
        catalog.commit_transaction().unwrap();
    }

    #[test]
    fn test_begin_with_serializable() {
        let catalog = Catalog::new();
        let catalog_arc = Arc::new(catalog.clone());
        catalog.begin_transaction_with_isolation(IsolationLevel::Serializable.into()).unwrap();
        catalog.commit_transaction().unwrap();
    }

    #[test]
    fn test_set_isolation_level() {
        let catalog = Catalog::new();
        let catalog_arc = Arc::new(catalog.clone());
        catalog.begin_transaction().unwrap();
        catalog.set_transaction_isolation(IsolationLevel::Serializable.into()).unwrap();
        catalog.commit_transaction().unwrap();
    }

    #[test]
    fn test_set_isolation_without_transaction() {
        let catalog = Catalog::new();
        let catalog_arc = Arc::new(catalog.clone());
        let result = catalog.set_transaction_isolation(IsolationLevel::Serializable.into());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "No active transaction");
    }

    #[test]
    fn test_read_committed_sees_committed_data() {
        let catalog = Catalog::new();
        let catalog_arc = Arc::new(catalog.clone());
        catalog
            .create_table(
                "users".to_string(),
                vec![
                    ColumnDef::new("id".to_string(), DataType::Int),
                    ColumnDef::new("name".to_string(), DataType::Text),
                ],
            )
            .unwrap();

        catalog.begin_transaction_with_isolation(IsolationLevel::ReadCommitted.into()).unwrap();
        catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
        catalog.commit_transaction().unwrap();

        catalog.begin_transaction_with_isolation(IsolationLevel::ReadCommitted.into()).unwrap();
        let result = Catalog::select_with_catalog(
            &catalog_arc,
            "users",
            false,
            vec![Expr::Star],
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
        catalog.commit_transaction().unwrap();
    }

    #[test]
    fn test_repeatable_read_isolation() {
        let catalog = Catalog::new();
        let catalog_arc = Arc::new(catalog.clone());
        catalog
            .create_table(
                "users".to_string(),
                vec![
                    ColumnDef::new("id".to_string(), DataType::Int),
                    ColumnDef::new("name".to_string(), DataType::Text),
                ],
            )
            .unwrap();

        catalog.begin_transaction_with_isolation(IsolationLevel::RepeatableRead.into()).unwrap();
        catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
        catalog.commit_transaction().unwrap();

        assert_eq!(catalog.row_count("users"), 1);
    }

    #[test]
    fn test_serializable_isolation() {
        let catalog = Catalog::new();
        let catalog_arc = Arc::new(catalog.clone());
        catalog
            .create_table(
                "users".to_string(),
                vec![
                    ColumnDef::new("id".to_string(), DataType::Int),
                    ColumnDef::new("name".to_string(), DataType::Text),
                ],
            )
            .unwrap();

        catalog.begin_transaction_with_isolation(IsolationLevel::Serializable.into()).unwrap();
        catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
        catalog.insert("users", vec![Expr::Number(2), Expr::String("Bob".to_string())]).unwrap();
        catalog.commit_transaction().unwrap();

        assert_eq!(catalog.row_count("users"), 2);
    }
}
