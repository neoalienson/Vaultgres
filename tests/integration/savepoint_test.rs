#[cfg(test)]
mod tests {
    use vaultgres::catalog::Catalog;
    use vaultgres::parser::Parser;
    use vaultgres::parser::ast::*;

    #[test]
    fn test_parse_savepoint() {
        let mut parser = Parser::new("SAVEPOINT sp1").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::Savepoint(name) if name == "sp1"));
    }

    #[test]
    fn test_parse_rollback_to() {
        let mut parser = Parser::new("ROLLBACK TO sp1").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::RollbackTo(name) if name == "sp1"));
    }

    #[test]
    fn test_parse_rollback_to_savepoint() {
        let mut parser = Parser::new("ROLLBACK TO SAVEPOINT sp1").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::RollbackTo(name) if name == "sp1"));
    }

    #[test]
    fn test_parse_release_savepoint() {
        let mut parser = Parser::new("RELEASE SAVEPOINT sp1").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::ReleaseSavepoint(name) if name == "sp1"));
    }

    #[test]
    fn test_parse_release() {
        let mut parser = Parser::new("RELEASE sp1").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::ReleaseSavepoint(name) if name == "sp1"));
    }

    #[test]
    fn test_savepoint_without_transaction() {
        let catalog = Catalog::new();
        let result = catalog.savepoint("sp1".to_string());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "No active transaction");
    }

    #[test]
    fn test_savepoint_basic() {
        let catalog = Catalog::new();
        catalog.begin_transaction().unwrap();
        catalog.savepoint("sp1".to_string()).unwrap();
        catalog.commit_transaction().unwrap();
    }

    #[test]
    fn test_rollback_to_savepoint() {
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
        catalog.savepoint("sp1".to_string()).unwrap();
        catalog.insert("users", vec![Expr::Number(2), Expr::String("Bob".to_string())]).unwrap();
        catalog.rollback_to_savepoint("sp1").unwrap();
        catalog.commit_transaction().unwrap();

        assert_eq!(catalog.row_count("users"), 1);
    }

    #[test]
    fn test_release_savepoint() {
        let catalog = Catalog::new();
        catalog.begin_transaction().unwrap();
        catalog.savepoint("sp1".to_string()).unwrap();
        catalog.release_savepoint("sp1").unwrap();
        catalog.commit_transaction().unwrap();
    }

    #[test]
    fn test_release_nonexistent_savepoint() {
        let catalog = Catalog::new();
        catalog.begin_transaction().unwrap();
        let result = catalog.release_savepoint("sp1");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Savepoint does not exist");
    }

    #[test]
    fn test_rollback_to_nonexistent_savepoint() {
        let catalog = Catalog::new();
        catalog.begin_transaction().unwrap();
        let result = catalog.rollback_to_savepoint("sp1");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Savepoint does not exist");
    }

    #[test]
    fn test_multiple_savepoints() {
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
        catalog.savepoint("sp1".to_string()).unwrap();
        catalog.insert("users", vec![Expr::Number(2), Expr::String("Bob".to_string())]).unwrap();
        catalog.savepoint("sp2".to_string()).unwrap();
        catalog
            .insert("users", vec![Expr::Number(3), Expr::String("Charlie".to_string())])
            .unwrap();
        catalog.rollback_to_savepoint("sp1").unwrap();
        catalog.commit_transaction().unwrap();

        assert_eq!(catalog.row_count("users"), 1);
    }

    #[test]
    fn test_savepoint_cleared_on_rollback() {
        let catalog = Catalog::new();
        catalog.begin_transaction().unwrap();
        catalog.savepoint("sp1".to_string()).unwrap();
        catalog.rollback_transaction().unwrap();

        catalog.begin_transaction().unwrap();
        let result = catalog.rollback_to_savepoint("sp1");
        assert!(result.is_err());
    }
}
