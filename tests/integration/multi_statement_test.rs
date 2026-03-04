#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use vaultgres::catalog::Catalog;
    use vaultgres::parser::ast::*;

    #[test]
    fn test_multi_statement_transaction_commit() {
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

        catalog.begin_transaction().unwrap();
        catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
        catalog.insert("users", vec![Expr::Number(2), Expr::String("Bob".to_string())]).unwrap();
        catalog
            .insert("users", vec![Expr::Number(3), Expr::String("Charlie".to_string())])
            .unwrap();
        catalog.commit_transaction().unwrap();

        assert_eq!(catalog.row_count("users"), 3);
    }

    #[test]
    fn test_multi_statement_transaction_rollback() {
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

        catalog.begin_transaction().unwrap();
        catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
        catalog.insert("users", vec![Expr::Number(2), Expr::String("Bob".to_string())]).unwrap();
        catalog.rollback_transaction().unwrap();

        assert_eq!(catalog.row_count("users"), 2);
    }

    #[test]
    fn test_multi_statement_mixed_operations() {
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

        catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();

        catalog.begin_transaction().unwrap();
        catalog.insert("users", vec![Expr::Number(2), Expr::String("Bob".to_string())]).unwrap();
        catalog
            .update(
                "users",
                vec![("name".to_string(), Expr::String("Bobby".to_string()))],
                Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column("id".to_string())),
                    op: BinaryOperator::Equals,
                    right: Box::new(Expr::Number(2)),
                }),
            )
            .unwrap();
        catalog
            .delete(
                "users",
                Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column("id".to_string())),
                    op: BinaryOperator::Equals,
                    right: Box::new(Expr::Number(1)),
                }),
            )
            .unwrap();
        catalog.commit_transaction().unwrap();

        assert_eq!(catalog.row_count("users"), 2);
    }

    #[test]
    fn test_multi_statement_with_savepoint() {
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

        catalog.begin_transaction().unwrap();
        catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
        catalog.insert("users", vec![Expr::Number(2), Expr::String("Bob".to_string())]).unwrap();
        catalog.savepoint("sp1".to_string()).unwrap();
        catalog
            .insert("users", vec![Expr::Number(3), Expr::String("Charlie".to_string())])
            .unwrap();
        catalog.insert("users", vec![Expr::Number(4), Expr::String("Dave".to_string())]).unwrap();
        catalog.rollback_to_savepoint("sp1").unwrap();
        catalog.insert("users", vec![Expr::Number(5), Expr::String("Eve".to_string())]).unwrap();
        catalog.commit_transaction().unwrap();

        assert_eq!(catalog.row_count("users"), 3);
    }

    #[test]
    fn test_multi_statement_isolation() {
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

        catalog.begin_transaction().unwrap();
        catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
        catalog.insert("users", vec![Expr::Number(2), Expr::String("Bob".to_string())]).unwrap();

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
        assert_eq!(result.unwrap().len(), 2);

        catalog.commit_transaction().unwrap();
    }

    #[test]
    fn test_multi_statement_error_handling() {
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

        catalog.begin_transaction().unwrap();
        catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();

        let result = catalog.insert("nonexistent", vec![Expr::Number(1)]);
        assert!(result.is_err());

        catalog.rollback_transaction().unwrap();
        assert_eq!(catalog.row_count("users"), 1);
    }

    #[test]
    fn test_sequential_multi_statement_transactions() {
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

        catalog.begin_transaction().unwrap();
        catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
        catalog.insert("users", vec![Expr::Number(2), Expr::String("Bob".to_string())]).unwrap();
        catalog.commit_transaction().unwrap();

        catalog.begin_transaction().unwrap();
        catalog
            .insert("users", vec![Expr::Number(3), Expr::String("Charlie".to_string())])
            .unwrap();
        catalog.insert("users", vec![Expr::Number(4), Expr::String("Dave".to_string())]).unwrap();
        catalog.commit_transaction().unwrap();

        assert_eq!(catalog.row_count("users"), 4);
    }

    #[test]
    fn test_multi_statement_large_transaction() {
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

        catalog.begin_transaction().unwrap();
        for i in 1..=100 {
            catalog
                .insert("users", vec![Expr::Number(i), Expr::String(format!("User{}", i))])
                .unwrap();
        }
        catalog.commit_transaction().unwrap();

        assert_eq!(catalog.row_count("users"), 100);
    }
}
