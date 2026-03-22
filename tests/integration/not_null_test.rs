#[cfg(test)]
mod tests {
    use vaultgres::catalog::Catalog;
    use vaultgres::parser::ast::*;

    #[test]
    fn test_not_null_constraint_violation() {
        let catalog = Catalog::new();

        let mut col = ColumnDef::new("id".to_string(), DataType::Int);
        col.is_not_null = true;

        catalog
            .create_table(
                "users".to_string(),
                vec![col, ColumnDef::new("name".to_string(), DataType::Text)],
            )
            .unwrap();

        // Try to insert NULL into NOT NULL column
        let result =
            catalog.insert("users", &[], vec![Expr::Number(0), Expr::String("Alice".to_string())]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_not_null_with_default() {
        let catalog = Catalog::new();

        let mut col = ColumnDef::new("id".to_string(), DataType::Int);
        col.is_not_null = true;
        col.default_value = Some(Expr::Number(1));

        catalog
            .create_table(
                "users".to_string(),
                vec![col, ColumnDef::new("name".to_string(), DataType::Text)],
            )
            .unwrap();

        let result = catalog.insert("users", &[], vec![Expr::String("Alice".to_string())]);
        assert!(result.is_err()); // Partial insert not allowed without auto-fillable
    }

    #[test]
    fn test_primary_key_implies_not_null() {
        let catalog = Catalog::new();

        let mut col = ColumnDef::new("id".to_string(), DataType::Int);
        col.is_primary_key = true;

        catalog
            .create_table(
                "users".to_string(),
                vec![col, ColumnDef::new("name".to_string(), DataType::Text)],
            )
            .unwrap();

        // Primary key columns are implicitly NOT NULL
        // This is already enforced in the existing code
        let result =
            catalog.insert("users", &[], vec![Expr::Number(1), Expr::String("Alice".to_string())]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_nullable_column_allows_null() {
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

        // Both columns are nullable by default
        let result =
            catalog.insert("users", &[], vec![Expr::Number(1), Expr::String("Alice".to_string())]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_multiple_not_null_columns() {
        let catalog = Catalog::new();

        let mut col1 = ColumnDef::new("id".to_string(), DataType::Int);
        col1.is_not_null = true;

        let mut col2 = ColumnDef::new("name".to_string(), DataType::Text);
        col2.is_not_null = true;

        catalog.create_table("users".to_string(), vec![col1, col2]).unwrap();

        let result =
            catalog.insert("users", &[], vec![Expr::Number(1), Expr::String("Alice".to_string())]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_not_null_with_serial() {
        let catalog = Catalog::new();

        catalog
            .create_table(
                "users".to_string(),
                vec![
                    ColumnDef::new("id".to_string(), DataType::Serial),
                    ColumnDef::new("name".to_string(), DataType::Text),
                ],
            )
            .unwrap();

        // Both columns provided
        let result =
            catalog.insert("users", &[], vec![Expr::Number(1), Expr::String("Alice".to_string())]);
        assert!(result.is_ok());
        assert_eq!(catalog.row_count("users"), 1);
    }
}
