#[cfg(test)]
mod tests {
    use crate::catalog::Catalog;
    use crate::parser::ast::{ColumnDef, DataType, Expr};

    #[test]
    fn test_batch_insert() {
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

        let batch = vec![
            vec![Expr::Number(1), Expr::String("Alice".to_string())],
            vec![Expr::Number(2), Expr::String("Bob".to_string())],
            vec![Expr::Number(3), Expr::String("Charlie".to_string())],
        ];

        let count = catalog.batch_insert("users", batch).unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn test_batch_insert_empty() {
        let catalog = Catalog::new();

        catalog
            .create_table("test".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
            .unwrap();

        let count = catalog.batch_insert("test", vec![]).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_batch_insert_error() {
        let catalog = Catalog::new();

        catalog
            .create_table("test".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
            .unwrap();

        let batch = vec![vec![Expr::Number(1), Expr::Number(2)]];

        let result = catalog.batch_insert("test", batch);
        assert!(result.is_err());
    }
}
