#[cfg(test)]
mod data_manipulation_tests {
    use crate::catalog::Catalog;
    use crate::parser::ast::{BinaryOperator, ColumnDef, DataType, Expr};

    fn setup_catalog() -> Catalog {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef::new("id".to_string(), DataType::Int),
            ColumnDef::new("name".to_string(), DataType::Text),
        ];
        catalog.create_table("users".to_string(), columns).unwrap();
        catalog
    }

    #[test]
    fn test_insert_and_row_count() {
        let catalog = setup_catalog();
        assert_eq!(catalog.row_count("users"), 0);

        let values = vec![Expr::Number(1), Expr::String("Alice".to_string())];
        assert!(catalog.insert("users", &[], values).is_ok());
        assert_eq!(catalog.row_count("users"), 1);
    }

    #[test]
    fn test_batch_insert() {
        let catalog = setup_catalog();
        let batch = vec![
            vec![Expr::Number(1), Expr::String("Alice".to_string())],
            vec![Expr::Number(2), Expr::String("Bob".to_string())],
        ];

        let result = catalog.batch_insert("users", &[], batch);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2);
        assert_eq!(catalog.row_count("users"), 2);
    }

    #[test]
    fn test_update() {
        let catalog = setup_catalog();
        catalog
            .insert("users", &[], vec![Expr::Number(1), Expr::String("Alice".to_string())])
            .unwrap();

        let assignments = vec![("name".to_string(), Expr::String("Alicia".to_string()))];
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(1)),
        });

        let result = catalog.update("users", assignments, where_clause);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn test_delete() {
        let catalog = setup_catalog();
        catalog
            .insert("users", &[], vec![Expr::Number(1), Expr::String("Alice".to_string())])
            .unwrap();
        catalog
            .insert("users", &[], vec![Expr::Number(2), Expr::String("Bob".to_string())])
            .unwrap();
        assert_eq!(catalog.row_count("users"), 2);

        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(1)),
        });

        let result = catalog.delete("users", where_clause);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
        assert_eq!(catalog.row_count("users"), 1);
    }
}
