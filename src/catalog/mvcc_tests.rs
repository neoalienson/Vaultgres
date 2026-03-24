#[cfg(test)]
mod mvcc_tests {
    use crate::catalog::Catalog;
    use crate::parser::ast::{BinaryOperator, ColumnDef, DataType, Expr};

    fn setup_catalog_with_data() -> Catalog {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef::new("id".to_string(), DataType::Int),
            ColumnDef::new("name".to_string(), DataType::Text),
        ];
        catalog.create_table("users".to_string(), columns).unwrap();
        catalog
            .insert("users", &[], vec![Expr::Number(1), Expr::String("Alice".to_string())])
            .unwrap();
        catalog
            .insert("users", &[], vec![Expr::Number(2), Expr::String("Bob".to_string())])
            .unwrap();
        catalog
    }

    #[test]
    fn test_deleted_rows_are_not_counted() {
        let catalog = setup_catalog_with_data();
        assert_eq!(catalog.row_count("users"), 2);

        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(1)),
        });

        // Delete a row
        assert!(catalog.delete("users", where_clause).is_ok());

        // The row count should now be 1
        assert_eq!(catalog.row_count("users"), 1);
    }
}
