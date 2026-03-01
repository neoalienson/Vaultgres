//! Additional tests for catalog coverage

#[cfg(test)]
mod tests {
    use crate::catalog::Catalog;
    use crate::parser::ast::{ColumnDef, DataType, Expr};

    #[test]
    fn test_list_tables() {
        let catalog = Catalog::new();
        catalog.create_table("t1".to_string(), vec![]).unwrap();
        catalog.create_table("t2".to_string(), vec![]).unwrap();
        let tables = catalog.list_tables();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"t1".to_string()));
        assert!(tables.contains(&"t2".to_string()));
    }

    #[test]
    fn test_row_count() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "t".to_string(),
                vec![ColumnDef { name: "id".to_string(), data_type: DataType::Int }],
            )
            .unwrap();
        assert_eq!(catalog.row_count("t"), 0);
        catalog.insert("t", vec![Expr::Number(1)]).unwrap();
        assert_eq!(catalog.row_count("t"), 1);
    }

    #[test]
    fn test_row_count_nonexistent() {
        let catalog = Catalog::new();
        assert_eq!(catalog.row_count("nonexistent"), 0);
    }

    #[test]
    fn test_insert_type_mismatch() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "t".to_string(),
                vec![ColumnDef { name: "id".to_string(), data_type: DataType::Int }],
            )
            .unwrap();
        let result = catalog.insert("t", vec![Expr::String("text".to_string())]);
        assert!(result.is_err());
    }

    #[test]
    fn test_insert_wrong_column_count() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "t".to_string(),
                vec![ColumnDef { name: "id".to_string(), data_type: DataType::Int }],
            )
            .unwrap();
        let result = catalog.insert("t", vec![Expr::Number(1), Expr::Number(2)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_select_nonexistent_column() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "t".to_string(),
                vec![ColumnDef { name: "id".to_string(), data_type: DataType::Int }],
            )
            .unwrap();
        catalog.insert("t", vec![Expr::Number(1)]).unwrap();
        let result = catalog.select(
            "t",
            false,
            vec!["nonexistent".to_string()],
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_update_nonexistent_column() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "t".to_string(),
                vec![ColumnDef { name: "id".to_string(), data_type: DataType::Int }],
            )
            .unwrap();
        catalog.insert("t", vec![Expr::Number(1)]).unwrap();
        let result = catalog.update("t", vec![("nonexistent".to_string(), Expr::Number(1))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_update_type_mismatch() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "t".to_string(),
                vec![ColumnDef { name: "id".to_string(), data_type: DataType::Int }],
            )
            .unwrap();
        catalog.insert("t", vec![Expr::Number(1)]).unwrap();
        let result =
            catalog.update("t", vec![("id".to_string(), Expr::String("text".to_string()))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_catalog_default() {
        let catalog = Catalog::default();
        assert_eq!(catalog.list_tables().len(), 0);
    }

    #[test]
    fn test_select_varchar_type() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "t".to_string(),
                vec![ColumnDef { name: "name".to_string(), data_type: DataType::Varchar(50) }],
            )
            .unwrap();
        catalog.insert("t", vec![Expr::String("test".to_string())]).unwrap();
        let result =
            catalog.select("t", false, vec!["*".to_string()], None, None, None, None, None, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_update_varchar_type() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "t".to_string(),
                vec![ColumnDef { name: "name".to_string(), data_type: DataType::Varchar(50) }],
            )
            .unwrap();
        catalog.insert("t", vec![Expr::String("old".to_string())]).unwrap();
        let result =
            catalog.update("t", vec![("name".to_string(), Expr::String("new".to_string()))], None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_text_type() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "t".to_string(),
                vec![ColumnDef { name: "desc".to_string(), data_type: DataType::Text }],
            )
            .unwrap();
        let result = catalog.insert("t", vec![Expr::String("description".to_string())]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_update_text_type() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "t".to_string(),
                vec![ColumnDef { name: "desc".to_string(), data_type: DataType::Text }],
            )
            .unwrap();
        catalog.insert("t", vec![Expr::String("old".to_string())]).unwrap();
        let result =
            catalog.update("t", vec![("desc".to_string(), Expr::String("new".to_string()))], None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_invalid_expr() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "t".to_string(),
                vec![ColumnDef { name: "id".to_string(), data_type: DataType::Int }],
            )
            .unwrap();
        let result = catalog.insert("t", vec![Expr::Column("col".to_string())]);
        assert!(result.is_err());
    }

    #[test]
    fn test_update_invalid_expr() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "t".to_string(),
                vec![ColumnDef { name: "id".to_string(), data_type: DataType::Int }],
            )
            .unwrap();
        catalog.insert("t", vec![Expr::Number(1)]).unwrap();
        let result =
            catalog.update("t", vec![("id".to_string(), Expr::Column("col".to_string()))], None);
        assert!(result.is_err());
    }
}
