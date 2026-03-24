#[cfg(test)]
mod materialized_view_tests {
    use crate::catalog::Catalog;
    use crate::parser::ast::{ColumnDef, DataType, Expr, SelectStmt};
    use std::sync::Arc;
    use tempfile;

    fn setup_catalog_with_data() -> Arc<Catalog> {
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
        Arc::new(catalog)
    }

    #[test]
    fn test_create_materialized_view() {
        let catalog = setup_catalog_with_data();
        let view_name = "users_mv";
        let query = SelectStmt {
            distinct: false,
            columns: vec![Expr::Column("id".to_string())],
            from: "users".to_string(),
            table_alias: None,
            joins: Vec::new(),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        let result = catalog.create_materialized_view(view_name.to_string(), query.clone());
        assert!(result.is_ok());

        let mv_data = catalog.get_materialized_view(view_name).unwrap();
        assert_eq!(mv_data.len(), 2);

        // Try to create it again
        let result = catalog.create_materialized_view(view_name.to_string(), query);
        assert!(result.is_err());
    }

    #[test]
    fn test_drop_materialized_view() {
        let catalog = setup_catalog_with_data();
        let view_name = "users_mv_to_drop";
        let query = SelectStmt {
            distinct: false,
            columns: vec![Expr::Column("id".to_string())],
            from: "users".to_string(),
            table_alias: None,
            joins: Vec::new(),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };
        catalog.create_materialized_view(view_name.to_string(), query).unwrap();
        assert!(catalog.get_materialized_view(view_name).is_some());

        // Drop it
        assert!(catalog.drop_materialized_view(view_name, false).is_ok());
        assert!(catalog.get_materialized_view(view_name).is_none());

        // Drop non-existent
        assert!(catalog.drop_materialized_view("nonexistent", false).is_err());
        assert!(catalog.drop_materialized_view("nonexistent", true).is_ok());
    }

    #[test]
    fn test_refresh_materialized_view() {
        let catalog = setup_catalog_with_data();
        let view_name = "users_mv_to_refresh";
        let query = SelectStmt {
            distinct: false,
            columns: vec![Expr::Column("id".to_string())],
            from: "users".to_string(),
            table_alias: None,
            joins: Vec::new(),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };
        catalog.create_materialized_view(view_name.to_string(), query).unwrap();

        let mv_data = catalog.get_materialized_view(view_name).unwrap();
        assert_eq!(mv_data.len(), 2);

        // Insert more data into the base table
        catalog
            .insert("users", &[], vec![Expr::Number(3), Expr::String("Charlie".to_string())])
            .unwrap();

        // Refresh the view
        assert!(catalog.refresh_materialized_view(view_name).is_ok());

        let mv_data_refreshed = catalog.get_materialized_view(view_name).unwrap();
        assert_eq!(mv_data_refreshed.len(), 3);
    }
}
