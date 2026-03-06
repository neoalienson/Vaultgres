// src/catalog/catalog_tests.rs

// This file will contain unit tests for src/catalog/catalog.rs
// It will be included as a module in src/catalog/mod.rs

#[cfg(test)]
mod tests {

    use crate::catalog::Catalog;
    use crate::catalog::TableSchema;
    use crate::parser::ast::{
        ColumnDef, DataType, Expr, ForeignKeyAction, ForeignKeyDef, SelectStmt,
    };
    use tempfile;

    fn create_mock_table(catalog: &Catalog, table_name: &str) -> Result<(), String> {
        let columns = vec![
            ColumnDef::new("id".to_string(), DataType::Int),
            ColumnDef::new("name".to_string(), DataType::Text),
        ];
        catalog.create_table(table_name.to_string(), columns)
    }

    #[test]
    fn test_new_catalog() {
        let catalog = Catalog::new();

        // Verify that all internal Arc<RwLock<HashMap<...>>> fields are initialized as empty
        assert_eq!(catalog.tables.read().unwrap().len(), 0);
        assert_eq!(catalog.views.read().unwrap().len(), 0);
        assert_eq!(catalog.materialized_views.read().unwrap().len(), 0);
        assert_eq!(catalog.triggers.read().unwrap().len(), 0);
        assert_eq!(catalog.indexes.read().unwrap().len(), 0);
        assert_eq!(catalog.functions.read().unwrap().len(), 0);
        assert_eq!(catalog.data.read().unwrap().len(), 0);
        assert_eq!(catalog.sequences.read().unwrap().len(), 0);
        assert!(catalog.active_txn.read().unwrap().is_none());
        assert_eq!(catalog.savepoints.read().unwrap().len(), 0);
        assert!(catalog.data_dir.is_none());
        assert!(catalog.save_tx.is_none());
    }

    #[test]
    fn test_new_with_data_dir() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
        let data_dir_path = temp_dir.path().to_str().unwrap();

        let catalog = Catalog::new_with_data_dir(data_dir_path);

        assert!(catalog.data_dir.is_some());
        assert_eq!(catalog.data_dir.unwrap(), data_dir_path);
        assert!(catalog.save_tx.is_some());

        // Ensure the initial state is empty even with a data directory
        assert_eq!(catalog.tables.read().unwrap().len(), 0);
        assert_eq!(catalog.views.read().unwrap().len(), 0);
        // ... and so on for other fields
    }

    #[test]
    fn test_create_table() {
        let catalog = Catalog::new();

        // Test creating a table successfully
        let table_name = "users";
        let result = create_mock_table(&catalog, table_name);
        assert!(result.is_ok());
        assert!(catalog.get_table(table_name).is_some());
        assert_eq!(catalog.tables.read().unwrap().len(), 1);
        assert_eq!(catalog.data.read().unwrap().len(), 1);

        // Test creating a table that already exists
        let result = create_mock_table(&catalog, table_name);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Table 'users' already exists");
    }

    #[test]
    fn test_drop_table() {
        let catalog = Catalog::new();
        let table_name = "products";
        create_mock_table(&catalog, table_name).unwrap();
        assert!(catalog.get_table(table_name).is_some());

        // Test dropping a table successfully
        let result = catalog.drop_table(table_name, false);
        assert!(result.is_ok());
        assert!(catalog.get_table(table_name).is_none());
        assert_eq!(catalog.tables.read().unwrap().len(), 0);
        assert_eq!(catalog.data.read().unwrap().len(), 0);

        // Test dropping a non-existent table without if_exists
        let result = catalog.drop_table("non_existent", false);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Table 'non_existent' does not exist");

        // Test dropping a non-existent table with if_exists
        let result = catalog.drop_table("non_existent", true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_view() {
        let catalog = Catalog::new();
        let view_name = "active_users_view";
        let select_stmt = SelectStmt {
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

        // Test creating a view successfully
        let result = catalog.create_view(view_name.to_string(), select_stmt.clone());
        assert!(result.is_ok());
        assert!(catalog.get_view(view_name).is_some());
        assert_eq!(catalog.views.read().unwrap().len(), 1);

        // Test creating a view that already exists
        let result = catalog.create_view(view_name.to_string(), select_stmt);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "View 'active_users_view' already exists");
    }

    #[test]
    fn test_drop_view() {
        let catalog = Catalog::new();
        let view_name = "inactive_products_view";
        let select_stmt = SelectStmt {
            distinct: false,
            columns: vec![Expr::Column("id".to_string())],
            from: "products".to_string(),
            table_alias: None,
            joins: Vec::new(),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };
        catalog.create_view(view_name.to_string(), select_stmt).unwrap();
        assert!(catalog.get_view(view_name).is_some());

        // Test dropping a view successfully
        let result = catalog.drop_view(view_name, false);
        assert!(result.is_ok());
        assert!(catalog.get_view(view_name).is_none());
        assert_eq!(catalog.views.read().unwrap().len(), 0);

        // Test dropping a non-existent view without if_exists
        let result = catalog.drop_view("non_existent_view", false);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "View 'non_existent_view' does not exist");

        // Test dropping a non-existent view with if_exists
        let result = catalog.drop_view("non_existent_view", true);
        assert!(result.is_ok());
    }
}
