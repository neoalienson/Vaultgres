#[cfg(test)]
mod tests {
    use vaultgres::catalog::Catalog;
    use vaultgres::parser::ast::*;

    #[test]
    fn test_fk_action_restrict_default() {
        let catalog = Catalog::new();

        catalog
            .create_table(
                "parent".to_string(),
                vec![ColumnDef::new("id".to_string(), DataType::Int)],
            )
            .unwrap();

        let mut child_col = ColumnDef::new("parent_id".to_string(), DataType::Int);
        child_col.foreign_key =
            Some(ForeignKeyRef { table: "parent".to_string(), column: "id".to_string() });

        catalog.create_table("child".to_string(), vec![child_col]).unwrap();

        // Insert parent
        catalog.insert("parent", &[], vec![Expr::Number(1)]).unwrap();

        // Insert child referencing parent
        catalog.insert("child", &[], vec![Expr::Number(1)]).unwrap();

        assert_eq!(catalog.row_count("parent"), 1);
        assert_eq!(catalog.row_count("child"), 1);
    }

    #[test]
    fn test_fk_with_explicit_actions() {
        let catalog = Catalog::new();

        catalog
            .create_table(
                "parent".to_string(),
                vec![ColumnDef::new("id".to_string(), DataType::Int)],
            )
            .unwrap();

        let fk = ForeignKeyDef {
            columns: vec!["parent_id".to_string()],
            ref_table: "parent".to_string(),
            ref_columns: vec!["id".to_string()],
            on_delete: ForeignKeyAction::Cascade,
            on_update: ForeignKeyAction::Cascade,
        };

        catalog
            .create_table_with_constraints(
                "child".to_string(),
                vec![ColumnDef::new("parent_id".to_string(), DataType::Int)],
                None,
                vec![fk],
            )
            .unwrap();

        assert!(catalog.get_table("child").is_some());
    }

    #[test]
    fn test_fk_action_set_null() {
        let catalog = Catalog::new();

        catalog
            .create_table(
                "parent".to_string(),
                vec![ColumnDef::new("id".to_string(), DataType::Int)],
            )
            .unwrap();

        let fk = ForeignKeyDef {
            columns: vec!["parent_id".to_string()],
            ref_table: "parent".to_string(),
            ref_columns: vec!["id".to_string()],
            on_delete: ForeignKeyAction::SetNull,
            on_update: ForeignKeyAction::SetNull,
        };

        catalog
            .create_table_with_constraints(
                "child".to_string(),
                vec![ColumnDef::new("parent_id".to_string(), DataType::Int)],
                None,
                vec![fk],
            )
            .unwrap();

        assert!(catalog.get_table("child").is_some());
    }

    #[test]
    fn test_fk_action_restrict() {
        let catalog = Catalog::new();

        catalog
            .create_table(
                "parent".to_string(),
                vec![ColumnDef::new("id".to_string(), DataType::Int)],
            )
            .unwrap();

        let fk = ForeignKeyDef {
            columns: vec!["parent_id".to_string()],
            ref_table: "parent".to_string(),
            ref_columns: vec!["id".to_string()],
            on_delete: ForeignKeyAction::Restrict,
            on_update: ForeignKeyAction::Restrict,
        };

        catalog
            .create_table_with_constraints(
                "child".to_string(),
                vec![ColumnDef::new("parent_id".to_string(), DataType::Int)],
                None,
                vec![fk],
            )
            .unwrap();

        assert!(catalog.get_table("child").is_some());
    }
}
