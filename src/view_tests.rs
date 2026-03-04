//! Integration tests for view expansion and the compositional execution engine

#[cfg(test)]
mod tests {
    use crate::catalog::Catalog;
    use crate::parser::ast::{Expr, SelectStmt};
    use std::sync::Arc;

    #[test]
    fn test_view_creation_and_query() {
        let catalog = Catalog::new();

        // Create a table
        let columns = vec![
            crate::parser::ast::ColumnDef::new("id".to_string(), crate::parser::ast::DataType::Int),
            crate::parser::ast::ColumnDef::new(
                "name".to_string(),
                crate::parser::ast::DataType::Text,
            ),
            crate::parser::ast::ColumnDef::new(
                "age".to_string(),
                crate::parser::ast::DataType::Int,
            ),
        ];
        catalog.create_table("users".to_string(), columns).unwrap();

        // Insert some data
        catalog
            .insert(
                "users",
                vec![Expr::Number(1), Expr::String("Alice".to_string()), Expr::Number(25)],
            )
            .unwrap();

        catalog
            .insert(
                "users",
                vec![Expr::Number(2), Expr::String("Bob".to_string()), Expr::Number(30)],
            )
            .unwrap();

        catalog
            .insert(
                "users",
                vec![Expr::Number(3), Expr::String("Charlie".to_string()), Expr::Number(35)],
            )
            .unwrap();

        // Create a view
        let view_query = SelectStmt {
            distinct: false,
            columns: vec![Expr::Column("id".to_string()), Expr::Column("name".to_string())],
            from: "users".to_string(),
            table_alias: None,
            joins: Vec::new(),
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: crate::parser::ast::BinaryOperator::LessThan,
                right: Box::new(Expr::Number(30)),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        catalog.create_view("young_users".to_string(), view_query).unwrap();

        // Verify the view was created
        assert!(catalog.get_view("young_users").is_some());

        // Query the view
        let results = Catalog::select_with_catalog(
            &Arc::new(catalog.clone()),
            "young_users",
            false,
            vec![Expr::Column("name".to_string())],
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        // Should return Alice only (age < 30)
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_view_with_outer_filter() {
        let catalog = Catalog::new();

        // Create a table
        let columns = vec![
            crate::parser::ast::ColumnDef::new("id".to_string(), crate::parser::ast::DataType::Int),
            crate::parser::ast::ColumnDef::new(
                "name".to_string(),
                crate::parser::ast::DataType::Text,
            ),
            crate::parser::ast::ColumnDef::new(
                "age".to_string(),
                crate::parser::ast::DataType::Int,
            ),
        ];
        catalog.create_table("users".to_string(), columns).unwrap();

        // Insert some data
        catalog
            .insert(
                "users",
                vec![Expr::Number(1), Expr::String("Alice".to_string()), Expr::Number(25)],
            )
            .unwrap();

        catalog
            .insert(
                "users",
                vec![Expr::Number(2), Expr::String("Bob".to_string()), Expr::Number(30)],
            )
            .unwrap();

        catalog
            .insert(
                "users",
                vec![Expr::Number(3), Expr::String("Charlie".to_string()), Expr::Number(35)],
            )
            .unwrap();

        // Create a view for all users
        let view_query = SelectStmt {
            distinct: false,
            columns: vec![
                Expr::Column("id".to_string()),
                Expr::Column("name".to_string()),
                Expr::Column("age".to_string()),
            ],
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

        catalog.create_view("all_users".to_string(), view_query).unwrap();

        // Query the view with an outer filter
        let results = Catalog::select_with_catalog(
            &Arc::new(catalog.clone()),
            "all_users",
            false,
            vec![Expr::Column("name".to_string())],
            Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: crate::parser::ast::BinaryOperator::GreaterThan,
                right: Box::new(Expr::Number(1)),
            }),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        // Should return Bob and Charlie (id > 1)
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_simple_table_query() {
        let catalog = Catalog::new();

        // Create a table
        let columns = vec![
            crate::parser::ast::ColumnDef::new("id".to_string(), crate::parser::ast::DataType::Int),
            crate::parser::ast::ColumnDef::new(
                "name".to_string(),
                crate::parser::ast::DataType::Text,
            ),
        ];
        catalog.create_table("test".to_string(), columns).unwrap();

        // Insert data
        catalog.insert("test", vec![Expr::Number(1), Expr::String("Test1".to_string())]).unwrap();

        catalog.insert("test", vec![Expr::Number(2), Expr::String("Test2".to_string())]).unwrap();

        // Query the table
        let results = Catalog::select_with_catalog(
            &Arc::new(catalog.clone()),
            "test",
            false,
            vec![Expr::Column("id".to_string()), Expr::Column("name".to_string())],
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        assert_eq!(results.len(), 2);
    }
}
