use std::sync::Arc;
use vaultgres::catalog::{Catalog, Value};
use vaultgres::parser::ast::{ColumnDef, DataType, Expr};

fn create_json_test_catalog() -> Arc<Catalog> {
    let catalog = Catalog::new();
    catalog
        .create_table(
            "users".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Serial),
                ColumnDef::new("name".to_string(), DataType::Text),
                ColumnDef::new("data".to_string(), DataType::Json),
                ColumnDef::new("profile".to_string(), DataType::Jsonb),
            ],
        )
        .unwrap();

    catalog
        .insert(
            "users",
            &["name".to_string(), "data".to_string(), "profile".to_string()],
            vec![
                Expr::String("Alice".to_string()),
                Expr::String(r#"{"age": 30, "city": "NYC"}"#.to_string()),
                Expr::String(r#"{"active": true, "role": "admin"}"#.to_string()),
            ],
        )
        .unwrap();

    catalog
        .insert(
            "users",
            &["name".to_string(), "data".to_string(), "profile".to_string()],
            vec![
                Expr::String("Bob".to_string()),
                Expr::String(r#"{"age": 25, "city": "LA"}"#.to_string()),
                Expr::String(r#"{"active": false, "role": "user"}"#.to_string()),
            ],
        )
        .unwrap();

    catalog
        .insert(
            "users",
            &["name".to_string(), "data".to_string(), "profile".to_string()],
            vec![
                Expr::String("Charlie".to_string()),
                Expr::String(r#"{"age": 35, "hobbies": ["reading", "gaming"]}"#.to_string()),
                Expr::String(r#"{"active": true, "permissions": ["read", "write"]}"#.to_string()),
            ],
        )
        .unwrap();

    Arc::new(catalog)
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_create_table_with_json() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "jsontable".to_string(),
                vec![
                    ColumnDef::new("id".to_string(), DataType::Int),
                    ColumnDef::new("jsondata".to_string(), DataType::Json),
                ],
            )
            .unwrap();

        let table = catalog.get_table("jsontable").unwrap();
        assert_eq!(table.columns[1].data_type, DataType::Json);
    }

    #[test]
    fn test_create_table_with_jsonb() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "jsonbtable".to_string(),
                vec![
                    ColumnDef::new("id".to_string(), DataType::Int),
                    ColumnDef::new("jsonbdata".to_string(), DataType::Jsonb),
                ],
            )
            .unwrap();

        let table = catalog.get_table("jsonbtable").unwrap();
        assert_eq!(table.columns[1].data_type, DataType::Jsonb);
    }

    #[test]
    fn test_insert_json_values() {
        let catalog = create_json_test_catalog();
        let rows = Catalog::select_with_catalog(
            &catalog,
            "users",
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
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_parse_json_type() {
        let sql = "CREATE TABLE jsontest (id INT, data JSON)";
        let mut parser = vaultgres::parser::Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        if let vaultgres::parser::ast::Statement::CreateTable(create) = stmt {
            assert_eq!(create.columns[1].data_type, DataType::Json);
        } else {
            panic!("Expected CREATE TABLE statement");
        }
    }

    #[test]
    fn test_parse_jsonb_type() {
        let sql = "CREATE TABLE jsonbtest (id INT, data JSONB)";
        let mut parser = vaultgres::parser::Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        if let vaultgres::parser::ast::Statement::CreateTable(create) = stmt {
            assert_eq!(create.columns[1].data_type, DataType::Jsonb);
        } else {
            panic!("Expected CREATE TABLE statement");
        }
    }

    #[test]
    fn test_json_extract_operator() {
        let catalog = create_json_test_catalog();
        let rows = Catalog::select_with_catalog(
            &catalog,
            "users",
            false,
            vec![Expr::BinaryOp {
                left: Box::new(Expr::Column("data".to_string())),
                op: vaultgres::parser::ast::BinaryOperator::JsonExtract,
                right: Box::new(Expr::String("age".to_string())),
            }],
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::Json("30".to_string()));
        assert_eq!(rows[1][0], Value::Json("25".to_string()));
        assert_eq!(rows[2][0], Value::Json("35".to_string()));
    }

    #[test]
    fn test_json_extract_text_operator() {
        let catalog = create_json_test_catalog();
        let rows = Catalog::select_with_catalog(
            &catalog,
            "users",
            false,
            vec![Expr::BinaryOp {
                left: Box::new(Expr::Column("data".to_string())),
                op: vaultgres::parser::ast::BinaryOperator::JsonExtractText,
                right: Box::new(Expr::String("city".to_string())),
            }],
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::Text("NYC".to_string()));
        assert_eq!(rows[1][0], Value::Text("LA".to_string()));
        assert_eq!(rows[2][0], Value::Null);
    }

    #[test]
    fn test_json_exists_operator() {
        let catalog = create_json_test_catalog();
        let rows = Catalog::select_with_catalog(
            &catalog,
            "users",
            false,
            vec![Expr::Column("name".to_string())],
            Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("data".to_string())),
                op: vaultgres::parser::ast::BinaryOperator::JsonExists,
                right: Box::new(Expr::String("age".to_string())),
            }),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_json_exists_operator_false() {
        let catalog = create_json_test_catalog();
        let rows = Catalog::select_with_catalog(
            &catalog,
            "users",
            false,
            vec![Expr::Column("name".to_string())],
            Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("data".to_string())),
                op: vaultgres::parser::ast::BinaryOperator::JsonExists,
                right: Box::new(Expr::String("nonexistent".to_string())),
            }),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_json_extract_nested() {
        let catalog = create_json_test_catalog();
        let rows = Catalog::select_with_catalog(
            &catalog,
            "users",
            false,
            vec![Expr::BinaryOp {
                left: Box::new(Expr::Column("profile".to_string())),
                op: vaultgres::parser::ast::BinaryOperator::JsonExtractText,
                right: Box::new(Expr::String("role".to_string())),
            }],
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::Text("admin".to_string()));
        assert_eq!(rows[1][0], Value::Text("user".to_string()));
    }

    #[test]
    fn test_json_extract_array() {
        let catalog = create_json_test_catalog();
        let rows = Catalog::select_with_catalog(
            &catalog,
            "users",
            false,
            vec![Expr::BinaryOp {
                left: Box::new(Expr::Column("data".to_string())),
                op: vaultgres::parser::ast::BinaryOperator::JsonExtractText,
                right: Box::new(Expr::String("hobbies".to_string())),
            }],
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[2][0], Value::Text("[\"reading\", \"gaming\"]".to_string()));
    }

    #[test]
    fn test_json_null_handling() {
        let catalog = create_json_test_catalog();
        let rows = Catalog::select_with_catalog(
            &catalog,
            "users",
            false,
            vec![Expr::BinaryOp {
                left: Box::new(Expr::Column("data".to_string())),
                op: vaultgres::parser::ast::BinaryOperator::JsonExtractText,
                right: Box::new(Expr::String("nonexistent_key".to_string())),
            }],
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::Null);
    }

    #[test]
    fn test_json_empty_object() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "empty_test".to_string(),
                vec![ColumnDef::new("data".to_string(), DataType::Json)],
            )
            .unwrap();
        catalog
            .insert("empty_test", &["data".to_string()], vec![Expr::String("{}".to_string())])
            .unwrap();
        let catalog_arc = Arc::new(catalog);
        let rows = Catalog::select_with_catalog(
            &catalog_arc,
            "empty_test",
            false,
            vec![Expr::Column("data".to_string())],
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_json_empty_array() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "array_test".to_string(),
                vec![ColumnDef::new("data".to_string(), DataType::Json)],
            )
            .unwrap();
        catalog
            .insert("array_test", &["data".to_string()], vec![Expr::String("[]".to_string())])
            .unwrap();
        let catalog_arc = Arc::new(catalog);
        let rows = Catalog::select_with_catalog(
            &catalog_arc,
            "array_test",
            false,
            vec![Expr::Column("data".to_string())],
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_json_with_special_characters() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "special_test".to_string(),
                vec![ColumnDef::new("data".to_string(), DataType::Json)],
            )
            .unwrap();
        catalog
            .insert(
                "special_test",
                &["data".to_string()],
                vec![Expr::String(r#"{"message": "Hello\nWorld"}"#.to_string())],
            )
            .unwrap();
        let catalog_arc = Arc::new(catalog);
        let rows = Catalog::select_with_catalog(
            &catalog_arc,
            "special_test",
            false,
            vec![Expr::BinaryOp {
                left: Box::new(Expr::Column("data".to_string())),
                op: vaultgres::parser::ast::BinaryOperator::JsonExtractText,
                right: Box::new(Expr::String("message".to_string())),
            }],
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_json_type_mismatch_in_insert() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "mismatch_test".to_string(),
                vec![ColumnDef::new("data".to_string(), DataType::Json)],
            )
            .unwrap();
        let result =
            catalog.insert("mismatch_test", &["data".to_string()], vec![Expr::Number(123)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_jsonb_and_json_types_distinct() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "type_test".to_string(),
                vec![
                    ColumnDef::new("j".to_string(), DataType::Json),
                    ColumnDef::new("jb".to_string(), DataType::Jsonb),
                ],
            )
            .unwrap();
        let table = catalog.get_table("type_test").unwrap();
        assert_eq!(table.columns[0].data_type, DataType::Json);
        assert_eq!(table.columns[1].data_type, DataType::Jsonb);
    }

    #[test]
    fn test_json_comparison_with_null() {
        let catalog = Catalog::new();
        catalog
            .create_table(
                "null_test".to_string(),
                vec![ColumnDef::new("data".to_string(), DataType::Json)],
            )
            .unwrap();
        catalog
            .insert(
                "null_test",
                &["data".to_string()],
                vec![Expr::String(r#"{"key": "value"}"#.to_string())],
            )
            .unwrap();
        catalog.insert("null_test", &["data".to_string()], vec![Expr::Null]).unwrap();
        let catalog_arc = Arc::new(catalog);
        let rows = Catalog::select_with_catalog(
            &catalog_arc,
            "null_test",
            false,
            vec![Expr::Column("data".to_string())],
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_json_extract_multiple_rows() {
        let catalog = create_json_test_catalog();
        let rows = Catalog::select_with_catalog(
            &catalog,
            "users",
            false,
            vec![
                Expr::Column("name".to_string()),
                Expr::BinaryOp {
                    left: Box::new(Expr::Column("data".to_string())),
                    op: vaultgres::parser::ast::BinaryOperator::JsonExtractText,
                    right: Box::new(Expr::String("city".to_string())),
                },
            ],
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_json_where_clause_with_exists() {
        let catalog = create_json_test_catalog();
        let rows = Catalog::select_with_catalog(
            &catalog,
            "users",
            false,
            vec![Expr::Column("name".to_string())],
            Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("data".to_string())),
                op: vaultgres::parser::ast::BinaryOperator::JsonExists,
                right: Box::new(Expr::String("age".to_string())),
            }),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_json_where_clause_with_exists_any() {
        let catalog = create_json_test_catalog();
        let rows = Catalog::select_with_catalog(
            &catalog,
            "users",
            false,
            vec![Expr::Column("name".to_string())],
            Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("data".to_string())),
                op: vaultgres::parser::ast::BinaryOperator::JsonExistsAny,
                right: Box::new(Expr::String(r#"["age", "name"]"#.to_string())),
            }),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_json_where_clause_with_exists_all() {
        let catalog = create_json_test_catalog();
        let rows = Catalog::select_with_catalog(
            &catalog,
            "users",
            false,
            vec![Expr::Column("name".to_string())],
            Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("data".to_string())),
                op: vaultgres::parser::ast::BinaryOperator::JsonExistsAll,
                right: Box::new(Expr::String(r#"["age", "city"]"#.to_string())),
            }),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
    }
}
