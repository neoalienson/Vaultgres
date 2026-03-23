use std::sync::Arc;
use vaultgres::catalog::{Catalog, EnumValue, Value};
use vaultgres::parser::ast::{ColumnDef, DataType, Expr};

fn create_enum_test_catalog() -> Arc<Catalog> {
    let catalog = Catalog::new();

    catalog
        .create_type(
            "order_status".to_string(),
            vec![
                "pending".to_string(),
                "processing".to_string(),
                "shipped".to_string(),
                "delivered".to_string(),
                "cancelled".to_string(),
            ],
        )
        .unwrap();

    catalog
        .create_type(
            "priority".to_string(),
            vec!["low".to_string(), "medium".to_string(), "high".to_string()],
        )
        .unwrap();

    catalog
        .create_table(
            "orders".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Serial),
                ColumnDef::new("customer".to_string(), DataType::Text),
                ColumnDef::new("status".to_string(), DataType::Enum("order_status".to_string())),
                ColumnDef::new("priority".to_string(), DataType::Enum("priority".to_string())),
            ],
        )
        .unwrap();

    catalog
        .insert(
            "orders",
            &["customer".to_string(), "status".to_string(), "priority".to_string()],
            vec![
                Expr::String("Alice".to_string()),
                Expr::String("pending".to_string()),
                Expr::String("high".to_string()),
            ],
        )
        .unwrap();

    catalog
        .insert(
            "orders",
            &["customer".to_string(), "status".to_string(), "priority".to_string()],
            vec![
                Expr::String("Bob".to_string()),
                Expr::String("shipped".to_string()),
                Expr::String("medium".to_string()),
            ],
        )
        .unwrap();

    catalog
        .insert(
            "orders",
            &["customer".to_string(), "status".to_string(), "priority".to_string()],
            vec![
                Expr::String("Charlie".to_string()),
                Expr::String("delivered".to_string()),
                Expr::String("low".to_string()),
            ],
        )
        .unwrap();

    Arc::new(catalog)
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_create_type_as_enum() {
        let catalog = Catalog::new();
        let result = catalog.create_type(
            "mood".to_string(),
            vec!["happy".to_string(), "sad".to_string(), "excited".to_string()],
        );
        assert!(result.is_ok());

        let enum_def = catalog.get_enum_type("mood").unwrap();
        assert_eq!(enum_def.type_name, "mood");
        assert_eq!(enum_def.labels.len(), 3);
    }

    #[test]
    fn test_create_table_with_enum_column() {
        let catalog = Catalog::new();
        catalog
            .create_type("color".to_string(), vec!["red".to_string(), "green".to_string()])
            .unwrap();

        catalog
            .create_table(
                "items".to_string(),
                vec![
                    ColumnDef::new("id".to_string(), DataType::Int),
                    ColumnDef::new("color".to_string(), DataType::Enum("color".to_string())),
                ],
            )
            .unwrap();

        let table = catalog.get_table("items").unwrap();
        assert_eq!(table.columns[1].data_type, DataType::Enum("color".to_string()));
    }

    #[test]
    fn test_insert_enum_values() {
        let catalog = Catalog::new();
        catalog
            .create_type(
                "status".to_string(),
                vec!["active".to_string(), "inactive".to_string(), "pending".to_string()],
            )
            .unwrap();

        catalog
            .create_table(
                "accounts".to_string(),
                vec![
                    ColumnDef::new("id".to_string(), DataType::Serial),
                    ColumnDef::new("status".to_string(), DataType::Enum("status".to_string())),
                ],
            )
            .unwrap();

        catalog
            .insert("accounts", &["status".to_string()], vec![Expr::String("active".to_string())])
            .unwrap();

        let rows = Catalog::select_with_catalog(
            &Arc::new(catalog),
            "accounts",
            false,
            vec![Expr::Column("status".to_string())],
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0][0],
            Value::Enum(EnumValue { type_name: "status".to_string(), index: 0 })
        );
    }

    #[test]
    fn test_select_all_enum_values() {
        let catalog = create_enum_test_catalog();

        let rows = Catalog::select_with_catalog(
            &catalog,
            "orders",
            false,
            vec![Expr::Column("customer".to_string())],
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::Text("Alice".to_string()));
        assert_eq!(rows[1][0], Value::Text("Bob".to_string()));
        assert_eq!(rows[2][0], Value::Text("Charlie".to_string()));
    }

    #[test]
    fn test_insert_invalid_enum_label() {
        let catalog = Catalog::new();
        catalog
            .create_type("status".to_string(), vec!["active".to_string(), "inactive".to_string()])
            .unwrap();

        catalog
            .create_table(
                "accounts".to_string(),
                vec![ColumnDef::new("status".to_string(), DataType::Enum("status".to_string()))],
            )
            .unwrap();

        let result = catalog.insert(
            "accounts",
            &["status".to_string()],
            vec![Expr::String("invalid_status".to_string())],
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid enum label"));
    }

    #[test]
    fn test_alter_type_add_value() {
        let catalog = Catalog::new();
        catalog
            .create_type("size".to_string(), vec!["small".to_string(), "medium".to_string()])
            .unwrap();

        catalog.alter_type_add_value("size", "large".to_string(), None).unwrap();

        let enum_def = catalog.get_enum_type("size").unwrap();
        assert_eq!(enum_def.labels.len(), 3);
        assert_eq!(enum_def.labels[2], "large");
    }

    #[test]
    fn test_drop_type_success() {
        let catalog = Catalog::new();
        catalog.create_type("temp".to_string(), vec!["a".to_string(), "b".to_string()]).unwrap();

        let result = catalog.drop_type("temp", false, false);
        assert!(result.is_ok());

        assert!(catalog.get_enum_type("temp").is_none());
    }

    #[test]
    fn test_drop_type_if_exists() {
        let catalog = Catalog::new();
        let result = catalog.drop_type("nonexistent", true, false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_drop_type_not_exists() {
        let catalog = Catalog::new();
        let result = catalog.drop_type("nonexistent", false, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("does not exist"));
    }

    #[test]
    fn test_enum_value_display() {
        let enum_val = Value::Enum(EnumValue { type_name: "test".to_string(), index: 1 });
        assert_eq!(format!("{}", enum_val), "test[1]");
    }

    #[test]
    fn test_enum_multiple_enum_types() {
        let catalog = Catalog::new();

        catalog
            .create_type(
                "color".to_string(),
                vec!["red".to_string(), "green".to_string(), "blue".to_string()],
            )
            .unwrap();

        catalog
            .create_type(
                "size".to_string(),
                vec!["small".to_string(), "medium".to_string(), "large".to_string()],
            )
            .unwrap();

        let color_def = catalog.get_enum_type("color").unwrap();
        let size_def = catalog.get_enum_type("size").unwrap();

        assert_eq!(color_def.labels.len(), 3);
        assert_eq!(size_def.labels.len(), 3);

        assert_eq!(catalog.get_enum_label_index("color", "red"), Some(0));
        assert_eq!(catalog.get_enum_label_index("size", "large"), Some(2));
    }

    #[test]
    fn test_create_type_empty_labels_fails() {
        let catalog = Catalog::new();
        let result = catalog.create_type("empty_enum".to_string(), vec![]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must have at least one label"));
    }

    #[test]
    fn test_create_type_duplicate_name_fails() {
        let catalog = Catalog::new();
        let result1 =
            catalog.create_type("color".to_string(), vec!["red".to_string(), "green".to_string()]);
        assert!(result1.is_ok());

        let result2 = catalog
            .create_type("color".to_string(), vec!["small".to_string(), "medium".to_string()]);
        assert!(result2.is_err());
        assert!(result2.unwrap_err().contains("already exists"));
    }

    #[test]
    fn test_alter_type_add_duplicate_label_fails() {
        let catalog = Catalog::new();
        catalog
            .create_type("color".to_string(), vec!["red".to_string(), "green".to_string()])
            .unwrap();

        let result = catalog.alter_type_add_value("color", "red".to_string(), None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("already exists"));
    }

    #[test]
    fn test_alter_type_add_value_after() {
        let catalog = Catalog::new();
        catalog
            .create_type("color".to_string(), vec!["red".to_string(), "blue".to_string()])
            .unwrap();

        catalog
            .alter_type_add_value("color", "green".to_string(), Some("red".to_string()))
            .unwrap();

        let enum_def = catalog.get_enum_type("color").unwrap();
        assert_eq!(enum_def.labels.len(), 3);
        assert_eq!(enum_def.labels[1], "green");
        assert_eq!(enum_def.labels[2], "blue");
    }

    #[test]
    fn test_enum_comparison_same_type() {
        let enum1 = Value::Enum(EnumValue { type_name: "color".to_string(), index: 0 });
        let enum2 = Value::Enum(EnumValue { type_name: "color".to_string(), index: 1 });
        let enum3 = Value::Enum(EnumValue { type_name: "color".to_string(), index: 0 });

        assert!(enum1 < enum2);
        assert_eq!(enum1, enum3);
    }

    #[test]
    fn test_enum_comparison_different_type() {
        let enum1 = Value::Enum(EnumValue { type_name: "color".to_string(), index: 0 });
        let enum2 = Value::Enum(EnumValue { type_name: "size".to_string(), index: 0 });

        assert!(enum1 != enum2);
    }

    #[test]
    fn test_enum_type_case_sensitive() {
        let catalog = Catalog::new();
        catalog.create_type("status".to_string(), vec!["Pending".to_string()]).unwrap();

        catalog
            .create_table(
                "items".to_string(),
                vec![ColumnDef::new("status".to_string(), DataType::Enum("status".to_string()))],
            )
            .unwrap();

        let result1 = catalog.insert(
            "items",
            &["status".to_string()],
            vec![Expr::String("Pending".to_string())],
        );
        assert!(result1.is_ok());

        let result2 = catalog.insert(
            "items",
            &["status".to_string()],
            vec![Expr::String("pending".to_string())],
        );
        assert!(result2.is_err());
    }
}
