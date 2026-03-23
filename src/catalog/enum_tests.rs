#[cfg(test)]
mod tests {
    use crate::catalog::{Catalog, EnumTypeDef, EnumValue, Value};
    use crate::parser::ast::{ColumnDef, DataType, Expr};

    #[test]
    fn test_enum_value_creation() {
        let enum_val = Value::Enum(EnumValue { type_name: "color".to_string(), index: 0 });
        assert_eq!(format!("{}", enum_val), "color[0]");
    }

    #[test]
    fn test_enum_value_comparison_same_type() {
        let enum1 = Value::Enum(EnumValue { type_name: "color".to_string(), index: 0 });
        let enum2 = Value::Enum(EnumValue { type_name: "color".to_string(), index: 1 });
        let enum3 = Value::Enum(EnumValue { type_name: "color".to_string(), index: 0 });
        assert!(enum1 < enum2);
        assert_eq!(enum1, enum3);
    }

    #[test]
    fn test_enum_value_comparison_different_type() {
        let enum1 = Value::Enum(EnumValue { type_name: "color".to_string(), index: 0 });
        let enum2 = Value::Enum(EnumValue { type_name: "size".to_string(), index: 0 });
        assert!(enum1 != enum2);
    }

    #[test]
    fn test_enum_value_comparison_same_index_different_type() {
        let enum1 = Value::Enum(EnumValue { type_name: "color".to_string(), index: 1 });
        let enum2 = Value::Enum(EnumValue { type_name: "size".to_string(), index: 1 });
        assert_ne!(enum1, enum2);
    }

    #[test]
    fn test_enum_value_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn calculate_hash<T: Hash>(t: &T) -> u64 {
            let mut s = DefaultHasher::new();
            t.hash(&mut s);
            s.finish()
        }

        let enum1 = Value::Enum(EnumValue { type_name: "color".to_string(), index: 0 });
        let enum2 = Value::Enum(EnumValue { type_name: "color".to_string(), index: 0 });
        let enum3 = Value::Enum(EnumValue { type_name: "color".to_string(), index: 1 });
        assert_eq!(calculate_hash(&enum1), calculate_hash(&enum2));
        assert_ne!(calculate_hash(&enum1), calculate_hash(&enum3));
    }

    #[test]
    fn test_enum_type_def() {
        let enum_def = EnumTypeDef {
            type_name: "weekday".to_string(),
            labels: vec![
                "Monday".to_string(),
                "Tuesday".to_string(),
                "Wednesday".to_string(),
                "Thursday".to_string(),
                "Friday".to_string(),
                "Saturday".to_string(),
                "Sunday".to_string(),
            ],
        };
        assert_eq!(enum_def.type_name, "weekday");
        assert_eq!(enum_def.labels.len(), 7);
        assert_eq!(enum_def.labels[0], "Monday");
        assert_eq!(enum_def.labels[6], "Sunday");
    }

    #[test]
    fn test_datatype_enum() {
        let dt = DataType::Enum("color".to_string());
        assert_eq!(dt, DataType::Enum("color".to_string()));
    }

    #[test]
    fn test_datatype_enum_different_types() {
        assert_ne!(DataType::Enum("color".to_string()), DataType::Enum("size".to_string()));
    }

    #[test]
    fn test_datatype_enum_vs_int() {
        assert_ne!(DataType::Enum("color".to_string()), DataType::Int);
    }

    #[test]
    fn test_create_type_basic() {
        let catalog = Catalog::new();
        let result = catalog.create_type(
            "color".to_string(),
            vec!["red".to_string(), "green".to_string(), "blue".to_string()],
        );
        assert!(result.is_ok());

        let enum_def = catalog.get_enum_type("color").unwrap();
        assert_eq!(enum_def.type_name, "color");
        assert_eq!(enum_def.labels.len(), 3);
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
    fn test_drop_type_success() {
        let catalog = Catalog::new();
        let result1 =
            catalog.create_type("color".to_string(), vec!["red".to_string(), "green".to_string()]);
        assert!(result1.is_ok());

        let result2 = catalog.drop_type("color", false, false);
        assert!(result2.is_ok());

        assert!(catalog.get_enum_type("color").is_none());
    }

    #[test]
    fn test_drop_type_if_exists() {
        let catalog = Catalog::new();
        let result = catalog.drop_type("nonexistent", true, false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_drop_type_not_exists_fails() {
        let catalog = Catalog::new();
        let result = catalog.drop_type("nonexistent", false, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("does not exist"));
    }

    #[test]
    fn test_drop_type_restrict_with_dependent_table() {
        let catalog = Catalog::new();
        let result1 =
            catalog.create_type("color".to_string(), vec!["red".to_string(), "green".to_string()]);
        assert!(result1.is_ok());

        let result2 = catalog.create_table(
            "items".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("color".to_string(), DataType::Enum("color".to_string())),
            ],
        );
        assert!(result2.is_ok());

        let result = catalog.drop_type("color", false, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cannot drop type"));
    }

    #[test]
    fn test_get_enum_label_index() {
        let catalog = Catalog::new();
        let result = catalog.create_type(
            "color".to_string(),
            vec!["red".to_string(), "green".to_string(), "blue".to_string()],
        );
        assert!(result.is_ok());

        assert_eq!(catalog.get_enum_label_index("color", "red"), Some(0));
        assert_eq!(catalog.get_enum_label_index("color", "green"), Some(1));
        assert_eq!(catalog.get_enum_label_index("color", "blue"), Some(2));
        assert_eq!(catalog.get_enum_label_index("color", "yellow"), None);
        assert_eq!(catalog.get_enum_label_index("nonexistent", "red"), None);
    }

    #[test]
    fn test_alter_type_add_value() {
        let catalog = Catalog::new();
        let result1 =
            catalog.create_type("color".to_string(), vec!["red".to_string(), "green".to_string()]);
        assert!(result1.is_ok());

        let result2 = catalog.alter_type_add_value("color", "blue".to_string(), None);
        assert!(result2.is_ok());

        let enum_def = catalog.get_enum_type("color").unwrap();
        assert_eq!(enum_def.labels.len(), 3);
        assert_eq!(enum_def.labels[2], "blue");
    }

    #[test]
    fn test_alter_type_add_value_after() {
        let catalog = Catalog::new();
        let result1 =
            catalog.create_type("color".to_string(), vec!["red".to_string(), "blue".to_string()]);
        assert!(result1.is_ok());

        let result2 =
            catalog.alter_type_add_value("color", "green".to_string(), Some("red".to_string()));
        assert!(result2.is_ok());

        let enum_def = catalog.get_enum_type("color").unwrap();
        assert_eq!(enum_def.labels.len(), 3);
        assert_eq!(enum_def.labels[1], "green");
        assert_eq!(enum_def.labels[2], "blue");
    }

    #[test]
    fn test_alter_type_add_duplicate_label_fails() {
        let catalog = Catalog::new();
        let result1 =
            catalog.create_type("color".to_string(), vec!["red".to_string(), "green".to_string()]);
        assert!(result1.is_ok());

        let result2 = catalog.alter_type_add_value("color", "red".to_string(), None);
        assert!(result2.is_err());
        assert!(result2.unwrap_err().contains("already exists"));
    }

    #[test]
    fn test_alter_type_add_value_nonexistent_type_fails() {
        let catalog = Catalog::new();
        let result = catalog.alter_type_add_value("nonexistent", "value".to_string(), None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("does not exist"));
    }

    #[test]
    fn test_alter_type_add_value_after_nonexistent_label_fails() {
        let catalog = Catalog::new();
        let result1 =
            catalog.create_type("color".to_string(), vec!["red".to_string(), "green".to_string()]);
        assert!(result1.is_ok());

        let result2 = catalog.alter_type_add_value(
            "color",
            "blue".to_string(),
            Some("nonexistent".to_string()),
        );
        assert!(result2.is_err());
        assert!(result2.unwrap_err().contains("does not exist"));
    }

    #[test]
    fn test_enum_value_partial_ord() {
        let enum1 = Value::Enum(EnumValue { type_name: "color".to_string(), index: 0 });
        let enum2 = Value::Enum(EnumValue { type_name: "color".to_string(), index: 1 });
        let enum3 = Value::Enum(EnumValue { type_name: "color".to_string(), index: 2 });

        assert_eq!(enum1.partial_cmp(&enum1), Some(std::cmp::Ordering::Equal));
        assert_eq!(enum1.partial_cmp(&enum2), Some(std::cmp::Ordering::Less));
        assert_eq!(enum2.partial_cmp(&enum1), Some(std::cmp::Ordering::Greater));
        assert_eq!(enum1.partial_cmp(&enum3), Some(std::cmp::Ordering::Less));

        let different_type = Value::Enum(EnumValue { type_name: "size".to_string(), index: 0 });
        assert_eq!(enum1.partial_cmp(&different_type), None);
    }

    #[test]
    fn test_enum_value_ord() {
        let enum1 = Value::Enum(EnumValue { type_name: "color".to_string(), index: 0 });
        let enum2 = Value::Enum(EnumValue { type_name: "color".to_string(), index: 1 });

        assert_eq!(enum1.cmp(&enum1), std::cmp::Ordering::Equal);
        assert_eq!(enum1.cmp(&enum2), std::cmp::Ordering::Less);
        assert_eq!(enum2.cmp(&enum1), std::cmp::Ordering::Greater);
    }

    #[test]
    fn test_enum_type_with_special_characters_in_labels() {
        let catalog = Catalog::new();
        let result = catalog.create_type(
            "status".to_string(),
            vec!["pending".to_string(), "in_progress".to_string(), "done".to_string()],
        );
        assert!(result.is_ok());

        let enum_def = catalog.get_enum_type("status").unwrap();
        assert_eq!(enum_def.labels.len(), 3);
    }

    #[test]
    fn test_enum_value_to_bytes() {
        let enum_val = Value::Enum(EnumValue { type_name: "color".to_string(), index: 1 });
        let bytes = enum_val.to_bytes();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_multiple_enum_types() {
        let catalog = Catalog::new();

        let result1 =
            catalog.create_type("color".to_string(), vec!["red".to_string(), "green".to_string()]);
        assert!(result1.is_ok());

        let result2 = catalog.create_type(
            "size".to_string(),
            vec!["small".to_string(), "medium".to_string(), "large".to_string()],
        );
        assert!(result2.is_ok());

        let color_def = catalog.get_enum_type("color").unwrap();
        let size_def = catalog.get_enum_type("size").unwrap();

        assert_eq!(color_def.labels.len(), 2);
        assert_eq!(size_def.labels.len(), 3);

        assert_eq!(catalog.get_enum_label_index("color", "red"), Some(0));
        assert_eq!(catalog.get_enum_label_index("size", "large"), Some(2));
    }
}
