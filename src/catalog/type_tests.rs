#[cfg(test)]
mod type_tests {
    use crate::catalog::Catalog;
    use crate::parser::ast::{ColumnDef, DataType};

    #[test]
    fn test_create_and_get_enum_type() {
        let catalog = Catalog::new();
        let type_name = "mood";
        let labels = vec!["sad".to_string(), "happy".to_string()];
        assert!(catalog.create_type(type_name.to_string(), labels.clone()).is_ok());

        let enum_type = catalog.get_enum_type(type_name).unwrap();
        assert_eq!(enum_type.type_name, type_name);
        assert_eq!(enum_type.labels, labels);
    }

    #[test]
    fn test_create_and_get_composite_type() {
        let catalog = Catalog::new();
        let type_name = "address";
        let fields =
            vec![("street".to_string(), DataType::Text), ("city".to_string(), DataType::Text)];
        assert!(catalog.create_composite_type(type_name.to_string(), fields.clone()).is_ok());

        let composite_type = catalog.get_composite_type(type_name).unwrap();
        assert_eq!(composite_type.type_name, type_name);
        assert_eq!(composite_type.fields, fields);
    }

    #[test]
    fn test_drop_type() {
        let catalog = Catalog::new();
        let enum_name = "color";
        assert!(catalog.create_type(enum_name.to_string(), vec!["red".to_string()]).is_ok());
        assert!(catalog.drop_type(enum_name, false, false).is_ok());
        assert!(catalog.get_enum_type(enum_name).is_none());

        let composite_name = "person";
        assert!(
            catalog
                .create_composite_type(
                    composite_name.to_string(),
                    vec![("name".to_string(), DataType::Text)]
                )
                .is_ok()
        );
        assert!(catalog.drop_type(composite_name, false, false).is_ok());
        assert!(catalog.get_composite_type(composite_name).is_none());
    }

    #[test]
    fn test_drop_type_with_dependency() {
        let catalog = Catalog::new();
        let type_name = "status";
        assert!(catalog.create_type(type_name.to_string(), vec!["active".to_string()]).is_ok());

        let columns = vec![ColumnDef::new(
            "current_status".to_string(),
            DataType::Enum(type_name.to_string()),
        )];
        assert!(catalog.create_table("tasks".to_string(), columns).is_ok());

        // Should fail without cascade
        assert!(catalog.drop_type(type_name, false, false).is_err());

        // Should succeed with cascade
        assert!(catalog.drop_type(type_name, false, true).is_ok());
        assert!(catalog.get_enum_type(type_name).is_none());
        assert!(catalog.get_table("tasks").is_none());
    }

    #[test]
    fn test_alter_type_add_value() {
        let catalog = Catalog::new();
        let type_name = "feeling";
        let labels = vec!["good".to_string(), "bad".to_string()];
        assert!(catalog.create_type(type_name.to_string(), labels).is_ok());

        // Add a new value at the end
        assert!(catalog.alter_type_add_value(type_name, "neutral".to_string(), None).is_ok());
        let enum_type = catalog.get_enum_type(type_name).unwrap();
        assert_eq!(enum_type.labels, vec!["good", "bad", "neutral"]);

        // Add a new value after an existing one
        assert!(
            catalog
                .alter_type_add_value(type_name, "awesome".to_string(), Some("good".to_string()))
                .is_ok()
        );
        let enum_type_after = catalog.get_enum_type(type_name).unwrap();
        assert_eq!(enum_type_after.labels, vec!["good", "awesome", "bad", "neutral"]);
    }
}
