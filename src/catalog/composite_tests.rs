use crate::catalog::{Catalog, CompositeTypeDef, Value};
use crate::parser::ast::{ColumnDef, DataType, Expr, TypeKind};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_composite_type() {
        let catalog = Catalog::new();
        let result = catalog.create_composite_type(
            "address".to_string(),
            vec![
                ("street".to_string(), DataType::Text),
                ("city".to_string(), DataType::Text),
                ("zip".to_string(), DataType::Varchar(10)),
            ],
        );
        assert!(result.is_ok());

        let composite_def = catalog.get_composite_type("address").unwrap();
        assert_eq!(composite_def.type_name, "address");
        assert_eq!(composite_def.fields.len(), 3);
        assert_eq!(composite_def.fields[0].0, "street");
        assert_eq!(composite_def.fields[1].0, "city");
        assert_eq!(composite_def.fields[2].0, "zip");
    }

    #[test]
    fn test_create_composite_type_empty_fields_fails() {
        let catalog = Catalog::new();
        let result = catalog.create_composite_type("empty_type".to_string(), vec![]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("at least one field"));
    }

    #[test]
    fn test_create_composite_type_duplicate_fields_fails() {
        let catalog = Catalog::new();
        let result = catalog.create_composite_type(
            "bad_type".to_string(),
            vec![("name".to_string(), DataType::Text), ("name".to_string(), DataType::Int)],
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("duplicate field names"));
    }

    #[test]
    fn test_create_composite_type_duplicate_name_fails() {
        let catalog = Catalog::new();
        let result1 = catalog.create_composite_type(
            "address".to_string(),
            vec![("street".to_string(), DataType::Text)],
        );
        assert!(result1.is_ok());

        let result2 = catalog.create_composite_type(
            "address".to_string(),
            vec![("name".to_string(), DataType::Text)],
        );
        assert!(result2.is_err());
        assert!(result2.unwrap_err().contains("already exists"));
    }

    #[test]
    fn test_create_composite_and_enum_same_name_fails() {
        let catalog = Catalog::new();
        let result1 = catalog.create_type("status".to_string(), vec!["active".to_string()]);
        assert!(result1.is_ok());

        let result2 = catalog.create_composite_type(
            "status".to_string(),
            vec![("name".to_string(), DataType::Text)],
        );
        assert!(result2.is_err());
        assert!(result2.unwrap_err().contains("already exists as enum"));
    }

    #[test]
    fn test_create_enum_and_composite_same_name_fails() {
        let catalog = Catalog::new();
        let result1 = catalog.create_composite_type(
            "status".to_string(),
            vec![("name".to_string(), DataType::Text)],
        );
        assert!(result1.is_ok());

        let result2 = catalog.create_type("status".to_string(), vec!["active".to_string()]);
        assert!(result2.is_err());
        assert!(result2.unwrap_err().contains("already exists as composite"));
    }

    #[test]
    fn test_drop_composite_type_success() {
        let catalog = Catalog::new();
        let result1 = catalog.create_composite_type(
            "address".to_string(),
            vec![("street".to_string(), DataType::Text)],
        );
        assert!(result1.is_ok());

        let result2 = catalog.drop_type("address", false, false);
        assert!(result2.is_ok());

        assert!(catalog.get_composite_type("address").is_none());
    }

    #[test]
    fn test_drop_composite_type_if_exists() {
        let catalog = Catalog::new();
        let result = catalog.drop_type("nonexistent", true, false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_drop_composite_type_not_exists() {
        let catalog = Catalog::new();
        let result = catalog.drop_type("nonexistent", false, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("does not exist"));
    }

    #[test]
    fn test_drop_composite_type_with_dependent_table_fails() {
        let catalog = Catalog::new();
        let result1 = catalog.create_composite_type(
            "address".to_string(),
            vec![("street".to_string(), DataType::Text)],
        );
        assert!(result1.is_ok());

        let result2 = catalog.create_table(
            "people".to_string(),
            vec![ColumnDef::new("addr".to_string(), DataType::Composite("address".to_string()))],
        );
        assert!(result2.is_ok());

        let result3 = catalog.drop_type("address", false, false);
        assert!(result3.is_err());
        assert!(result3.unwrap_err().contains("used by table column"));
    }

    #[test]
    fn test_drop_composite_type_with_cascade() {
        let catalog = Catalog::new();
        let result1 = catalog.create_composite_type(
            "address".to_string(),
            vec![("street".to_string(), DataType::Text)],
        );
        assert!(result1.is_ok());

        let result2 = catalog.create_table(
            "people".to_string(),
            vec![ColumnDef::new("addr".to_string(), DataType::Composite("address".to_string()))],
        );
        assert!(result2.is_ok());

        let result3 = catalog.drop_type("address", false, true);
        assert!(result3.is_ok());

        assert!(catalog.get_composite_type("address").is_none());
        assert!(catalog.get_table("people").is_none());
    }

    #[test]
    fn test_composite_type_with_nested_composite() {
        let catalog = Catalog::new();

        let result1 = catalog.create_composite_type(
            "inner_type".to_string(),
            vec![("x".to_string(), DataType::Int)],
        );
        assert!(result1.is_ok());

        let result2 = catalog.create_composite_type(
            "outer_type".to_string(),
            vec![("inner".to_string(), DataType::Composite("inner_type".to_string()))],
        );
        assert!(result2.is_ok());

        let outer_def = catalog.get_composite_type("outer_type").unwrap();
        assert_eq!(outer_def.fields.len(), 1);
        assert_eq!(outer_def.fields[0].0, "inner");
        assert_eq!(outer_def.fields[0].1, DataType::Composite("inner_type".to_string()));
    }

    #[test]
    fn test_composite_type_with_enum_field() {
        let catalog = Catalog::new();

        let result1 =
            catalog.create_type("mood".to_string(), vec!["happy".to_string(), "sad".to_string()]);
        assert!(result1.is_ok());

        let result2 = catalog.create_composite_type(
            "person".to_string(),
            vec![
                ("name".to_string(), DataType::Text),
                ("disposition".to_string(), DataType::Enum("mood".to_string())),
            ],
        );
        assert!(result2.is_ok());

        let person_def = catalog.get_composite_type("person").unwrap();
        assert_eq!(person_def.fields.len(), 2);
        assert_eq!(person_def.fields[1].0, "disposition");
        assert_eq!(person_def.fields[1].1, DataType::Enum("mood".to_string()));
    }

    #[test]
    fn test_composite_type_with_array_field() {
        let catalog = Catalog::new();

        let result = catalog.create_composite_type(
            "multi_value".to_string(),
            vec![("values".to_string(), DataType::Array(Box::new(DataType::Int)))],
        );
        assert!(result.is_ok());

        let def = catalog.get_composite_type("multi_value").unwrap();
        assert_eq!(def.fields.len(), 1);
        assert_eq!(def.fields[0].0, "values");
        assert_eq!(def.fields[0].1, DataType::Array(Box::new(DataType::Int)));
    }

    #[test]
    fn test_multiple_composite_types() {
        let catalog = Catalog::new();

        let result1 = catalog.create_composite_type(
            "address".to_string(),
            vec![("street".to_string(), DataType::Text)],
        );
        assert!(result1.is_ok());

        let result2 = catalog.create_composite_type(
            "phone".to_string(),
            vec![("number".to_string(), DataType::Varchar(20))],
        );
        assert!(result2.is_ok());

        assert!(catalog.get_composite_type("address").is_some());
        assert!(catalog.get_composite_type("phone").is_some());
    }

    #[test]
    fn test_composite_type_case_sensitive() {
        let catalog = Catalog::new();

        let result1 = catalog.create_composite_type(
            "Address".to_string(),
            vec![("Street".to_string(), DataType::Text)],
        );
        assert!(result1.is_ok());

        assert!(catalog.get_composite_type("address").is_none());
        assert!(catalog.get_composite_type("Address").is_some());
    }

    #[test]
    fn test_composite_type_insert_validation() {
        let catalog = Catalog::new();

        let result1 = catalog.create_composite_type(
            "address".to_string(),
            vec![("street".to_string(), DataType::Text), ("city".to_string(), DataType::Text)],
        );
        assert!(result1.is_ok());

        let result2 = catalog.create_table(
            "people".to_string(),
            vec![ColumnDef::new("addr".to_string(), DataType::Composite("address".to_string()))],
        );
        assert!(result2.is_ok());

        let result3 = catalog.insert(
            "people",
            &["addr".to_string()],
            vec![Expr::String("ROW(123 Main St, NYC)".to_string())],
        );
        assert!(result3.is_ok());
    }

    #[test]
    fn test_composite_type_insert_wrong_field_count() {
        let catalog = Catalog::new();

        let result1 = catalog.create_composite_type(
            "address".to_string(),
            vec![("street".to_string(), DataType::Text), ("city".to_string(), DataType::Text)],
        );
        assert!(result1.is_ok());

        let result2 = catalog.create_table(
            "people".to_string(),
            vec![ColumnDef::new("addr".to_string(), DataType::Composite("address".to_string()))],
        );
        assert!(result2.is_ok());

        let result3 = catalog.insert(
            "people",
            &["addr".to_string()],
            vec![Expr::String("ROW(only one)".to_string())],
        );
        assert!(result3.is_err());
    }

    #[test]
    fn test_composite_type_insert_null_field() {
        let catalog = Catalog::new();

        let result1 = catalog.create_composite_type(
            "address".to_string(),
            vec![("street".to_string(), DataType::Text), ("city".to_string(), DataType::Text)],
        );
        assert!(result1.is_ok());

        let result2 = catalog.create_table(
            "people".to_string(),
            vec![ColumnDef::new("addr".to_string(), DataType::Composite("address".to_string()))],
        );
        assert!(result2.is_ok());

        let result3 = catalog.insert("people", &["addr".to_string()], vec![Expr::Null]);
        assert!(result3.is_ok());
    }
}
