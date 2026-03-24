use std::sync::Arc;
use vaultgres::catalog::{Catalog, CompositeValue, Value};
use vaultgres::parser::ast::{ColumnDef, DataType, Expr};

fn create_composite_test_catalog() -> Arc<Catalog> {
    let catalog = Catalog::new();

    catalog
        .create_composite_type(
            "address".to_string(),
            vec![
                ("street".to_string(), DataType::Text),
                ("city".to_string(), DataType::Text),
                ("state".to_string(), DataType::Varchar(50)),
                ("zip".to_string(), DataType::Varchar(10)),
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
        .create_composite_type(
            "customer".to_string(),
            vec![
                ("name".to_string(), DataType::Text),
                ("street".to_string(), DataType::Text),
                ("city".to_string(), DataType::Text),
                ("priority".to_string(), DataType::Enum("priority".to_string())),
            ],
        )
        .unwrap();

    catalog
        .create_table(
            "customers".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Serial),
                ColumnDef::new("data".to_string(), DataType::Composite("customer".to_string())),
            ],
        )
        .unwrap();

    catalog
        .insert(
            "customers",
            &["data".to_string()],
            vec![Expr::String("ROW(John Doe, 123 Main St, NYC, medium)".to_string())],
        )
        .unwrap();

    catalog
        .insert(
            "customers",
            &["data".to_string()],
            vec![Expr::String("ROW(Jane Smith, 456 Oak Ave, LA, high)".to_string())],
        )
        .unwrap();

    Arc::new(catalog)
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_create_and_get_composite_type() {
        let catalog = Catalog::new();
        let result = catalog.create_composite_type(
            "test_type".to_string(),
            vec![("field1".to_string(), DataType::Int), ("field2".to_string(), DataType::Text)],
        );
        assert!(result.is_ok());

        let composite_def = catalog.get_composite_type("test_type").unwrap();
        assert_eq!(composite_def.type_name, "test_type");
        assert_eq!(composite_def.fields.len(), 2);
    }

    #[test]
    fn test_table_with_composite_column() {
        let catalog = Catalog::new();

        catalog
            .create_composite_type(
                "address".to_string(),
                vec![("street".to_string(), DataType::Text), ("city".to_string(), DataType::Text)],
            )
            .unwrap();

        catalog
            .create_table(
                "people".to_string(),
                vec![
                    ColumnDef::new("id".to_string(), DataType::Serial),
                    ColumnDef::new(
                        "home_address".to_string(),
                        DataType::Composite("address".to_string()),
                    ),
                ],
            )
            .unwrap();

        let table = catalog.get_table("people").unwrap();
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.columns[1].data_type, DataType::Composite("address".to_string()));
    }

    #[test]
    fn test_insert_and_select_composite_value() {
        let catalog = Catalog::new();

        catalog
            .create_composite_type(
                "address".to_string(),
                vec![("street".to_string(), DataType::Text), ("city".to_string(), DataType::Text)],
            )
            .unwrap();

        catalog
            .create_table(
                "people".to_string(),
                vec![
                    ColumnDef::new("id".to_string(), DataType::Serial),
                    ColumnDef::new(
                        "address".to_string(),
                        DataType::Composite("address".to_string()),
                    ),
                ],
            )
            .unwrap();

        catalog
            .insert(
                "people",
                &["address".to_string()],
                vec![Expr::String("ROW(123 Main St, NYC)".to_string())],
            )
            .unwrap();

        let rows = Catalog::select_with_catalog(
            &Arc::new(catalog),
            "people",
            false,
            vec![Expr::Column("address".to_string())],
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
    fn test_composite_with_multiple_fields() {
        let catalog = Catalog::new();

        catalog
            .create_composite_type(
                "full_address".to_string(),
                vec![
                    ("street".to_string(), DataType::Text),
                    ("city".to_string(), DataType::Text),
                    ("state".to_string(), DataType::Varchar(50)),
                    ("zip".to_string(), DataType::Varchar(10)),
                    ("country".to_string(), DataType::Text),
                ],
            )
            .unwrap();

        catalog
            .create_table(
                "locations".to_string(),
                vec![ColumnDef::new(
                    "address".to_string(),
                    DataType::Composite("full_address".to_string()),
                )],
            )
            .unwrap();

        let result = catalog.insert(
            "locations",
            &["address".to_string()],
            vec![Expr::String("ROW(123 Main St, Springfield, IL, 62701, USA)".to_string())],
        );
        assert!(result.is_ok());

        let rows = Catalog::select_with_catalog(
            &Arc::new(catalog),
            "locations",
            false,
            vec![Expr::Column("address".to_string())],
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
    fn test_composite_with_nested_composite() {
        let catalog = Catalog::new();

        catalog
            .create_composite_type(
                "inner".to_string(),
                vec![("x".to_string(), DataType::Int), ("y".to_string(), DataType::Int)],
            )
            .unwrap();

        catalog
            .create_composite_type(
                "outer".to_string(),
                vec![
                    ("inner_x".to_string(), DataType::Int),
                    ("inner_y".to_string(), DataType::Int),
                    ("name".to_string(), DataType::Text),
                ],
            )
            .unwrap();

        catalog
            .create_table(
                "nested_table".to_string(),
                vec![ColumnDef::new("data".to_string(), DataType::Composite("outer".to_string()))],
            )
            .unwrap();

        let result = catalog.insert(
            "nested_table",
            &["data".to_string()],
            vec![Expr::String("ROW(10, 20, test)".to_string())],
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_composite_with_enum_field() {
        let catalog = Catalog::new();

        catalog
            .create_type(
                "status".to_string(),
                vec!["pending".to_string(), "approved".to_string(), "rejected".to_string()],
            )
            .unwrap();

        catalog
            .create_composite_type(
                "request".to_string(),
                vec![
                    ("title".to_string(), DataType::Text),
                    ("status".to_string(), DataType::Enum("status".to_string())),
                ],
            )
            .unwrap();

        catalog
            .create_table(
                "requests".to_string(),
                vec![ColumnDef::new("req".to_string(), DataType::Composite("request".to_string()))],
            )
            .unwrap();

        let result = catalog.insert(
            "requests",
            &["req".to_string()],
            vec![Expr::String("ROW(Submit request, pending)".to_string())],
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_drop_composite_cascades_to_table() {
        let catalog = Catalog::new();

        catalog
            .create_composite_type("addr".to_string(), vec![("street".to_string(), DataType::Text)])
            .unwrap();

        catalog
            .create_table(
                "people".to_string(),
                vec![ColumnDef::new(
                    "address".to_string(),
                    DataType::Composite("addr".to_string()),
                )],
            )
            .unwrap();

        let result = catalog.drop_type("addr", false, true);
        assert!(result.is_ok());

        assert!(catalog.get_composite_type("addr").is_none());
        assert!(catalog.get_table("people").is_none());
    }

    #[test]
    fn test_drop_composite_blocked_by_table() {
        let catalog = Catalog::new();

        catalog
            .create_composite_type("addr".to_string(), vec![("street".to_string(), DataType::Text)])
            .unwrap();

        catalog
            .create_table(
                "people".to_string(),
                vec![ColumnDef::new(
                    "address".to_string(),
                    DataType::Composite("addr".to_string()),
                )],
            )
            .unwrap();

        let result = catalog.drop_type("addr", false, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("used by table column"));
    }

    #[test]
    fn test_composite_multiple_rows() {
        let catalog = create_composite_test_catalog();

        let rows = Catalog::select_with_catalog(
            &catalog,
            "customers",
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
    fn test_composite_type_equality() {
        let val1 = Value::Composite(CompositeValue {
            type_name: "test".to_string(),
            fields: vec![
                ("a".to_string(), Value::Int(1)),
                ("b".to_string(), Value::Text("hello".to_string())),
            ],
        });

        let val2 = Value::Composite(CompositeValue {
            type_name: "test".to_string(),
            fields: vec![
                ("a".to_string(), Value::Int(1)),
                ("b".to_string(), Value::Text("hello".to_string())),
            ],
        });

        let val3 = Value::Composite(CompositeValue {
            type_name: "test".to_string(),
            fields: vec![
                ("a".to_string(), Value::Int(2)),
                ("b".to_string(), Value::Text("hello".to_string())),
            ],
        });

        assert_eq!(val1, val2);
        assert_ne!(val1, val3);
    }

    #[test]
    fn test_composite_type_ordering() {
        let val1 = Value::Composite(CompositeValue {
            type_name: "test".to_string(),
            fields: vec![("a".to_string(), Value::Int(1))],
        });

        let val2 = Value::Composite(CompositeValue {
            type_name: "test".to_string(),
            fields: vec![("a".to_string(), Value::Int(2))],
        });

        let val3 = Value::Composite(CompositeValue {
            type_name: "test".to_string(),
            fields: vec![("a".to_string(), Value::Int(1))],
        });

        assert!(val1 < val2);
        assert!(val2 > val1);
        assert_eq!(val1, val3);
    }

    #[test]
    fn test_composite_type_with_decimal_field() {
        let catalog = Catalog::new();

        catalog
            .create_composite_type(
                "price_info".to_string(),
                vec![
                    ("amount".to_string(), DataType::Decimal(10, 2)),
                    ("currency".to_string(), DataType::Text),
                ],
            )
            .unwrap();

        catalog
            .create_table(
                "products".to_string(),
                vec![ColumnDef::new(
                    "price".to_string(),
                    DataType::Composite("price_info".to_string()),
                )],
            )
            .unwrap();

        let result = catalog.insert(
            "products",
            &["price".to_string()],
            vec![Expr::String("ROW(19.99, USD)".to_string())],
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_composite_type_with_boolean_field() {
        let catalog = Catalog::new();

        catalog
            .create_composite_type(
                "config".to_string(),
                vec![
                    ("enabled".to_string(), DataType::Boolean),
                    ("name".to_string(), DataType::Text),
                ],
            )
            .unwrap();

        catalog
            .create_table(
                "settings".to_string(),
                vec![ColumnDef::new(
                    "config".to_string(),
                    DataType::Composite("config".to_string()),
                )],
            )
            .unwrap();

        let result = catalog.insert(
            "settings",
            &["config".to_string()],
            vec![Expr::String("ROW(true, feature_x)".to_string())],
        );
        assert!(result.is_ok());
    }
}
