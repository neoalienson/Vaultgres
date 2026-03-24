use std::sync::Arc;
use vaultgres::catalog::{Catalog, Range, Value};
use vaultgres::parser::ast::{ColumnDef, DataType, Expr};

fn setup_catalog_with_ranges() -> Catalog {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("price_range".to_string(), DataType::NumRange),
        ColumnDef::new("date_range".to_string(), DataType::DateRange),
    ];
    catalog.create_table("products".to_string(), columns).unwrap();
    catalog
}

fn create_int4range_catalog() -> Catalog {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("qty_range".to_string(), DataType::Int4Range),
    ];
    catalog.create_table("inventory".to_string(), columns).unwrap();
    catalog
}

#[test]
fn test_datatype_int4range() {
    let dt = DataType::Int4Range;
    assert_eq!(format!("{:?}", dt), "Int4Range");
}

#[test]
fn test_datatype_int8range() {
    let dt = DataType::Int8Range;
    assert_eq!(format!("{:?}", dt), "Int8Range");
}

#[test]
fn test_datatype_numrange() {
    let dt = DataType::NumRange;
    assert_eq!(format!("{:?}", dt), "NumRange");
}

#[test]
fn test_datatype_daterange() {
    let dt = DataType::DateRange;
    assert_eq!(format!("{:?}", dt), "DateRange");
}

#[test]
fn test_datatype_tsrange() {
    let dt = DataType::TsRange;
    assert_eq!(format!("{:?}", dt), "TsRange");
}

#[test]
fn test_datatype_tstzrange() {
    let dt = DataType::TsTzRange;
    assert_eq!(format!("{:?}", dt), "TsTzRange");
}

#[test]
fn test_value_range_creation() {
    let range = Range::new(Some(Value::Int(1)), true, Some(Value::Int(10)), true);
    assert_eq!(range.lower_bound(), Some(&Value::Int(1)));
    assert_eq!(range.upper_bound(), Some(&Value::Int(10)));
    assert!(range.lower_inclusive());
    assert!(range.upper_inclusive());
}

#[test]
fn test_value_range_exclusive_bounds() {
    let range = Range::new(Some(Value::Int(1)), false, Some(Value::Int(10)), false);
    assert!(!range.lower_inclusive());
    assert!(!range.upper_inclusive());
}

#[test]
fn test_value_range_with_null_lower() {
    let range = Range::new(None, true, Some(Value::Int(10)), true);
    assert!(range.lower_bound().is_none());
    assert!(range.upper_bound().is_some());
}

#[test]
fn test_value_range_with_null_upper() {
    let range = Range::new(Some(Value::Int(1)), true, None, true);
    assert!(range.lower_bound().is_some());
    assert!(range.upper_bound().is_none());
}

#[test]
fn test_value_range_empty() {
    let range = Range::empty();
    assert!(range.lower_bound().is_none());
    assert!(range.upper_bound().is_none());
}

#[test]
fn test_value_range_is_empty() {
    let range = Range::new(Some(Value::Int(10)), true, Some(Value::Int(5)), true);
    assert!(range.is_empty());
}

#[test]
fn test_value_range_not_empty() {
    let range = Range::new(Some(Value::Int(1)), true, Some(Value::Int(10)), true);
    assert!(!range.is_empty());
}

#[test]
fn test_value_range_display_inclusive() {
    let range = Range::new(Some(Value::Int(1)), true, Some(Value::Int(5)), true);
    assert_eq!(format!("{}", range), "[1,5]");
}

#[test]
fn test_value_range_display_exclusive() {
    let range = Range::new(Some(Value::Int(1)), false, Some(Value::Int(5)), false);
    assert_eq!(format!("{}", range), "(1,5)");
}

#[test]
fn test_value_range_display_mixed_bounds() {
    let range = Range::new(Some(Value::Int(1)), true, Some(Value::Int(5)), false);
    assert_eq!(format!("{}", range), "[1,5)");
}

#[test]
fn test_value_range_display_with_nulls() {
    let range = Range::new(None, true, Some(Value::Int(5)), true);
    assert_eq!(format!("{}", range), "(,5]");
}

#[test]
fn test_catalog_create_table_with_int4range() {
    let catalog = create_int4range_catalog();
    let schema = catalog.get_table("inventory").unwrap();
    assert_eq!(schema.columns.len(), 2);
    assert_eq!(schema.columns[0].name, "id");
    assert_eq!(schema.columns[1].name, "qty_range");
}

#[test]
fn test_catalog_create_table_with_numrange() {
    let catalog = setup_catalog_with_ranges();
    let schema = catalog.get_table("products").unwrap();
    assert_eq!(schema.columns.len(), 3);
    assert_eq!(schema.columns[1].data_type, DataType::NumRange);
}

#[test]
fn test_catalog_create_table_with_daterange() {
    let catalog = setup_catalog_with_ranges();
    let schema = catalog.get_table("products").unwrap();
    assert_eq!(schema.columns.len(), 3);
    assert_eq!(schema.columns[2].data_type, DataType::DateRange);
}

#[test]
fn test_range_value_clone() {
    let range1 = Range::new(Some(Value::Int(1)), true, Some(Value::Int(5)), true);
    let range2 = range1.clone();
    assert_eq!(range1.lower_bound(), range2.lower_bound());
    assert_eq!(range1.upper_bound(), range2.upper_bound());
}

#[test]
fn test_range_value_serialize_deserialize() {
    use vaultgres::catalog::RangeBound;
    let range = Range::new(Some(Value::Int(1)), true, Some(Value::Int(5)), true);
    let json = serde_json::to_string(&range).unwrap();
    let deserialized: Range = serde_json::from_str(&json).unwrap();
    assert_eq!(range.lower_bound(), deserialized.lower_bound());
    assert_eq!(range.upper_bound(), deserialized.upper_bound());
}
