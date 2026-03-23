#[cfg(test)]
mod tests {
    use crate::catalog::value::Value;
    use crate::parser::ast::DataType;

    #[test]
    fn test_json_value_creation() {
        let json = Value::Json(r#"{"name": "John", "age": 30}"#.to_string());
        assert_eq!(format!("{}", json), r#"{"name": "John", "age": 30}"#);
    }

    #[test]
    fn test_json_value_comparison() {
        let json1 = Value::Json(r#"{"a": 1}"#.to_string());
        let json2 = Value::Json(r#"{"a": 2}"#.to_string());
        let json3 = Value::Json(r#"{"a": 1}"#.to_string());
        assert!(json1 < json2);
        assert_eq!(json1, json3);
    }

    #[test]
    fn test_json_value_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn calculate_hash<T: Hash>(t: &T) -> u64 {
            let mut s = DefaultHasher::new();
            t.hash(&mut s);
            s.finish()
        }

        let json1 = Value::Json(r#"{"key": "value"}"#.to_string());
        let json2 = Value::Json(r#"{"key": "value"}"#.to_string());
        let json3 = Value::Json(r#"{"key": "other"}"#.to_string());
        assert_eq!(calculate_hash(&json1), calculate_hash(&json2));
        assert_ne!(calculate_hash(&json1), calculate_hash(&json3));
    }

    #[test]
    fn test_json_value_to_bytes() {
        let json = Value::Json(r#"{"key": "value"}"#.to_string());
        let bytes = json.to_bytes();
        assert_eq!(bytes, b"JSON".to_vec());
    }

    #[test]
    fn test_json_value_display() {
        let json = Value::Json(r#"{"name": "Alice"}"#.to_string());
        assert_eq!(format!("{}", json), r#"{"name": "Alice"}"#);
    }

    #[test]
    fn test_datatype_json() {
        let dt = DataType::Json;
        assert_eq!(dt, DataType::Json);
    }

    #[test]
    fn test_datatype_jsonb() {
        let dt = DataType::Jsonb;
        assert_eq!(dt, DataType::Jsonb);
    }

    #[test]
    fn test_json_and_jsonb_are_different_types() {
        assert_ne!(DataType::Json, DataType::Jsonb);
    }

    #[test]
    fn test_json_null_handling() {
        let json = Value::Json(r#"{"key": null}"#.to_string());
        assert!(json.to_string().contains("null"));
    }

    #[test]
    fn test_json_nested_objects() {
        let json = Value::Json(r#"{"outer": {"inner": "value"}}"#.to_string());
        assert!(json.to_string().contains("outer"));
        assert!(json.to_string().contains("inner"));
    }

    #[test]
    fn test_json_arrays() {
        let json = Value::Json(r#"[1, 2, 3]"#.to_string());
        assert!(json.to_string().contains("[1, 2, 3]"));
    }

    #[test]
    fn test_json_string_with_escapes() {
        let json = Value::Json(r#"{"message": "Hello\nWorld"}"#.to_string());
        assert!(json.to_string().contains("\\n"));
    }

    #[test]
    fn test_json_boolean_values() {
        let json = Value::Json(r#"{"active": true, "deleted": false}"#.to_string());
        assert!(json.to_string().contains("true"));
        assert!(json.to_string().contains("false"));
    }

    #[test]
    fn test_json_number_values() {
        let json = Value::Json(r#"{"count": 42, "price": 3.14}"#.to_string());
        assert!(json.to_string().contains("42"));
        assert!(json.to_string().contains("3.14"));
    }

    #[test]
    fn test_empty_json_object() {
        let json = Value::Json("{}".to_string());
        assert_eq!(json.to_string(), "{}");
    }

    #[test]
    fn test_empty_json_array() {
        let json = Value::Json("[]".to_string());
        assert_eq!(json.to_string(), "[]");
    }
}
