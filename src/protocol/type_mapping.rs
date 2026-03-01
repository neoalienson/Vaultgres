use crate::catalog::Value;

/// PostgreSQL type OIDs
pub mod pg_types {
    pub const BOOL: i32 = 16;
    pub const INT8: i32 = 20;
    pub const INT2: i32 = 21;
    pub const INT4: i32 = 23;
    pub const TEXT: i32 = 25;
    pub const FLOAT4: i32 = 700;
    pub const FLOAT8: i32 = 701;
    pub const VARCHAR: i32 = 1043;
}

/// Map RustGres Value to PostgreSQL type OID and size
pub fn value_to_pg_type(value: &Value) -> (i32, i16) {
    match value {
        Value::Int(_) => (pg_types::INT8, 8),
        Value::Float(_) => (pg_types::FLOAT8, 8),
        Value::Bool(_) => (pg_types::BOOL, 1),
        Value::Text(_) => (pg_types::TEXT, -1),
        Value::Json(_) => (pg_types::TEXT, -1),
        Value::Array(_) => (pg_types::TEXT, -1),
        Value::Null => (pg_types::TEXT, -1),
    }
}

/// Serialize value to wire format (text representation)
pub fn serialize_value(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::Int(i) => Some(i.to_string().into_bytes()),
        Value::Float(f) => Some(f.to_string().into_bytes()),
        Value::Bool(b) => Some(if *b { b"t" } else { b"f" }.to_vec()),
        Value::Text(s) => Some(s.as_bytes().to_vec()),
        Value::Json(j) => Some(j.as_bytes().to_vec()),
        Value::Array(_) => Some(b"[]".to_vec()), // Simplified
        Value::Null => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_serialization() {
        let value = Value::Int(42);
        let serialized = serialize_value(&value);
        assert_eq!(serialized, Some(b"42".to_vec()));
    }

    #[test]
    fn test_negative_integer() {
        let value = Value::Int(-100);
        let serialized = serialize_value(&value);
        assert_eq!(serialized, Some(b"-100".to_vec()));
    }

    #[test]
    fn test_text_serialization() {
        let value = Value::Text("hello".to_string());
        let serialized = serialize_value(&value);
        assert_eq!(serialized, Some(b"hello".to_vec()));
    }

    #[test]
    fn test_empty_text() {
        let value = Value::Text("".to_string());
        let serialized = serialize_value(&value);
        assert_eq!(serialized, Some(b"".to_vec()));
    }

    #[test]
    fn test_null_serialization() {
        let value = Value::Null;
        let serialized = serialize_value(&value);
        assert_eq!(serialized, None);
    }

    #[test]
    fn test_bool_serialization() {
        assert_eq!(serialize_value(&Value::Bool(true)), Some(b"t".to_vec()));
        assert_eq!(serialize_value(&Value::Bool(false)), Some(b"f".to_vec()));
    }

    #[test]
    fn test_float_serialization() {
        let value = Value::Float(std::f64::consts::PI);
        let serialized = serialize_value(&value);
        assert!(serialized.is_some());
        assert!(String::from_utf8_lossy(&serialized.unwrap()).starts_with("3.14"));
    }

    #[test]
    fn test_type_mapping_int() {
        let value = Value::Int(42);
        let (oid, size) = value_to_pg_type(&value);
        assert_eq!(oid, pg_types::INT8);
        assert_eq!(size, 8);
    }

    #[test]
    fn test_type_mapping_text() {
        let value = Value::Text("test".to_string());
        let (oid, size) = value_to_pg_type(&value);
        assert_eq!(oid, pg_types::TEXT);
        assert_eq!(size, -1);
    }

    #[test]
    fn test_type_mapping_bool() {
        let value = Value::Bool(true);
        let (oid, size) = value_to_pg_type(&value);
        assert_eq!(oid, pg_types::BOOL);
        assert_eq!(size, 1);
    }

    #[test]
    fn test_type_mapping_null() {
        let value = Value::Null;
        let (oid, size) = value_to_pg_type(&value);
        assert_eq!(oid, pg_types::TEXT);
        assert_eq!(size, -1);
    }
}
