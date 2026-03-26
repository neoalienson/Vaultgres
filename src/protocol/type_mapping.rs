use crate::catalog::{EnumTypeDef, Value};
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

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
    pub const ENUM: i32 = 3500;
    pub const COMPOSITE: i32 = 3501;
}

/// Map VaultGres Value to PostgreSQL type OID and size
pub fn value_to_pg_type(value: &Value) -> (i32, i16) {
    match value {
        Value::Int(_) => (pg_types::INT8, 8),
        Value::Float(_) => (pg_types::FLOAT8, 8),
        Value::Bool(_) => (pg_types::BOOL, 1),
        Value::Text(_) => (pg_types::TEXT, -1),
        Value::Json(_) => (pg_types::TEXT, -1),
        Value::Array(_) => (pg_types::TEXT, -1),
        Value::Date(_) => (pg_types::INT4, 4),
        Value::Time(_) => (pg_types::INT8, 8),
        Value::Timestamp(_) => (pg_types::INT8, 8),
        Value::Decimal(_, _) => (pg_types::TEXT, -1),
        Value::Bytea(_) => (pg_types::TEXT, -1),
        Value::Enum(_) => (pg_types::ENUM, 4),
        Value::Composite(_) => (pg_types::COMPOSITE, -1),
        Value::Range(_) => (pg_types::TEXT, -1),
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
        Value::Array(_) => Some(b"[]".to_vec()),
        Value::Date(d) => {
            let epoch = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
            let date = epoch + Duration::days(*d as i64);
            Some(date.format("%Y-%m-%d").to_string().into_bytes())
        }
        Value::Time(t) => {
            let time = NaiveTime::from_hms_micro_opt(
                (*t / 3_600_000_000) as u32,                // hours
                ((*t % 3_600_000_000) / 60_000_000) as u32, // minutes
                ((*t % 60_000_000) / 1_000_000) as u32,     // seconds
                (*t % 1_000_000) as u32,                    // microseconds
            )
            .unwrap();
            Some(time.format("%H:%M:%S.%6f").to_string().into_bytes())
        }
        Value::Timestamp(ts) => {
            let epoch_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
            let epoch_time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
            let epoch = NaiveDateTime::new(epoch_date, epoch_time);
            let timestamp = epoch + Duration::microseconds(*ts);
            Some(timestamp.format("%Y-%m-%dT%H:%M:%S").to_string().into_bytes())
        }
        Value::Decimal(v, s) => {
            let scale_factor = 10_i128.pow(*s as u32);
            Some(
                format!(
                    "{}.{:0>width$}",
                    v / scale_factor,
                    (v % scale_factor).abs(),
                    width = *s as usize
                )
                .into_bytes(),
            )
        }
        Value::Bytea(b) => {
            let hex_str = b.iter().map(|byte| format!("{:02x}", byte)).collect::<String>();
            Some(format!("\\x{}", hex_str).into_bytes())
        }
        Value::Enum(e) => Some(format!("{}[{}]", e.type_name, e.index).into_bytes()),
        Value::Composite(c) => {
            let field_values: Vec<String> =
                c.fields.iter().map(|(_, v)| format!("{}", v)).collect();
            Some(format!("({})", field_values.join(", ")).into_bytes())
        }
        Value::Range(r) => Some(format!("{}", r).into_bytes()),
        Value::Null => None,
    }
}

/// Serialize value to wire format with enum labels resolved
pub fn serialize_value_with_enum_types(
    value: &Value,
    enum_types: &Arc<RwLock<HashMap<String, EnumTypeDef>>>,
) -> Option<Vec<u8>> {
    match value {
        Value::Enum(e) => {
            if let Ok(types) = enum_types.read() {
                if let Some(def) = types.get(&e.type_name) {
                    if let Some(label) = def.labels.get(e.index as usize) {
                        return Some(label.as_bytes().to_vec());
                    }
                }
            }
            Some(format!("{}[{}]", e.type_name, e.index).into_bytes())
        }
        _ => serialize_value(value),
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

    #[test]
    fn test_type_mapping_float() {
        let value = Value::Float(std::f64::consts::PI);
        let (oid, size) = value_to_pg_type(&value);
        assert_eq!(oid, pg_types::FLOAT8);
        assert_eq!(size, 8);
    }

    #[test]
    fn test_type_mapping_json() {
        let value = Value::Json("{}".to_string());
        let (oid, size) = value_to_pg_type(&value);
        assert_eq!(oid, pg_types::TEXT);
        assert_eq!(size, -1);
    }

    #[test]
    fn test_type_mapping_array() {
        let value = Value::Array(vec![]);
        let (oid, size) = value_to_pg_type(&value);
        assert_eq!(oid, pg_types::TEXT);
        assert_eq!(size, -1);
    }

    #[test]
    fn test_type_mapping_date() {
        let value = Value::Date(Default::default());
        let (oid, size) = value_to_pg_type(&value);
        assert_eq!(oid, pg_types::INT4);
        assert_eq!(size, 4);
    }

    #[test]
    fn test_type_mapping_time() {
        let value = Value::Time(Default::default());
        let (oid, size) = value_to_pg_type(&value);
        assert_eq!(oid, pg_types::INT8);
        assert_eq!(size, 8);
    }

    #[test]
    fn test_type_mapping_timestamp() {
        let value = Value::Timestamp(Default::default());
        let (oid, size) = value_to_pg_type(&value);
        assert_eq!(oid, pg_types::INT8);
        assert_eq!(size, 8);
    }

    #[test]
    fn test_type_mapping_decimal() {
        let value = Value::Decimal(12345, 2);
        let (oid, size) = value_to_pg_type(&value);
        assert_eq!(oid, pg_types::TEXT);
        assert_eq!(size, -1);
    }

    #[test]
    fn test_type_mapping_bytea() {
        let value = Value::Bytea(vec![1, 2, 3]);
        let (oid, size) = value_to_pg_type(&value);
        assert_eq!(oid, pg_types::TEXT);
        assert_eq!(size, -1);
    }

    #[test]
    fn test_json_serialization() {
        let value = Value::Json(r#"{"key": "value"}"#.to_string());
        let serialized = serialize_value(&value);
        assert_eq!(serialized, Some(br#"{"key": "value"}"#.to_vec()));
    }

    #[test]
    fn test_empty_json_serialization() {
        let value = Value::Json("{}".to_string());
        let serialized = serialize_value(&value);
        assert_eq!(serialized, Some(b"{}".to_vec()));
    }

    #[test]
    fn test_array_serialization() {
        let value = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let serialized = serialize_value(&value);
        // Current implementation for Array is always "[]"
        assert_eq!(serialized, Some(b"[]".to_vec()));
    }

    #[test]
    fn test_empty_array_serialization() {
        let value = Value::Array(vec![]);
        let serialized = serialize_value(&value);
        assert_eq!(serialized, Some(b"[]".to_vec()));
    }

    #[test]
    fn test_date_serialization() {
        let date_val = 8401; // 2023-01-01 is 8401 days from 2000-01-01
        let value = Value::Date(date_val);
        let serialized = serialize_value(&value);
        assert_eq!(serialized, Some(b"2023-01-01".to_vec()));
    }

    #[test]
    fn test_time_serialization() {
        let time_val = 45045000000; // 12:30:45 as microseconds from midnight
        let value = Value::Time(time_val);
        let serialized = serialize_value(&value);
        assert_eq!(serialized, Some(b"12:30:45.000000".to_vec()));
    }

    #[test]
    fn test_timestamp_serialization() {
        let timestamp_val = 725846400000000; // 2023-01-01T00:00:00 from 2000-01-01T00:00:00 UTC in microseconds
        let value = Value::Timestamp(timestamp_val);
        let serialized = serialize_value(&value);
        assert_eq!(serialized, Some(b"2023-01-01T00:00:00".to_vec()));
    }

    #[test]
    fn test_decimal_serialization_positive() {
        let value = Value::Decimal(12345, 2); // Represents 123.45
        let serialized = serialize_value(&value);
        assert_eq!(serialized, Some(b"123.45".to_vec()));
    }

    #[test]
    fn test_decimal_serialization_negative() {
        let value = Value::Decimal(-12345, 2); // Represents -123.45
        let serialized = serialize_value(&value);
        assert_eq!(serialized, Some(b"-123.45".to_vec()));
    }

    #[test]
    fn test_decimal_serialization_zero() {
        let value = Value::Decimal(0, 2); // Represents 0.00
        let serialized = serialize_value(&value);
        assert_eq!(serialized, Some(b"0.00".to_vec()));
    }

    #[test]
    fn test_decimal_serialization_different_scale() {
        let value = Value::Decimal(123, 0); // Represents 123
        let serialized = serialize_value(&value);
        assert_eq!(serialized, Some(b"123.0".to_vec()));
    }

    #[test]
    fn test_bytea_serialization_empty() {
        let value = Value::Bytea(vec![]);
        let serialized = serialize_value(&value);
        assert_eq!(serialized, Some(b"\\x".to_vec()));
    }

    #[test]
    fn test_bytea_serialization_normal() {
        let value = Value::Bytea(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let serialized = serialize_value(&value);
        assert_eq!(serialized, Some(b"\\xdeadbeef".to_vec()));
    }

    #[test]
    fn test_bytea_serialization_with_zero() {
        let value = Value::Bytea(vec![0x00, 0x01, 0xFF]);
        let serialized = serialize_value(&value);
        assert_eq!(serialized, Some(b"\\x0001ff".to_vec()));
    }
}
