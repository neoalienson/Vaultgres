//! Helper functions for expression evaluation
//!
//! Shared utilities for value conversion, comparison, and pattern matching.

use super::super::catalog::{EnumTypeDef, Value};
use super::ExecutorError;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

/// Convert a Value to string for concatenation
pub fn value_to_string(val: &Value) -> String {
    match val {
        Value::Text(s) => s.clone(),
        Value::Int(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        _ => format!("{:?}", val),
    }
}

/// Helper for comparison operations
pub fn compare_values<F>(left: &Value, right: &Value, cmp_fn: F) -> Result<Value, ExecutorError>
where
    F: FnOnce(std::cmp::Ordering) -> bool,
{
    match (left, right) {
        (Value::Int(l), Value::Int(r)) => Ok(Value::Bool(cmp_fn(l.cmp(r)))),
        (Value::Float(l), Value::Float(r)) => Ok(Value::Bool(cmp_fn(l.partial_cmp(r).unwrap()))),
        (Value::Text(l), Value::Text(r)) => Ok(Value::Bool(cmp_fn(l.cmp(r)))),
        _ => Err(ExecutorError::TypeMismatch("Comparison requires compatible types".to_string())),
    }
}

/// Evaluate LIKE pattern matching
pub fn eval_like(
    left: &Value,
    right: &Value,
    case_insensitive: bool,
) -> Result<Value, ExecutorError> {
    let text = match left {
        Value::Text(s) => s,
        _ => return Err(ExecutorError::TypeMismatch("LIKE requires text operand".to_string())),
    };

    let pattern = match right {
        Value::Text(s) => s,
        _ => return Err(ExecutorError::TypeMismatch("LIKE requires text pattern".to_string())),
    };

    let regex_pattern = regex::escape(pattern).replace('%', ".*").replace('_', ".");

    let regex = if case_insensitive {
        regex::Regex::new(&format!("(?i)^{}$", regex_pattern))
    } else {
        regex::Regex::new(&format!("^{}$", regex_pattern))
    }
    .map_err(|e| ExecutorError::InternalError(format!("Invalid LIKE pattern: {}", e)))?;

    Ok(Value::Bool(regex.is_match(text)))
}

/// Compare enum value to text value, returning Some(true/false) if one is Enum and other is Text
pub fn compare_enum_text(
    enum_val: &Value,
    text_val: &Value,
    enum_types: &Arc<RwLock<HashMap<String, EnumTypeDef>>>,
) -> Result<Option<bool>, ExecutorError> {
    match (enum_val, text_val) {
        (Value::Enum(e), Value::Text(s)) => {
            let types = enum_types.read().unwrap();
            if let Some(def) = types.get(&e.type_name) {
                if let Some(label) = def.labels.get(e.index as usize) {
                    return Ok(Some(label == s));
                }
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

/// Check if two values are equal, with enum support
pub fn values_equal_with_enum_support(
    left: &Value,
    right: &Value,
    enum_types: &Arc<RwLock<HashMap<String, EnumTypeDef>>>,
) -> Result<bool, ExecutorError> {
    if left == right {
        return Ok(true);
    }
    if let Some(result) = compare_enum_text(left, right, enum_types)? {
        return Ok(result);
    }
    if let Some(result) = compare_enum_text(right, left, enum_types)? {
        return Ok(result);
    }
    Ok(false)
}

/// Convert a Value to its SQL string representation
pub fn value_to_sql_string(value: &Value) -> String {
    match value {
        Value::Int(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Bool(b) => {
            if *b {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Value::Null => "NULL".to_string(),
        Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(|v| value_to_sql_string(v)).collect();
            format!("ARRAY[{}]", items.join(", "))
        }
        Value::Json(j) => format!("'{}'", j.replace('\'', "''")),
        Value::Date(d) => format!("DATE '{}'", d),
        Value::Time(t) => format!("TIME '{}'", t),
        Value::Timestamp(ts) => format!("TIMESTAMP '{}'", ts),
        Value::Decimal(v, _) => v.to_string(),
        Value::Bytea(b) => {
            let hex_str: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
            format!("'\\x{}'", hex_str)
        }
        Value::Enum(e) => format!("'{}[{}]'", e.type_name, e.index),
        Value::Composite(c) => format!(
            "ROW({})",
            c.fields.iter().map(|(_, v)| value_to_sql_string(v)).collect::<Vec<_>>().join(", ")
        ),
        Value::Range(r) => r.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_to_string_int() {
        assert_eq!(value_to_string(&Value::Int(42)), "42");
    }

    #[test]
    fn test_value_to_string_text() {
        assert_eq!(value_to_string(&Value::Text("hello".to_string())), "hello");
    }

    #[test]
    fn test_value_to_string_bool() {
        assert_eq!(value_to_string(&Value::Bool(true)), "true");
        assert_eq!(value_to_string(&Value::Bool(false)), "false");
    }

    #[test]
    fn test_value_to_string_null() {
        assert_eq!(value_to_string(&Value::Null), "");
    }

    #[test]
    fn test_value_to_sql_string_text() {
        assert_eq!(value_to_sql_string(&Value::Text("O'Reilly".to_string())), "'O''Reilly'");
    }

    #[test]
    fn test_value_to_sql_string_null() {
        assert_eq!(value_to_sql_string(&Value::Null), "NULL");
    }
}
