//! Type validation for UPDATE operations
//!
//! Provides type checking for UPDATE assignments to ensure
//! type compatibility between the target column and the assigned value.

use crate::catalog::Value;
use crate::parser::ast::DataType;

/// Validates that a value is compatible with the target column data type.
///
/// # Type Coercion Rules
///
/// - `Float` column accepts both `Float` and `Int` values (Int is coerced to Float)
/// - Other type combinations require exact match
///
/// # Arguments
///
/// * `data_type` - The target column's data type
/// * `value` - The value being assigned
/// * `col_name` - The column name (for error messages)
///
/// # Returns
///
/// Returns `Ok(())` if the types are compatible, or an error message
pub fn validate_assignment_type(
    data_type: &DataType,
    value: &Value,
    col_name: &str,
) -> Result<(), String> {
    match (data_type, value) {
        (DataType::Int, Value::Int(_)) => Ok(()),
        (DataType::Float, Value::Float(_)) => Ok(()),
        (DataType::Float, Value::Int(_)) => Ok(()),
        (DataType::Text, Value::Text(_)) => Ok(()),
        (DataType::Varchar(_), Value::Text(_)) => Ok(()),
        (DataType::Boolean, Value::Bool(_)) => Ok(()),
        (DataType::Json, Value::Json(_)) => Ok(()),
        (DataType::Jsonb, Value::Json(_)) => Ok(()),
        (DataType::Enum(_), Value::Enum(_)) => Ok(()),
        (DataType::Array(_), Value::Array(_)) => Ok(()),
        _ => Err(format!("Type mismatch for column '{}'", col_name)),
    }
}

/// Returns a description of the type coercion behavior
pub fn type_coercion_notes() -> &'static str {
    "Type coercion rules:\n\
     - Float columns accept Int values (coerced to Float)\n\
     - All other types require exact match"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_int_column() {
        assert!(validate_assignment_type(&DataType::Int, &Value::Int(10), "col").is_ok());
    }

    #[test]
    fn test_validate_text_column() {
        assert!(
            validate_assignment_type(&DataType::Text, &Value::Text("hello".to_string()), "col")
                .is_ok()
        );
    }

    #[test]
    fn test_validate_varchar_column() {
        assert!(
            validate_assignment_type(
                &DataType::Varchar(10),
                &Value::Text("short".to_string()),
                "col"
            )
            .is_ok()
        );
    }

    #[test]
    fn test_validate_float_accepts_int() {
        assert!(validate_assignment_type(&DataType::Float, &Value::Int(10), "col").is_ok());
    }

    #[test]
    fn test_validate_float_rejects_text() {
        assert!(
            validate_assignment_type(&DataType::Float, &Value::Text("hello".to_string()), "col")
                .is_err()
        );
    }

    #[test]
    fn test_validate_int_rejects_text() {
        assert!(
            validate_assignment_type(&DataType::Int, &Value::Text("hello".to_string()), "col")
                .is_err()
        );
    }

    #[test]
    fn test_validate_text_rejects_int() {
        assert!(validate_assignment_type(&DataType::Text, &Value::Int(10), "col").is_err());
    }

    #[test]
    fn test_validate_bool_column() {
        assert!(validate_assignment_type(&DataType::Boolean, &Value::Bool(true), "col").is_ok());
    }

    #[test]
    fn test_validate_json_column() {
        assert!(
            validate_assignment_type(&DataType::Json, &Value::Json("{}".to_string()), "col")
                .is_ok()
        );
    }
}
