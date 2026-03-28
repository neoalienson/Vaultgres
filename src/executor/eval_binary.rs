//! Binary operation evaluation
//!
//! Handles all binary operators including arithmetic, comparison, logical,
//! JSON, array, and range operators.

use super::super::catalog::{EnumTypeDef, Value};
use super::ExecutorError;
use super::eval_helpers::{
    compare_enum_text, compare_values, eval_like, value_to_sql_string,
    values_equal_with_enum_support,
};
use crate::parser::ast::BinaryOperator;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

/// Evaluate a binary operation
pub fn eval_binary_op(
    left: &Value,
    op: &BinaryOperator,
    right: &Value,
    enum_types: &Arc<RwLock<HashMap<String, EnumTypeDef>>>,
) -> Result<Value, ExecutorError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return eval_binary_op_null_safe(left, op, right);
    }

    match op {
        BinaryOperator::Equals => {
            if let Some(result) = compare_enum_text(left, right, enum_types)? {
                return Ok(Value::Bool(result));
            }
            if let Some(result) = compare_enum_text(right, left, enum_types)? {
                return Ok(Value::Bool(result));
            }
            Ok(Value::Bool(left == right))
        }
        BinaryOperator::NotEquals => {
            if let Some(result) = compare_enum_text(left, right, enum_types)? {
                return Ok(Value::Bool(!result));
            }
            if let Some(result) = compare_enum_text(right, left, enum_types)? {
                return Ok(Value::Bool(!result));
            }
            Ok(Value::Bool(left != right))
        }

        BinaryOperator::LessThan => {
            compare_values(left, right, |cmp| cmp == std::cmp::Ordering::Less)
        }
        BinaryOperator::LessThanOrEqual => {
            compare_values(left, right, |cmp| cmp != std::cmp::Ordering::Greater)
        }
        BinaryOperator::GreaterThan => {
            compare_values(left, right, |cmp| cmp == std::cmp::Ordering::Greater)
        }
        BinaryOperator::GreaterThanOrEqual => {
            compare_values(left, right, |cmp| cmp != std::cmp::Ordering::Less)
        }

        BinaryOperator::And => match (left, right) {
            (Value::Bool(l), Value::Bool(r)) => Ok(Value::Bool(*l && *r)),
            _ => Err(ExecutorError::TypeMismatch("AND requires boolean operands".to_string())),
        },
        BinaryOperator::Or => match (left, right) {
            (Value::Bool(l), Value::Bool(r)) => Ok(Value::Bool(*l || *r)),
            _ => Err(ExecutorError::TypeMismatch("OR requires boolean operands".to_string())),
        },

        BinaryOperator::Add => eval_add(left, right),
        BinaryOperator::Subtract => eval_subtract(left, right),
        BinaryOperator::Multiply => eval_multiply(left, right),
        BinaryOperator::Divide => eval_divide(left, right),
        BinaryOperator::Modulo => eval_modulo(left, right),
        BinaryOperator::StringConcat => {
            let l_str = value_to_sql_string(left);
            let r_str = value_to_sql_string(right);
            Ok(Value::Text(format!("{}{}", l_str.trim_matches('\''), r_str.trim_matches('\''))))
        }

        BinaryOperator::Like => eval_like(left, right, false),
        BinaryOperator::ILike => eval_like(left, right, true),
        BinaryOperator::In => eval_in(left, right),
        BinaryOperator::Between => {
            Err(ExecutorError::InternalError("BETWEEN should be converted by parser".to_string()))
        }
        BinaryOperator::Any | BinaryOperator::All | BinaryOperator::Some => {
            Err(ExecutorError::UnsupportedExpression(
                "ANY/ALL/SOME operators require subquery".to_string(),
            ))
        }

        BinaryOperator::JsonExtract => {
            super::json_evaluator::JsonEvaluator::eval_json_extract(left, right, false)
        }
        BinaryOperator::JsonExtractText => {
            super::json_evaluator::JsonEvaluator::eval_json_extract(left, right, true)
        }
        BinaryOperator::JsonPath => {
            super::json_evaluator::JsonEvaluator::eval_json_path(left, right, false)
        }
        BinaryOperator::JsonPathText => {
            super::json_evaluator::JsonEvaluator::eval_json_path(left, right, true)
        }
        BinaryOperator::JsonExists => {
            super::json_evaluator::JsonEvaluator::eval_json_exists(left, right)
        }
        BinaryOperator::JsonExistsAny => {
            super::json_evaluator::JsonEvaluator::eval_json_exists_any(left, right)
        }
        BinaryOperator::JsonExistsAll => {
            super::json_evaluator::JsonEvaluator::eval_json_exists_all(left, right)
        }

        BinaryOperator::ArrayContains => {
            super::array_evaluator::ArrayEvaluator::eval_array_contains(left, right)
        }
        BinaryOperator::ArrayContainedBy => {
            super::array_evaluator::ArrayEvaluator::eval_array_contained_by(left, right)
        }
        BinaryOperator::ArrayOverlaps => {
            super::array_evaluator::ArrayEvaluator::eval_array_overlaps(left, right)
        }
        BinaryOperator::ArrayConcat => {
            super::array_evaluator::ArrayEvaluator::eval_array_concat(left, right)
        }
        BinaryOperator::ArrayAccess => {
            super::array_evaluator::ArrayEvaluator::eval_array_access(left, right)
        }

        BinaryOperator::RangeContains => {
            super::range_evaluator::RangeEvaluator::eval_range_contains(left, right)
        }
        BinaryOperator::RangeContainedBy => {
            super::range_evaluator::RangeEvaluator::eval_range_contained_by(left, right)
        }
        BinaryOperator::RangeOverlaps => {
            super::range_evaluator::RangeEvaluator::eval_range_overlaps(left, right)
        }
        BinaryOperator::RangeLeftOf => {
            super::range_evaluator::RangeEvaluator::eval_range_left_of(left, right)
        }
        BinaryOperator::RangeRightOf => {
            super::range_evaluator::RangeEvaluator::eval_range_right_of(left, right)
        }
        BinaryOperator::RangeAdjacent => {
            super::range_evaluator::RangeEvaluator::eval_range_adjacent(left, right)
        }
    }
}

/// Handle NULL propagation for binary operations
fn eval_binary_op_null_safe(
    left: &Value,
    op: &BinaryOperator,
    right: &Value,
) -> Result<Value, ExecutorError> {
    match op {
        BinaryOperator::And => {
            if let Value::Bool(false) = left {
                return Ok(Value::Bool(false));
            }
            if let Value::Bool(false) = right {
                return Ok(Value::Bool(false));
            }
            Ok(Value::Null)
        }
        BinaryOperator::Or => {
            if let Value::Bool(true) = left {
                return Ok(Value::Bool(true));
            }
            if let Value::Bool(true) = right {
                return Ok(Value::Bool(true));
            }
            Ok(Value::Null)
        }
        BinaryOperator::In => {
            if let Value::Text(list_str) = right {
                let items: Vec<&str> = list_str.split(',').map(|s| s.trim()).collect();
                let left_str = value_to_sql_string(left);
                Ok(Value::Bool(items.contains(&left_str.as_str())))
            } else {
                Err(ExecutorError::UnsupportedExpression("IN operator requires a list".to_string()))
            }
        }
        _ => Ok(Value::Null),
    }
}

fn eval_add(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
    match (left, right) {
        (Value::Int(l), Value::Int(r)) => Ok(Value::Int(*l + *r)),
        (Value::Float(l), Value::Float(r)) => Ok(Value::Float(*l + *r)),
        (Value::Float(l), Value::Int(r)) => Ok(Value::Float(*l + *r as f64)),
        (Value::Int(l), Value::Float(r)) => Ok(Value::Float(*l as f64 + *r)),
        (Value::Text(l), Value::Text(r)) => Ok(Value::Text(format!("{}{}", l, r))),
        _ => Err(ExecutorError::TypeMismatch("ADD requires numeric or text operands".to_string())),
    }
}

fn eval_subtract(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
    match (left, right) {
        (Value::Int(l), Value::Int(r)) => Ok(Value::Int(*l - *r)),
        (Value::Float(l), Value::Float(r)) => Ok(Value::Float(*l - *r)),
        (Value::Float(l), Value::Int(r)) => Ok(Value::Float(*l - *r as f64)),
        (Value::Int(l), Value::Float(r)) => Ok(Value::Float(*l as f64 - *r)),
        _ => Err(ExecutorError::TypeMismatch("SUBTRACT requires numeric operands".to_string())),
    }
}

fn eval_multiply(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
    match (left, right) {
        (Value::Int(l), Value::Int(r)) => Ok(Value::Int(*l * *r)),
        (Value::Float(l), Value::Float(r)) => Ok(Value::Float(*l * *r)),
        (Value::Float(l), Value::Int(r)) => Ok(Value::Float(*l * *r as f64)),
        (Value::Int(l), Value::Float(r)) => Ok(Value::Float(*l as f64 * *r)),
        _ => Err(ExecutorError::TypeMismatch("MULTIPLY requires numeric operands".to_string())),
    }
}

fn eval_divide(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
    match (left, right) {
        (Value::Int(l), Value::Int(r)) => {
            if *r == 0 {
                Err(ExecutorError::DivisionByZero)
            } else {
                Ok(Value::Int(*l / *r))
            }
        }
        (Value::Float(l), Value::Float(r)) => {
            if *r == 0.0 {
                Err(ExecutorError::DivisionByZero)
            } else {
                Ok(Value::Float(*l / *r))
            }
        }
        (Value::Float(l), Value::Int(r)) => {
            if *r == 0 {
                Err(ExecutorError::DivisionByZero)
            } else {
                Ok(Value::Float(*l / *r as f64))
            }
        }
        (Value::Int(l), Value::Float(r)) => {
            if *r == 0.0 {
                Err(ExecutorError::DivisionByZero)
            } else {
                Ok(Value::Float(*l as f64 / *r))
            }
        }
        _ => Err(ExecutorError::TypeMismatch("DIVIDE requires numeric operands".to_string())),
    }
}

fn eval_modulo(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
    match (left, right) {
        (Value::Int(l), Value::Int(r)) => {
            if *r == 0 {
                Err(ExecutorError::DivisionByZero)
            } else {
                Ok(Value::Int(*l % *r))
            }
        }
        _ => Err(ExecutorError::TypeMismatch("MODULO requires integer operands".to_string())),
    }
}

fn eval_in(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
    match right {
        Value::Text(list_str) => {
            let items: Vec<&str> = list_str.split(',').map(|s| s.trim()).collect();
            let left_str = value_to_sql_string(left);
            Ok(Value::Bool(items.contains(&left_str.as_str())))
        }
        _ => Err(ExecutorError::UnsupportedExpression("IN operator requires a list".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Range;
    use crate::parser::ast::BinaryOperator;

    fn empty_enum_types() -> Arc<RwLock<HashMap<String, EnumTypeDef>>> {
        Arc::new(RwLock::new(HashMap::new()))
    }

    #[test]
    fn test_add_ints() {
        let result = eval_binary_op(
            &Value::Int(1),
            &BinaryOperator::Add,
            &Value::Int(2),
            &empty_enum_types(),
        )
        .unwrap();
        assert_eq!(result, Value::Int(3));
    }

    #[test]
    fn test_add_float_int() {
        let result = eval_binary_op(
            &Value::Float(1.5),
            &BinaryOperator::Add,
            &Value::Int(2),
            &empty_enum_types(),
        )
        .unwrap();
        assert_eq!(result, Value::Float(3.5));
    }

    #[test]
    fn test_add_text_concat() {
        let result = eval_binary_op(
            &Value::Text("hello".to_string()),
            &BinaryOperator::Add,
            &Value::Text("world".to_string()),
            &empty_enum_types(),
        )
        .unwrap();
        assert_eq!(result, Value::Text("helloworld".to_string()));
    }

    #[test]
    fn test_divide_by_zero() {
        let result = eval_binary_op(
            &Value::Int(1),
            &BinaryOperator::Divide,
            &Value::Int(0),
            &empty_enum_types(),
        );
        assert!(matches!(result, Err(ExecutorError::DivisionByZero)));
    }

    #[test]
    fn test_modulo_by_zero() {
        let result = eval_binary_op(
            &Value::Int(5),
            &BinaryOperator::Modulo,
            &Value::Int(0),
            &empty_enum_types(),
        );
        assert!(matches!(result, Err(ExecutorError::DivisionByZero)));
    }

    #[test]
    fn test_null_and_false() {
        let result = eval_binary_op(
            &Value::Null,
            &BinaryOperator::And,
            &Value::Bool(false),
            &empty_enum_types(),
        )
        .unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_null_or_true() {
        let result = eval_binary_op(
            &Value::Null,
            &BinaryOperator::Or,
            &Value::Bool(true),
            &empty_enum_types(),
        )
        .unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_null_add() {
        let result =
            eval_binary_op(&Value::Null, &BinaryOperator::Add, &Value::Int(1), &empty_enum_types())
                .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_range_contains() {
        let range =
            Value::Range(Range::new(Some(Value::Int(1)), true, Some(Value::Int(10)), false));
        let result = eval_binary_op(
            &range,
            &BinaryOperator::RangeContains,
            &Value::Int(5),
            &empty_enum_types(),
        )
        .unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_array_overlaps() {
        let left = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let right = Value::Array(vec![Value::Int(2), Value::Int(3)]);
        let result =
            eval_binary_op(&left, &BinaryOperator::ArrayOverlaps, &right, &empty_enum_types())
                .unwrap();
        assert_eq!(result, Value::Bool(true));
    }
}
