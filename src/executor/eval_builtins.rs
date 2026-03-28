//! Builtin function evaluation
//!
//! Handles SQL builtin functions like UPPER, LOWER, COALESCE, NULLIF, CONCAT, SUBSTRING.

use super::super::catalog::{Value, string_functions};
use super::ExecutorError;
use crate::parser::ast::UnaryOperator;

/// Evaluate a builtin function by name
pub fn eval_builtin_function(name: &str, args: &[Value]) -> Option<Result<Value, ExecutorError>> {
    match name.to_uppercase().as_str() {
        "UPPER" => Some(eval_upper(args)),
        "LOWER" => Some(eval_lower(args)),
        "LENGTH" => Some(eval_length(args)),
        "COALESCE" => Some(eval_coalesce(args)),
        "NULLIF" => Some(eval_nullif(args)),
        "CONCAT" => Some(eval_concat(args)),
        "SUBSTRING" => Some(eval_substring(args)),
        _ => None,
    }
}

fn eval_upper(args: &[Value]) -> Result<Value, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::TypeMismatch("UPPER takes one argument".to_string()));
    }
    string_functions::StringFunctions::upper(args[0].clone()).map_err(ExecutorError::TypeMismatch)
}

fn eval_lower(args: &[Value]) -> Result<Value, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::TypeMismatch("LOWER takes one argument".to_string()));
    }
    string_functions::StringFunctions::lower(args[0].clone()).map_err(ExecutorError::TypeMismatch)
}

fn eval_length(args: &[Value]) -> Result<Value, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::TypeMismatch("LENGTH takes one argument".to_string()));
    }
    string_functions::StringFunctions::length(args[0].clone()).map_err(ExecutorError::TypeMismatch)
}

fn eval_coalesce(args: &[Value]) -> Result<Value, ExecutorError> {
    for arg in args {
        if !matches!(arg, Value::Null) {
            return Ok(arg.clone());
        }
    }
    Ok(Value::Null)
}

fn eval_nullif(args: &[Value]) -> Result<Value, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::TypeMismatch("NULLIF takes two arguments".to_string()));
    }
    Ok(if args[0] == args[1] { Value::Null } else { args[0].clone() })
}

fn eval_concat(args: &[Value]) -> Result<Value, ExecutorError> {
    let mut result = String::new();
    for arg in args {
        match arg {
            Value::Text(s) => result.push_str(&s),
            Value::Int(i) => result.push_str(&i.to_string()),
            Value::Float(f) => result.push_str(&f.to_string()),
            Value::Null => continue,
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "CONCAT requires text or numeric values".to_string(),
                ));
            }
        }
    }
    Ok(Value::Text(result))
}

fn eval_substring(args: &[Value]) -> Result<Value, ExecutorError> {
    if args.len() < 2 || args.len() > 3 {
        return Err(ExecutorError::TypeMismatch("SUBSTRING takes 2 or 3 arguments".to_string()));
    }
    let length = if args.len() == 3 { Some(args[2].clone()) } else { None };
    string_functions::StringFunctions::substring(args[0].clone(), args[1].clone(), length)
        .map_err(ExecutorError::TypeMismatch)
}

/// Evaluate a unary operation
pub fn eval_unary_op(op: &UnaryOperator, val: &Value) -> Result<Value, ExecutorError> {
    match op {
        UnaryOperator::Not => match val {
            Value::Bool(b) => Ok(Value::Bool(!b)),
            _ => Err(ExecutorError::TypeMismatch("NOT requires boolean operand".to_string())),
        },
        UnaryOperator::Minus => match val {
            Value::Int(n) => Ok(Value::Int(-n)),
            _ => {
                Err(ExecutorError::TypeMismatch("Unary minus requires numeric operand".to_string()))
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::UnaryOperator;

    #[test]
    fn test_coalesce_returns_first_non_null() {
        let result = eval_coalesce(&[
            Value::Null,
            Value::Null,
            Value::Text("found".to_string()),
            Value::Text("ignored".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Text("found".to_string()));
    }

    #[test]
    fn test_coalesce_all_nulls() {
        let result = eval_coalesce(&[Value::Null, Value::Null]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_coalesce_empty() {
        let result = eval_coalesce(&[]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_nullif_equal_returns_null() {
        let result =
            eval_nullif(&[Value::Text("same".to_string()), Value::Text("same".to_string())])
                .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_nullif_different_returns_first() {
        let result =
            eval_nullif(&[Value::Text("first".to_string()), Value::Text("second".to_string())])
                .unwrap();
        assert_eq!(result, Value::Text("first".to_string()));
    }

    #[test]
    fn test_nullif_wrong_arg_count() {
        let result = eval_nullif(&[Value::Int(1)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_concat_two_strings() {
        let result =
            eval_concat(&[Value::Text("hello".to_string()), Value::Text("world".to_string())])
                .unwrap();
        assert_eq!(result, Value::Text("helloworld".to_string()));
    }

    #[test]
    fn test_concat_with_int() {
        let result = eval_concat(&[Value::Text("Value: ".to_string()), Value::Int(42)]).unwrap();
        assert_eq!(result, Value::Text("Value: 42".to_string()));
    }

    #[test]
    fn test_concat_with_null_skipped() {
        let result = eval_concat(&[
            Value::Text("hello".to_string()),
            Value::Null,
            Value::Text("world".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Text("helloworld".to_string()));
    }

    #[test]
    fn test_concat_empty() {
        let result = eval_concat(&[]).unwrap();
        assert_eq!(result, Value::Text("".to_string()));
    }

    #[test]
    fn test_concat_invalid_type() {
        let result = eval_concat(&[Value::Bool(true)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_unary_not_true() {
        let result = eval_unary_op(&UnaryOperator::Not, &Value::Bool(true)).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_unary_not_false() {
        let result = eval_unary_op(&UnaryOperator::Not, &Value::Bool(false)).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_unary_not_non_bool() {
        let result = eval_unary_op(&UnaryOperator::Not, &Value::Int(1));
        assert!(result.is_err());
    }

    #[test]
    fn test_unary_minus() {
        let result = eval_unary_op(&UnaryOperator::Minus, &Value::Int(42)).unwrap();
        assert_eq!(result, Value::Int(-42));
    }

    #[test]
    fn test_unary_minus_non_numeric() {
        let result = eval_unary_op(&UnaryOperator::Minus, &Value::Text("hello".to_string()));
        assert!(result.is_err());
    }
}
