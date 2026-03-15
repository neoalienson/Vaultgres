use crate::catalog::tuple::Tuple;
use crate::catalog::value::Value;
use crate::parser::ast::{BinaryOperator, CheckConstraint, Expr};

pub struct CheckValidator;

impl CheckValidator {
    pub fn validate(constraint: &CheckConstraint, tuple: &Tuple) -> Result<(), String> {
        if Self::evaluate_expr(&constraint.expr, tuple)? {
            Ok(())
        } else {
            let name = constraint.name.as_deref().unwrap_or("unnamed");
            Err(format!("CHECK constraint '{}' violated", name))
        }
    }

    fn evaluate_expr(expr: &Expr, tuple: &Tuple) -> Result<bool, String> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left_val = Self::get_value(left, tuple)?;
                let right_val = Self::get_value(right, tuple)?;
                Self::compare(left_val, *op, right_val)
            }
            Expr::UnaryOp { op, expr } => {
                let val = Self::evaluate_expr(expr, tuple)?;
                match op {
                    crate::parser::ast::UnaryOperator::Not => Ok(!val),
                    _ => Err("Unsupported unary operator in CHECK".to_string()),
                }
            }
            _ => Err("Unsupported expression in CHECK".to_string()),
        }
    }

    fn get_value(expr: &Expr, tuple: &Tuple) -> Result<Value, String> {
        match expr {
            Expr::Column(name) => {
                tuple.get_value(name).ok_or_else(|| format!("Column '{}' not found", name))
            }
            Expr::Number(n) => Ok(Value::Int(*n)),
            Expr::String(s) => Ok(Value::Text(s.clone())),
            Expr::Null => Ok(Value::Null),
            _ => Err("Unsupported value expression in CHECK".to_string()),
        }
    }

    fn compare(left: Value, op: BinaryOperator, right: Value) -> Result<bool, String> {
        match (left, right) {
            (Value::Int(l), Value::Int(r)) => Ok(match op {
                BinaryOperator::Equals => l == r,
                BinaryOperator::NotEquals => l != r,
                BinaryOperator::LessThan => l < r,
                BinaryOperator::LessThanOrEqual => l <= r,
                BinaryOperator::GreaterThan => l > r,
                BinaryOperator::GreaterThanOrEqual => l >= r,
                BinaryOperator::And | BinaryOperator::Or => {
                    return Err("Logical operators require boolean operands".to_string());
                }
                _ => return Err(format!("Unsupported operator: {:?}", op)),
            }),
            (Value::Text(l), Value::Text(r)) => Ok(match op {
                BinaryOperator::Equals => l == r,
                BinaryOperator::NotEquals => l != r,
                _ => return Err("Text comparison only supports = and !=".to_string()),
            }),
            _ => Err("Type mismatch in CHECK constraint".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::tuple::Tuple;
    use crate::catalog::value::Value;
    use crate::parser::ast::{BinaryOperator, CheckConstraint, Expr};

    fn create_tuple(values: Vec<(&str, Value)>) -> Tuple {
        let mut tuple = Tuple::new();
        for (name, value) in values {
            tuple.add_value(name.to_string(), value);
        }
        tuple
    }

    #[test]
    fn test_check_greater_than() {
        let constraint = CheckConstraint {
            name: Some("age_check".to_string()),
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expr::Number(0)),
            },
        };

        let tuple = create_tuple(vec![("age", Value::Int(25))]);
        assert!(CheckValidator::validate(&constraint, &tuple).is_ok());

        let tuple = create_tuple(vec![("age", Value::Int(0))]);
        assert!(CheckValidator::validate(&constraint, &tuple).is_err());
    }

    #[test]
    fn test_check_less_than_or_equal() {
        let constraint = CheckConstraint {
            name: Some("max_age".to_string()),
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: BinaryOperator::LessThanOrEqual,
                right: Box::new(Expr::Number(100)),
            },
        };

        let tuple = create_tuple(vec![("age", Value::Int(100))]);
        assert!(CheckValidator::validate(&constraint, &tuple).is_ok());

        let tuple = create_tuple(vec![("age", Value::Int(101))]);
        assert!(CheckValidator::validate(&constraint, &tuple).is_err());
    }

    #[test]
    fn test_check_equals() {
        let constraint = CheckConstraint {
            name: None,
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("status".to_string())),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::String("active".to_string())),
            },
        };

        let tuple = create_tuple(vec![("status", Value::Text("active".to_string()))]);
        assert!(CheckValidator::validate(&constraint, &tuple).is_ok());

        let tuple = create_tuple(vec![("status", Value::Text("inactive".to_string()))]);
        assert!(CheckValidator::validate(&constraint, &tuple).is_err());
    }

    #[test]
    fn test_check_not_equals() {
        let constraint = CheckConstraint {
            name: Some("status_check".to_string()),
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("status".to_string())),
                op: BinaryOperator::NotEquals,
                right: Box::new(Expr::String("deleted".to_string())),
            },
        };

        let tuple = create_tuple(vec![("status", Value::Text("active".to_string()))]);
        assert!(CheckValidator::validate(&constraint, &tuple).is_ok());

        let tuple = create_tuple(vec![("status", Value::Text("deleted".to_string()))]);
        assert!(CheckValidator::validate(&constraint, &tuple).is_err());
    }

    #[test]
    fn test_check_missing_column() {
        let constraint = CheckConstraint {
            name: Some("test".to_string()),
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("missing".to_string())),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expr::Number(0)),
            },
        };

        let tuple = create_tuple(vec![("age", Value::Int(25))]);
        assert!(CheckValidator::validate(&constraint, &tuple).is_err());
    }

    #[test]
    fn test_check_range() {
        let constraint = CheckConstraint {
            name: Some("range_check".to_string()),
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("value".to_string())),
                op: BinaryOperator::GreaterThanOrEqual,
                right: Box::new(Expr::Number(10)),
            },
        };

        let tuple = create_tuple(vec![("value", Value::Int(10))]);
        assert!(CheckValidator::validate(&constraint, &tuple).is_ok());

        let tuple = create_tuple(vec![("value", Value::Int(9))]);
        assert!(CheckValidator::validate(&constraint, &tuple).is_err());
    }

    // New tests for error cases in evaluate_expr and compare

    #[test]
    fn test_evaluate_expr_unsupported_unary_operator() {
        let constraint = CheckConstraint {
            name: None,
            expr: Expr::UnaryOp {
                op: crate::parser::ast::UnaryOperator::Minus, // Example of unsupported unary op for boolean result
                expr: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Column("val".to_string())),
                    op: BinaryOperator::Equals,
                    right: Box::new(Expr::Number(1)),
                }),
            },
        };
        let tuple = create_tuple(vec![("val", Value::Int(1))]);
        let result = CheckValidator::validate(&constraint, &tuple);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Unsupported unary operator in CHECK");
    }

    #[test]
    fn test_evaluate_expr_unsupported_expression_type() {
        let constraint = CheckConstraint {
            name: None,
            expr: Expr::FunctionCall {
                // FunctionCall is not supported directly in evaluate_expr
                name: "foo".to_string(),
                args: vec![],
            },
        };
        let tuple = create_tuple(vec![]);
        let result = CheckValidator::validate(&constraint, &tuple);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Unsupported expression in CHECK");
    }

    #[test]
    fn test_get_value_unsupported_expression_type() {
        let constraint = CheckConstraint {
            name: None,
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Parameter(0)), // Parameter is not supported in get_value
                op: BinaryOperator::Equals,
                right: Box::new(Expr::Number(1)),
            },
        };
        let tuple = create_tuple(vec![]);
        let result = CheckValidator::validate(&constraint, &tuple);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Unsupported value expression in CHECK");
    }

    #[test]
    fn test_compare_integer_logical_operators_error() {
        let constraint = CheckConstraint {
            name: None,
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Number(1)),
                op: BinaryOperator::And, // Logical operator with Int values
                right: Box::new(Expr::Number(2)),
            },
        };
        let tuple = create_tuple(vec![]);
        let result = CheckValidator::validate(&constraint, &tuple);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Logical operators require boolean operands");
    }

    #[test]
    fn test_compare_integer_unsupported_operator_error() {
        let constraint = CheckConstraint {
            name: None,
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Number(1)),
                op: BinaryOperator::Like, // Unsupported operator for Int values
                right: Box::new(Expr::Number(2)),
            },
        };
        let tuple = create_tuple(vec![]);
        let result = CheckValidator::validate(&constraint, &tuple);
        assert!(result.is_err());
        assert!(result.unwrap_err().starts_with("Unsupported operator:"));
    }

    #[test]
    fn test_compare_text_unsupported_operator_error() {
        let constraint = CheckConstraint {
            name: None,
            expr: Expr::BinaryOp {
                left: Box::new(Expr::String("a".to_string())),
                op: BinaryOperator::GreaterThan, // Unsupported operator for Text values
                right: Box::new(Expr::String("b".to_string())),
            },
        };
        let tuple = create_tuple(vec![]);
        let result = CheckValidator::validate(&constraint, &tuple);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Text comparison only supports = and !=");
    }

    #[test]
    fn test_compare_type_mismatch_error() {
        let constraint = CheckConstraint {
            name: None,
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Number(1)),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::String("a".to_string())),
            },
        };
        let tuple = create_tuple(vec![]);
        let result = CheckValidator::validate(&constraint, &tuple);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Type mismatch in CHECK constraint");
    }
}

#[cfg(test)]
mod edge_tests {
    use super::*;
    use crate::catalog::tuple::Tuple;
    use crate::catalog::value::Value;
    use crate::parser::ast::{BinaryOperator, CheckConstraint, Expr};

    fn create_tuple(values: Vec<(&str, Value)>) -> Tuple {
        let mut tuple = Tuple::new();
        for (name, value) in values {
            tuple.add_value(name.to_string(), value);
        }
        tuple
    }

    #[test]
    fn test_check_zero_value() {
        let constraint = CheckConstraint {
            name: Some("zero_check".to_string()),
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("val".to_string())),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::Number(0)),
            },
        };

        let tuple = create_tuple(vec![("val", Value::Int(0))]);
        assert!(CheckValidator::validate(&constraint, &tuple).is_ok());
    }

    #[test]
    fn test_check_negative_value() {
        let constraint = CheckConstraint {
            name: Some("negative_check".to_string()),
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("val".to_string())),
                op: BinaryOperator::LessThan,
                right: Box::new(Expr::Number(0)),
            },
        };

        let tuple = create_tuple(vec![("val", Value::Int(-10))]);
        assert!(CheckValidator::validate(&constraint, &tuple).is_ok());
    }

    #[test]
    fn test_check_empty_string() {
        let constraint = CheckConstraint {
            name: Some("empty_check".to_string()),
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("name".to_string())),
                op: BinaryOperator::NotEquals,
                right: Box::new(Expr::String("".to_string())),
            },
        };

        let tuple = create_tuple(vec![("name", Value::Text("".to_string()))]);
        assert!(CheckValidator::validate(&constraint, &tuple).is_err());
    }

    #[test]
    fn test_check_large_number() {
        let constraint = CheckConstraint {
            name: Some("large_check".to_string()),
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("val".to_string())),
                op: BinaryOperator::LessThan,
                right: Box::new(Expr::Number(i64::MAX)),
            },
        };

        let tuple = create_tuple(vec![("val", Value::Int(i64::MAX - 1))]);
        assert!(CheckValidator::validate(&constraint, &tuple).is_ok());
    }

    #[test]
    fn test_check_unnamed_constraint() {
        let constraint = CheckConstraint {
            name: None,
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("val".to_string())),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expr::Number(0)),
            },
        };

        let tuple = create_tuple(vec![("val", Value::Int(-1))]);
        let result = CheckValidator::validate(&constraint, &tuple);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unnamed"));
    }

    #[test]
    fn test_check_boundary_values() {
        let constraint = CheckConstraint {
            name: Some("boundary".to_string()),
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("val".to_string())),
                op: BinaryOperator::GreaterThanOrEqual,
                right: Box::new(Expr::Number(0)),
            },
        };

        let tuple = create_tuple(vec![("val", Value::Int(0))]);
        assert!(CheckValidator::validate(&constraint, &tuple).is_ok());

        let tuple = create_tuple(vec![("val", Value::Int(-1))]);
        assert!(CheckValidator::validate(&constraint, &tuple).is_err());
    }

    #[test]
    fn test_check_special_characters() {
        let constraint = CheckConstraint {
            name: Some("special_check".to_string()),
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("text".to_string())),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::String("a'b\"c".to_string())),
            },
        };

        let tuple = create_tuple(vec![("text", Value::Text("a'b\"c".to_string()))]);
        assert!(CheckValidator::validate(&constraint, &tuple).is_ok());
    }

    #[test]
    fn test_check_unicode() {
        let constraint = CheckConstraint {
            name: Some("unicode_check".to_string()),
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("name".to_string())),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::String("张三".to_string())),
            },
        };

        let tuple = create_tuple(vec![("name", Value::Text("张三".to_string()))]);
        assert!(CheckValidator::validate(&constraint, &tuple).is_ok());
    }

    #[test]
    fn test_check_multiple_constraints() {
        let constraint1 = CheckConstraint {
            name: Some("check1".to_string()),
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expr::Number(0)),
            },
        };

        let constraint2 = CheckConstraint {
            name: Some("check2".to_string()),
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: BinaryOperator::LessThan,
                right: Box::new(Expr::Number(150)),
            },
        };

        let tuple = create_tuple(vec![("age", Value::Int(25))]);
        assert!(CheckValidator::validate(&constraint1, &tuple).is_ok());
        assert!(CheckValidator::validate(&constraint2, &tuple).is_ok());
    }

    #[test]
    fn test_check_long_string() {
        let long_str = "a".repeat(10000);
        let constraint = CheckConstraint {
            name: Some("long_check".to_string()),
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("text".to_string())),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::String(long_str.clone())),
            },
        };

        let tuple = create_tuple(vec![("text", Value::Text(long_str))]);
        assert!(CheckValidator::validate(&constraint, &tuple).is_ok());
    }
}
