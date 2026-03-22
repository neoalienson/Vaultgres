use crate::catalog::Value;
use crate::parser::ast::{BinaryOperator, Expr, UnaryOperator};

pub fn eval_binary_op(left: &Value, op: &BinaryOperator, right: &Value) -> Result<Value, String> {
    match (left, right) {
        (Value::Int(l), Value::Int(r)) => match op {
            BinaryOperator::Equals => Ok(Value::Bool(l == r)),
            BinaryOperator::GreaterThan => Ok(Value::Bool(l > r)),
            BinaryOperator::LessThan => Ok(Value::Bool(l < r)),
            BinaryOperator::GreaterThanOrEqual => Ok(Value::Bool(l >= r)),
            BinaryOperator::LessThanOrEqual => Ok(Value::Bool(l <= r)),
            BinaryOperator::Add => Ok(Value::Int(l + r)),
            BinaryOperator::Subtract => Ok(Value::Int(l - r)),
            BinaryOperator::Multiply => Ok(Value::Int(l * r)),
            BinaryOperator::Divide => {
                if *r == 0 {
                    Err("Division by zero".to_string())
                } else {
                    Ok(Value::Int(l / r))
                }
            }
            BinaryOperator::Modulo => {
                if *r == 0 {
                    Err("Division by zero".to_string())
                } else {
                    Ok(Value::Int(l % r))
                }
            }
            BinaryOperator::And => Ok(Value::Bool(*l != 0 && *r != 0)),
            BinaryOperator::Or => Ok(Value::Bool(*l != 0 || *r != 0)),
            BinaryOperator::NotEquals => Ok(Value::Bool(l != r)),
            _ => Err(format!("Operator {:?} not supported for INT", op)),
        },
        (Value::Float(l), Value::Float(r)) => match op {
            BinaryOperator::Equals => Ok(Value::Bool((l - r).abs() < f64::EPSILON)),
            BinaryOperator::GreaterThan => Ok(Value::Bool(l > r)),
            BinaryOperator::LessThan => Ok(Value::Bool(l < r)),
            BinaryOperator::GreaterThanOrEqual => Ok(Value::Bool(l >= r)),
            BinaryOperator::LessThanOrEqual => Ok(Value::Bool(l <= r)),
            BinaryOperator::Add => Ok(Value::Float(l + r)),
            BinaryOperator::Subtract => Ok(Value::Float(l - r)),
            BinaryOperator::Multiply => Ok(Value::Float(l * r)),
            BinaryOperator::Divide => {
                if *r == 0.0 {
                    Err("Division by zero".to_string())
                } else {
                    Ok(Value::Float(l / r))
                }
            }
            BinaryOperator::NotEquals => Ok(Value::Bool((l - r).abs() >= f64::EPSILON)),
            _ => Err(format!("Operator {:?} not supported for FLOAT", op)),
        },
        (Value::Float(l), Value::Int(r)) => match op {
            BinaryOperator::Equals => Ok(Value::Bool((*l - *r as f64).abs() < f64::EPSILON)),
            BinaryOperator::GreaterThan => Ok(Value::Bool(*l > *r as f64)),
            BinaryOperator::LessThan => Ok(Value::Bool(*l < *r as f64)),
            BinaryOperator::GreaterThanOrEqual => Ok(Value::Bool(*l >= *r as f64)),
            BinaryOperator::LessThanOrEqual => Ok(Value::Bool(*l <= *r as f64)),
            BinaryOperator::Add => Ok(Value::Float(l + *r as f64)),
            BinaryOperator::Subtract => Ok(Value::Float(l - *r as f64)),
            BinaryOperator::Multiply => Ok(Value::Float(l * *r as f64)),
            BinaryOperator::Divide => {
                if *r == 0 {
                    Err("Division by zero".to_string())
                } else {
                    Ok(Value::Float(l / *r as f64))
                }
            }
            BinaryOperator::NotEquals => Ok(Value::Bool((*l - *r as f64).abs() >= f64::EPSILON)),
            _ => Err(format!("Operator {:?} not supported for FLOAT/INT", op)),
        },
        (Value::Int(l), Value::Float(r)) => match op {
            BinaryOperator::Equals => Ok(Value::Bool((*l as f64 - r).abs() < f64::EPSILON)),
            BinaryOperator::GreaterThan => Ok(Value::Bool((*l as f64) > *r)),
            BinaryOperator::LessThan => Ok(Value::Bool((*l as f64) < *r)),
            BinaryOperator::GreaterThanOrEqual => Ok(Value::Bool((*l as f64) >= *r)),
            BinaryOperator::LessThanOrEqual => Ok(Value::Bool((*l as f64) <= *r)),
            BinaryOperator::Add => Ok(Value::Float(*l as f64 + r)),
            BinaryOperator::Subtract => Ok(Value::Float(*l as f64 - r)),
            BinaryOperator::Multiply => Ok(Value::Float(*l as f64 * r)),
            BinaryOperator::Divide => {
                if *r == 0.0 {
                    Err("Division by zero".to_string())
                } else {
                    Ok(Value::Float(*l as f64 / r))
                }
            }
            BinaryOperator::NotEquals => Ok(Value::Bool((*l as f64 - r).abs() >= f64::EPSILON)),
            _ => Err(format!("Operator {:?} not supported for INT/FLOAT", op)),
        },
        (Value::Bool(l), Value::Bool(r)) => match op {
            BinaryOperator::And => Ok(Value::Bool(*l && *r)),
            BinaryOperator::Or => Ok(Value::Bool(*l || *r)),
            BinaryOperator::Equals => Ok(Value::Bool(l == r)),
            BinaryOperator::NotEquals => Ok(Value::Bool(l != r)),
            _ => Err(format!("Operator {:?} not supported for BOOL", op)),
        },
        (Value::Text(l), Value::Text(r)) => match op {
            BinaryOperator::Equals => Ok(Value::Bool(l == r)),
            BinaryOperator::NotEquals => Ok(Value::Bool(l != r)),
            BinaryOperator::StringConcat => Ok(Value::Text(format!("{}{}", l, r))),
            BinaryOperator::Like => {
                let pattern = r.replace('%', ".*").replace('_', ".");
                let re = regex::Regex::new(&format!("^{}$", pattern))
                    .map_err(|e| format!("Invalid pattern: {}", e))?;
                Ok(Value::Bool(re.is_match(l)))
            }
            BinaryOperator::ILike => {
                let pattern = r.replace('%', ".*").replace('_', ".");
                let re = regex::Regex::new(&format!("(?i)^{}$", pattern))
                    .map_err(|e| format!("Invalid pattern: {}", e))?;
                Ok(Value::Bool(re.is_match(l)))
            }
            _ => Err(format!("Operator {:?} not supported for TEXT", op)),
        },
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err("Type mismatch in binary operation".to_string()),
    }
}

pub fn eval_unary_op(op: &UnaryOperator, val: &Value) -> Result<Value, String> {
    match op {
        UnaryOperator::Not => match val {
            Value::Int(n) => Ok(Value::Bool(*n == 0)),
            Value::Bool(b) => Ok(Value::Bool(!*b)),
            _ => Err("NOT requires integer or boolean".to_string()),
        },
        UnaryOperator::Minus => match val {
            Value::Int(n) => Ok(Value::Int(-*n)),
            Value::Float(f) => Ok(Value::Float(-*f)),
            _ => Err("Unary minus requires integer or float".to_string()),
        },
    }
}

pub trait ExprEvaluator {
    fn eval(&self, expr: &Expr) -> Result<Value, String>;
}

impl ExprEvaluator for Value {
    fn eval(&self, _expr: &Expr) -> Result<Value, String> {
        Ok(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::BinaryOperator;
    use std::collections::HashMap;

    #[derive(Clone)]
    struct HashMapEvaluator<'a>(&'a HashMap<String, Value>);

    impl<'a> ExprEvaluator for HashMapEvaluator<'a> {
        fn eval(&self, expr: &Expr) -> Result<Value, String> {
            match expr {
                Expr::Number(n) => Ok(Value::Int(*n)),
                Expr::Float(f) => Ok(Value::Float(*f)),
                Expr::String(s) => Ok(Value::Text(s.clone())),
                Expr::Column(name) => self
                    .0
                    .get(name)
                    .cloned()
                    .ok_or_else(|| format!("Variable '{}' not found", name)),
                Expr::List(exprs) => {
                    let mut arr = Vec::new();
                    for e in exprs {
                        arr.push(self.eval(e)?);
                    }
                    Ok(Value::Array(arr))
                }
                Expr::UnaryOp { op, expr } => {
                    let val = self.eval(expr)?;
                    eval_unary_op(op, &val)
                }
                Expr::BinaryOp { left, op, right } => {
                    let l = self.eval(left)?;
                    let r = self.eval(right)?;
                    eval_binary_op(&l, op, &r)
                }
                _ => Err("Unsupported expression".to_string()),
            }
        }
    }

    #[test]
    fn test_binary_op_int_arithmetic() {
        let left = Value::Int(10);
        let right = Value::Int(3);

        assert_eq!(eval_binary_op(&left, &BinaryOperator::Add, &right).unwrap(), Value::Int(13));
        assert_eq!(
            eval_binary_op(&left, &BinaryOperator::Subtract, &right).unwrap(),
            Value::Int(7)
        );
        assert_eq!(
            eval_binary_op(&left, &BinaryOperator::Multiply, &right).unwrap(),
            Value::Int(30)
        );
        assert_eq!(eval_binary_op(&left, &BinaryOperator::Divide, &right).unwrap(), Value::Int(3));
        assert_eq!(eval_binary_op(&left, &BinaryOperator::Modulo, &right).unwrap(), Value::Int(1));
    }

    #[test]
    fn test_binary_op_int_division_by_zero() {
        let left = Value::Int(10);
        let right = Value::Int(0);

        assert!(eval_binary_op(&left, &BinaryOperator::Divide, &right).is_err());
        assert!(eval_binary_op(&left, &BinaryOperator::Modulo, &right).is_err());
    }

    #[test]
    fn test_binary_op_float_arithmetic() {
        let left = Value::Float(10.5);
        let right = Value::Float(2.0);

        assert_eq!(
            eval_binary_op(&left, &BinaryOperator::Add, &right).unwrap(),
            Value::Float(12.5)
        );
        assert_eq!(
            eval_binary_op(&left, &BinaryOperator::Subtract, &right).unwrap(),
            Value::Float(8.5)
        );
        assert_eq!(
            eval_binary_op(&left, &BinaryOperator::Multiply, &right).unwrap(),
            Value::Float(21.0)
        );
        assert_eq!(
            eval_binary_op(&left, &BinaryOperator::Divide, &right).unwrap(),
            Value::Float(5.25)
        );
    }

    #[test]
    fn test_binary_op_float_division_by_zero() {
        let left = Value::Float(10.0);
        let right = Value::Float(0.0);

        assert!(eval_binary_op(&left, &BinaryOperator::Divide, &right).is_err());
    }

    #[test]
    fn test_binary_op_comparison_int() {
        let left = Value::Int(10);
        let right = Value::Int(10);

        assert!(
            eval_binary_op(&left, &BinaryOperator::Equals, &right).unwrap() == Value::Bool(true)
        );
        assert!(
            eval_binary_op(&left, &BinaryOperator::NotEquals, &right).unwrap()
                == Value::Bool(false)
        );
        assert!(
            eval_binary_op(&left, &BinaryOperator::GreaterThan, &right).unwrap()
                == Value::Bool(false)
        );
        assert!(
            eval_binary_op(&left, &BinaryOperator::LessThan, &right).unwrap() == Value::Bool(false)
        );
        assert!(
            eval_binary_op(&left, &BinaryOperator::GreaterThanOrEqual, &right).unwrap()
                == Value::Bool(true)
        );
        assert!(
            eval_binary_op(&left, &BinaryOperator::LessThanOrEqual, &right).unwrap()
                == Value::Bool(true)
        );
    }

    #[test]
    fn test_binary_op_bool() {
        let left = Value::Bool(true);
        let right = Value::Bool(false);

        assert!(eval_binary_op(&left, &BinaryOperator::And, &right).unwrap() == Value::Bool(false));
        assert!(eval_binary_op(&left, &BinaryOperator::Or, &right).unwrap() == Value::Bool(true));
        assert!(
            eval_binary_op(&left, &BinaryOperator::Equals, &right).unwrap() == Value::Bool(false)
        );
    }

    #[test]
    fn test_binary_op_text() {
        let left = Value::Text("Hello".to_string());
        let right = Value::Text("World".to_string());

        assert!(
            eval_binary_op(&left, &BinaryOperator::Equals, &right).unwrap() == Value::Bool(false)
        );
        assert!(
            eval_binary_op(&left, &BinaryOperator::StringConcat, &right).unwrap()
                == Value::Text("HelloWorld".to_string())
        );
    }

    #[test]
    fn test_binary_op_null() {
        let left = Value::Null;
        let right = Value::Int(10);

        assert!(eval_binary_op(&left, &BinaryOperator::Add, &right).unwrap() == Value::Null);
        assert!(eval_binary_op(&right, &BinaryOperator::Add, &left).unwrap() == Value::Null);
    }

    #[test]
    fn test_binary_op_type_mismatch() {
        let left = Value::Int(10);
        let right = Value::Text("hello".to_string());

        assert!(eval_binary_op(&left, &BinaryOperator::Add, &right).is_err());
    }

    #[test]
    fn test_unary_op_minus() {
        assert!(eval_unary_op(&UnaryOperator::Minus, &Value::Int(10)).unwrap() == Value::Int(-10));
        assert!(
            eval_unary_op(&UnaryOperator::Minus, &Value::Float(5.5)).unwrap() == Value::Float(-5.5)
        );
    }

    #[test]
    fn test_hashmap_evaluator_arithmetic() {
        let mut vars = HashMap::new();
        vars.insert("x".to_string(), Value::Int(5));
        vars.insert("y".to_string(), Value::Int(3));

        let evaluator = HashMapEvaluator(&vars);

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("x".to_string())),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Column("y".to_string())),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Int(8));
    }

    #[test]
    fn test_hashmap_evaluator_nested_arithmetic() {
        let mut vars = HashMap::new();
        vars.insert("a".to_string(), Value::Int(2));
        vars.insert("b".to_string(), Value::Int(3));
        vars.insert("c".to_string(), Value::Int(4));

        let evaluator = HashMapEvaluator(&vars);

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("a".to_string())),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("b".to_string())),
                op: BinaryOperator::Add,
                right: Box::new(Expr::Column("c".to_string())),
            }),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Int(14));
    }

    #[test]
    fn test_hashmap_evaluator_column_not_found() {
        let vars = HashMap::new();
        let evaluator = HashMapEvaluator(&vars);

        let expr = Expr::Column("missing".to_string());
        assert!(evaluator.eval(&expr).is_err());
    }

    #[test]
    fn test_hashmap_evaluator_with_literals() {
        let mut vars = HashMap::new();
        vars.insert("x".to_string(), Value::Int(10));

        let evaluator = HashMapEvaluator(&vars);

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("x".to_string())),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Number(2)),
        };
        assert_eq!(evaluator.eval(&expr).unwrap(), Value::Int(20));
    }
}
