use crate::catalog::Value;
use crate::parser::ast::{BinaryOperator, Expr, UnaryOperator};
use std::collections::HashMap;

pub struct ExprEvaluator<'a> {
    variables: &'a HashMap<String, Value>,
}

impl<'a> ExprEvaluator<'a> {
    pub fn new(variables: &'a HashMap<String, Value>) -> Self {
        Self { variables }
    }

    pub fn eval(&self, expr: &Expr) -> Result<Value, String> {
        match expr {
            Expr::Number(n) => Ok(Value::Int(*n)),
            Expr::Float(f) => Ok(Value::Float(*f)),
            Expr::String(s) => Ok(Value::Text(s.clone())),
            Expr::Column(name) => self
                .variables
                .get(name)
                .cloned()
                .ok_or_else(|| format!("Variable '{}' not found", name)),
            Expr::List(exprs) => {
                let mut arr = Vec::new();
                for expr in exprs {
                    arr.push(self.eval(expr)?);
                }
                Ok(Value::Array(arr))
            }
            Expr::UnaryOp { op, expr } => {
                let val = self.eval(expr)?;
                self.eval_unary_op(op, &val)
            }
            Expr::BinaryOp { left, op, right } => {
                let l = self.eval(left)?;
                let r = self.eval(right)?;
                self.eval_binary_op(&l, op, &r)
            }
            _ => Err("Unsupported expression".to_string()),
        }
    }

    pub fn eval_string(&self, s: &str) -> Result<String, String> {
        let mut result = s.to_string();
        for (key, value) in self.variables {
            let placeholder = format!("${}", key.trim_start_matches('$'));
            if result.contains(&placeholder) {
                let val_str = match value {
                    Value::Int(n) => n.to_string(),
                    Value::Float(f) => f.to_string(),
                    Value::Bool(b) => b.to_string(),
                    Value::Text(t) => t.clone(),
                    Value::Array(_) => "[array]".to_string(),
                    Value::Json(j) => j.clone(),
                    Value::Date(d) => d.to_string(),
                    Value::Time(t) => t.to_string(),
                    Value::Timestamp(ts) => ts.to_string(),
                    Value::Decimal(v, _) => v.to_string(),
                    Value::Bytea(_) => "[binary]".to_string(),
                    Value::Null => "NULL".to_string(),
                };
                result = result.replace(&placeholder, &val_str);
            }
        }
        Ok(result)
    }

    pub fn is_true(value: &Value) -> bool {
        match value {
            Value::Int(n) => *n != 0,
            Value::Bool(b) => *b,
            _ => false,
        }
    }

    fn eval_unary_op(&self, op: &UnaryOperator, val: &Value) -> Result<Value, String> {
        match op {
            UnaryOperator::Not => match val {
                Value::Int(n) => Ok(Value::Bool(*n == 0)),
                Value::Bool(b) => Ok(Value::Bool(!b)),
                _ => Err("NOT requires integer or boolean".to_string()),
            },
            _ => Err(format!("Unsupported unary operator: {:?}", op)),
        }
    }

    fn eval_binary_op(
        &self,
        left: &Value,
        op: &BinaryOperator,
        right: &Value,
    ) -> Result<Value, String> {
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
                _ => Err(format!("Operator {:?} not supported for FLOAT", op)),
            },
            (Value::Bool(l), Value::Bool(r)) => match op {
                BinaryOperator::And => Ok(Value::Bool(*l && *r)),
                BinaryOperator::Or => Ok(Value::Bool(*l || *r)),
                BinaryOperator::Equals => Ok(Value::Bool(l == r)),
                _ => Err(format!("Operator {:?} not supported for BOOL", op)),
            },
            (Value::Text(l), Value::Text(r)) => match op {
                BinaryOperator::Equals => Ok(Value::Bool(l == r)),
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
            _ => Err("Type mismatch in binary operation".to_string()),
        }
    }
}
