use crate::catalog::Value;
use crate::executor::expr_evaluator::{eval_binary_op, eval_unary_op};
use crate::parser::ast::Expr;
use std::collections::HashMap;

pub struct PlPgSqlExprEvaluator<'a> {
    variables: &'a HashMap<String, Value>,
}

impl<'a> PlPgSqlExprEvaluator<'a> {
    pub fn new(variables: &'a HashMap<String, Value>) -> Self {
        Self { variables }
    }

    pub fn eval(&self, expr: &Expr) -> Result<Value, String> {
        match expr {
            Expr::Number(n) => Ok(Value::Int(*n)),
            Expr::Float(f) => Ok(Value::Float(*f)),
            Expr::String(s) => Ok(Value::Text(s.clone())),
            Expr::Null => Ok(Value::Null),
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
                eval_unary_op(op, &val)
            }
            Expr::BinaryOp { left, op, right } => {
                let l = self.eval(left)?;
                let r = self.eval(right)?;
                eval_binary_op(&l, op, &r)
            }
            Expr::IsNull(expr) => {
                let val = self.eval(expr)?;
                Ok(Value::Bool(matches!(val, Value::Null)))
            }
            Expr::IsNotNull(expr) => {
                let val = self.eval(expr)?;
                Ok(Value::Bool(!matches!(val, Value::Null)))
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
}
