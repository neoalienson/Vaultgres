use super::executor::{Executor, ExecutorError, Tuple};
use crate::executor::eval::Eval;
use crate::parser::ast::Expr;

pub struct ProjectExecutor {
    child: Box<dyn Executor>,
    columns: Vec<Expr>,
}

impl ProjectExecutor {
    pub fn new(child: Box<dyn Executor>, columns: Vec<Expr>) -> Self {
        Self { child, columns }
    }

    /// Get the column name for an expression
    fn get_column_name(expr: &Expr) -> String {
        match expr {
            Expr::Column(name) => name.clone(),
            Expr::QualifiedColumn { table, column } => format!("{}.{}", table, column),
            Expr::FunctionCall { name, .. } => name.to_lowercase(),
            Expr::Aggregate { func, .. } => format!("{:?}", func).to_lowercase(),
            Expr::Alias { alias, .. } => alias.clone(),
            Expr::BinaryOp { .. } | Expr::UnaryOp { .. } => {
                // For complex expressions, use a generated name
                format!("{:?}", expr)
            }
            Expr::Number(_) => "number".to_string(),
            Expr::String(_) => "string".to_string(),
            Expr::Star => "*".to_string(),
            _ => format!("expr_{:?}", expr),
        }
    }
}

impl Executor for ProjectExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        match self.child.next()? {
            None => Ok(None),
            Some(input_tuple) => {
                let mut result_tuple = Tuple::new();
                for expr in &self.columns {
                    let evaluated_value = Eval::eval_expr(expr, &input_tuple)?;
                    let col_name = Self::get_column_name(expr);
                    result_tuple.insert(col_name, evaluated_value);
                }
                Ok(Some(result_tuple))
            }
        }
    }
}
