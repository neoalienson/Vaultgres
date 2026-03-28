use crate::executor::operators::executor::{ExecutorError, Tuple};
use crate::executor::volcano::aggregate_state::hash_value;
use crate::parser::ast::Expr;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

pub fn compute_group_key(tuple: &Tuple, group_by: &[Expr]) -> Result<u64, ExecutorError> {
    let mut hasher = DefaultHasher::new();
    for expr in group_by {
        match expr {
            Expr::Column(name) => {
                if let Some(val) = tuple.get(name) {
                    hash_value(val, &mut hasher);
                }
            }
            Expr::QualifiedColumn { table, column } => {
                let qualified_name = format!("{}.{}", table, column);
                if let Some(val) = tuple.get(&qualified_name).or_else(|| tuple.get(column)) {
                    hash_value(val, &mut hasher);
                }
            }
            _ => {
                return Err(ExecutorError::UnsupportedExpression(format!(
                    "Unsupported GROUP BY expression: {:?}",
                    expr
                )));
            }
        }
    }
    Ok(hasher.finish())
}
