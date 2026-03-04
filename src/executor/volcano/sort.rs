//! SortExecutor - Sorts tuples based on ORDER BY expressions

use crate::catalog::Value;
use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};
use crate::parser::ast::OrderByExpr;

pub struct SortExecutor {
    buffered_tuples: Vec<Tuple>,
    order_by: Vec<OrderByExpr>,
    current_idx: usize,
    schema: crate::catalog::TableSchema,
}

impl SortExecutor {
    pub fn new(
        mut child: Box<dyn Executor>,
        order_by: Vec<OrderByExpr>,
        schema: crate::catalog::TableSchema,
    ) -> Result<Self, ExecutorError> {
        // Buffer all tuples from child
        let mut buffered_tuples = Vec::new();
        while let Some(tuple) = child.next()? {
            buffered_tuples.push(tuple);
        }

        // Validate ORDER BY columns exist in schema
        for order in &order_by {
            if !schema.columns.iter().any(|col| col.name == order.column) {
                return Err(ExecutorError::ColumnNotFound(order.column.clone()));
            }
        }

        Ok(Self { buffered_tuples, order_by, current_idx: 0, schema })
    }

    /// Compare two tuples based on ORDER BY expressions
    fn compare_tuples(
        a: &Tuple,
        b: &Tuple,
        order_by: &[OrderByExpr],
    ) -> Result<std::cmp::Ordering, ExecutorError> {
        for order in order_by {
            let col_name = &order.column;

            let val_a =
                a.get(col_name).ok_or_else(|| ExecutorError::ColumnNotFound(col_name.clone()))?;
            let val_b =
                b.get(col_name).ok_or_else(|| ExecutorError::ColumnNotFound(col_name.clone()))?;

            let cmp = Self::compare_values(val_a, val_b)?;

            // Adjust for ascending/descending
            let adjusted_cmp = if order.ascending { cmp } else { cmp.reverse() };

            if adjusted_cmp != std::cmp::Ordering::Equal {
                return Ok(adjusted_cmp);
            }
        }
        Ok(std::cmp::Ordering::Equal)
    }

    /// Compare two values
    fn compare_values(a: &Value, b: &Value) -> Result<std::cmp::Ordering, ExecutorError> {
        match (a, b) {
            (Value::Int(a), Value::Int(b)) => Ok(a.cmp(b)),
            (Value::Text(a), Value::Text(b)) => Ok(a.cmp(b)),
            (Value::Bool(a), Value::Bool(b)) => Ok(a.cmp(b)),
            (Value::Null, Value::Null) => Ok(std::cmp::Ordering::Equal),
            // NULLs are sorted last
            (Value::Null, _) => Ok(std::cmp::Ordering::Greater),
            (_, Value::Null) => Ok(std::cmp::Ordering::Less),
            _ => Err(ExecutorError::TypeMismatch("Cannot compare different types".to_string())),
        }
    }
}

impl Executor for SortExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        // Sort on first call (lazy sorting)
        if self.current_idx == 0 && !self.buffered_tuples.is_empty() {
            let order_by = self.order_by.clone();
            self.buffered_tuples.sort_by(|a, b| {
                Self::compare_tuples(a, b, &order_by).unwrap_or(std::cmp::Ordering::Equal)
            });
        }

        if self.current_idx >= self.buffered_tuples.len() {
            return Ok(None);
        }

        let tuple = self.buffered_tuples[self.current_idx].clone();
        self.current_idx += 1;
        Ok(Some(tuple))
    }
}
