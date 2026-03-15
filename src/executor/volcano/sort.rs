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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{TableSchema, Value};
    use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};
    use crate::parser::ast::{ColumnDef, DataType, OrderByExpr};
    use std::collections::HashMap;

    struct MockExecutor {
        tuples: Vec<Tuple>,
        idx: usize,
    }

    impl MockExecutor {
        fn new(tuples: Vec<Tuple>) -> Self {
            Self { tuples, idx: 0 }
        }
    }

    impl Executor for MockExecutor {
        fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
            if self.idx >= self.tuples.len() {
                Ok(None)
            } else {
                self.idx += 1;
                Ok(Some(self.tuples[self.idx - 1].clone()))
            }
        }
    }

    fn create_test_schema() -> TableSchema {
        TableSchema::new(
            "test".to_string(),
            vec![
                ColumnDef {
                    name: "a".to_string(),
                    data_type: DataType::Int,
                    is_primary_key: false,
                    is_unique: false,
                    is_auto_increment: false,
                    is_not_null: false,
                    default_value: None,
                    foreign_key: None,
                },
                ColumnDef {
                    name: "b".to_string(),
                    data_type: DataType::Text,
                    is_primary_key: false,
                    is_unique: false,
                    is_auto_increment: false,
                    is_not_null: false,
                    default_value: None,
                    foreign_key: None,
                },
            ],
        )
    }

    #[test]
    fn test_sort_asc() {
        let tuples = vec![
            [("a".to_string(), Value::Int(3))].into(),
            [("a".to_string(), Value::Int(1))].into(),
            [("a".to_string(), Value::Int(2))].into(),
        ];
        let child = Box::new(MockExecutor::new(tuples));
        let order_by = vec![OrderByExpr { column: "a".to_string(), ascending: true }];
        let schema = create_test_schema();

        let mut sort_executor = SortExecutor::new(child, order_by, schema).unwrap();

        assert_eq!(sort_executor.next().unwrap().unwrap().get("a"), Some(&Value::Int(1)));
        assert_eq!(sort_executor.next().unwrap().unwrap().get("a"), Some(&Value::Int(2)));
        assert_eq!(sort_executor.next().unwrap().unwrap().get("a"), Some(&Value::Int(3)));
        assert!(sort_executor.next().unwrap().is_none());
    }

    #[test]
    fn test_sort_desc() {
        let tuples = vec![
            [("a".to_string(), Value::Int(3))].into(),
            [("a".to_string(), Value::Int(1))].into(),
            [("a".to_string(), Value::Int(2))].into(),
        ];
        let child = Box::new(MockExecutor::new(tuples));
        let order_by = vec![OrderByExpr { column: "a".to_string(), ascending: false }];
        let schema = create_test_schema();

        let mut sort_executor = SortExecutor::new(child, order_by, schema).unwrap();

        assert_eq!(sort_executor.next().unwrap().unwrap().get("a"), Some(&Value::Int(3)));
        assert_eq!(sort_executor.next().unwrap().unwrap().get("a"), Some(&Value::Int(2)));
        assert_eq!(sort_executor.next().unwrap().unwrap().get("a"), Some(&Value::Int(1)));
        assert!(sort_executor.next().unwrap().is_none());
    }

    #[test]
    fn test_sort_multiple_columns() {
        let tuples = vec![
            [("a".to_string(), Value::Int(1)), ("b".to_string(), Value::Text("z".to_string()))]
                .into(),
            [("a".to_string(), Value::Int(2)), ("b".to_string(), Value::Text("y".to_string()))]
                .into(),
            [("a".to_string(), Value::Int(1)), ("b".to_string(), Value::Text("x".to_string()))]
                .into(),
        ];
        let child = Box::new(MockExecutor::new(tuples));
        let order_by = vec![
            OrderByExpr { column: "a".to_string(), ascending: true },
            OrderByExpr { column: "b".to_string(), ascending: true },
        ];
        let schema = create_test_schema();

        let mut sort_executor = SortExecutor::new(child, order_by, schema).unwrap();

        let r1 = sort_executor.next().unwrap().unwrap();
        assert_eq!(r1.get("a"), Some(&Value::Int(1)));
        assert_eq!(r1.get("b"), Some(&Value::Text("x".to_string())));

        let r2 = sort_executor.next().unwrap().unwrap();
        assert_eq!(r2.get("a"), Some(&Value::Int(1)));
        assert_eq!(r2.get("b"), Some(&Value::Text("z".to_string())));

        let r3 = sort_executor.next().unwrap().unwrap();
        assert_eq!(r3.get("a"), Some(&Value::Int(2)));
        assert_eq!(r3.get("b"), Some(&Value::Text("y".to_string())));

        assert!(sort_executor.next().unwrap().is_none());
    }

    #[test]
    fn test_sort_with_nulls() {
        let tuples = vec![
            [("a".to_string(), Value::Int(3))].into(),
            [("a".to_string(), Value::Null)].into(),
            [("a".to_string(), Value::Int(1))].into(),
        ];
        let child = Box::new(MockExecutor::new(tuples));
        let order_by = vec![OrderByExpr { column: "a".to_string(), ascending: true }];
        let schema = create_test_schema();

        let mut sort_executor = SortExecutor::new(child, order_by, schema).unwrap();

        assert_eq!(sort_executor.next().unwrap().unwrap().get("a"), Some(&Value::Int(1)));
        assert_eq!(sort_executor.next().unwrap().unwrap().get("a"), Some(&Value::Int(3)));
        assert_eq!(sort_executor.next().unwrap().unwrap().get("a"), Some(&Value::Null));
        assert!(sort_executor.next().unwrap().is_none());
    }

    #[test]
    fn test_sort_empty() {
        let tuples = vec![];
        let child = Box::new(MockExecutor::new(tuples));
        let order_by = vec![OrderByExpr { column: "a".to_string(), ascending: true }];
        let schema = create_test_schema();

        let mut sort_executor = SortExecutor::new(child, order_by, schema).unwrap();
        assert!(sort_executor.next().unwrap().is_none());
    }
}
