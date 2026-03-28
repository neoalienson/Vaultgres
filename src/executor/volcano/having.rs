//! HavingExecutor - Filters grouped tuples based on a HAVING condition
//!
//! This executor filters tuples after aggregation based on a HAVING clause condition.
//! It works similarly to a filter but is typically used after GROUP BY operations.

use crate::catalog::{Catalog, Value};
use crate::executor::eval::Eval;
use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};
use crate::parser::ast::Expr;
use std::sync::Arc;

pub struct HavingExecutor {
    child: Box<dyn Executor>,
    condition: Expr,
    catalog: Option<Arc<Catalog>>,
}

impl HavingExecutor {
    /// Create a new HavingExecutor
    ///
    /// # Arguments
    /// * `child` - The child executor (typically an aggregation executor)
    /// * `condition` - The HAVING condition expression
    pub fn new(child: Box<dyn Executor>, condition: Expr) -> Self {
        Self { child, condition, catalog: None }
    }

    /// Set the catalog for expression evaluation
    pub fn with_catalog(mut self, catalog: Arc<Catalog>) -> Self {
        self.catalog = Some(catalog);
        self
    }
}

impl Executor for HavingExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        loop {
            match self.child.next()? {
                None => return Ok(None),
                Some(tuple) => {
                    // Evaluate the HAVING condition
                    let result = if let Some(ref catalog) = self.catalog {
                        Eval::eval_expr_with_catalog(
                            &self.condition,
                            &tuple,
                            Some(catalog.as_ref()),
                            None,
                            None,
                        )?
                    } else {
                        Eval::eval_expr(&self.condition, &tuple)?
                    };

                    // Check if the condition is satisfied
                    match result {
                        Value::Bool(matches) => {
                            if matches {
                                return Ok(Some(tuple));
                            }
                            // If false, continue to next tuple
                        }
                        _ => {
                            return Err(ExecutorError::TypeMismatch(
                                "HAVING condition did not evaluate to a boolean".to_string(),
                            ));
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Value;
    use crate::executor::test_helpers::{MockExecutor, TupleBuilder};
    use crate::parser::ast::{BinaryOperator, Expr};

    #[test]
    fn test_having_basic() {
        // Create input tuples with aggregated data (count, sum)
        let input = MockExecutor::with_tuples(vec![
            TupleBuilder::new()
                .with_int("group_key", 1)
                .with_int("count", 3)
                .with_int("sum", 60)
                .build(),
            TupleBuilder::new()
                .with_int("group_key", 2)
                .with_int("count", 1)
                .with_int("sum", 10)
                .build(),
            TupleBuilder::new()
                .with_int("group_key", 3)
                .with_int("count", 5)
                .with_int("sum", 150)
                .build(),
        ]);

        // HAVING count > 2
        let condition = Expr::BinaryOp {
            left: Box::new(Expr::Column("count".to_string())),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expr::Number(2)),
        };

        let mut having = HavingExecutor::new(Box::new(input), condition);

        let t1 = having.next().unwrap().unwrap();
        assert_eq!(t1.get("group_key"), Some(&Value::Int(1)));
        assert_eq!(t1.get("count"), Some(&Value::Int(3)));

        let t2 = having.next().unwrap().unwrap();
        assert_eq!(t2.get("group_key"), Some(&Value::Int(3)));
        assert_eq!(t2.get("count"), Some(&Value::Int(5)));

        assert!(having.next().unwrap().is_none());
    }

    #[test]
    fn test_having_empty_input() {
        let input = MockExecutor::empty();

        let condition = Expr::BinaryOp {
            left: Box::new(Expr::Column("count".to_string())),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expr::Number(0)),
        };

        let mut having = HavingExecutor::new(Box::new(input), condition);
        assert!(having.next().unwrap().is_none());
    }

    #[test]
    fn test_having_no_matches() {
        let input = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("count", 1).build(),
            TupleBuilder::new().with_int("count", 2).build(),
        ]);

        // HAVING count > 100
        let condition = Expr::BinaryOp {
            left: Box::new(Expr::Column("count".to_string())),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expr::Number(100)),
        };

        let mut having = HavingExecutor::new(Box::new(input), condition);
        assert!(having.next().unwrap().is_none());
    }

    #[test]
    fn test_having_all_match() {
        let input = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("count", 50).build(),
            TupleBuilder::new().with_int("count", 60).build(),
            TupleBuilder::new().with_int("count", 70).build(),
        ]);

        // HAVING count > 10
        let condition = Expr::BinaryOp {
            left: Box::new(Expr::Column("count".to_string())),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expr::Number(10)),
        };

        let mut having = HavingExecutor::new(Box::new(input), condition);

        let mut count = 0;
        while having.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn test_having_equality() {
        let input = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("count", 42).build(),
            TupleBuilder::new().with_int("count", 42).build(),
            TupleBuilder::new().with_int("count", 43).build(),
        ]);

        // HAVING count = 42
        let condition = Expr::BinaryOp {
            left: Box::new(Expr::Column("count".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(42)),
        };

        let mut having = HavingExecutor::new(Box::new(input), condition);

        let mut count = 0;
        while having.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 2);
    }

    #[test]
    fn test_having_sum_condition() {
        let input = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("sum", 30).build(),
            TupleBuilder::new().with_int("sum", 10).build(),
            TupleBuilder::new().with_int("sum", 50).build(),
        ]);

        // HAVING sum > 20
        let condition = Expr::BinaryOp {
            left: Box::new(Expr::Column("sum".to_string())),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expr::Number(20)),
        };

        let mut having = HavingExecutor::new(Box::new(input), condition);

        let t1 = having.next().unwrap().unwrap();
        assert_eq!(t1.get("sum"), Some(&Value::Int(30)));

        let t2 = having.next().unwrap().unwrap();
        assert_eq!(t2.get("sum"), Some(&Value::Int(50)));

        assert!(having.next().unwrap().is_none());
    }

    #[test]
    fn test_having_with_catalog() {
        use crate::catalog::Catalog;

        let input = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("value", 100).build(),
            TupleBuilder::new().with_int("value", 50).build(),
        ]);

        // HAVING value >= 100
        let condition = Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::GreaterThanOrEqual,
            right: Box::new(Expr::Number(100)),
        };

        let catalog = Arc::new(Catalog::new());
        let mut having = HavingExecutor::new(Box::new(input), condition).with_catalog(catalog);

        let result = having.next().unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().get("value"), Some(&Value::Int(100)));

        assert!(having.next().unwrap().is_none());
    }
}
