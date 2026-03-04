use super::old_executor::{OldExecutor as Executor, OldExecutorError as ExecutorError, Tuple};
use crate::parser::Expr;

pub struct Filter {
    child: Box<dyn Executor>,
    predicate: Expr,
}

impl Filter {
    pub fn new(child: Box<dyn Executor>, predicate: Expr) -> Self {
        Self { child, predicate }
    }

    fn eval_predicate(&self, tuple: &Tuple) -> Result<bool, ExecutorError> {
        match &self.predicate {
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.eval_expr(left, tuple)?;
                let right_val = self.eval_expr(right, tuple)?;

                use crate::parser::BinaryOperator;
                match op {
                    BinaryOperator::Equals => Ok(left_val == right_val),
                    BinaryOperator::NotEquals => Ok(left_val != right_val),
                    BinaryOperator::LessThan => Ok(left_val < right_val),
                    BinaryOperator::LessThanOrEqual => Ok(left_val <= right_val),
                    BinaryOperator::GreaterThan => Ok(left_val > right_val),
                    BinaryOperator::GreaterThanOrEqual => Ok(left_val >= right_val),
                    BinaryOperator::And => Ok(left_val == b"1" && right_val == b"1"),
                    BinaryOperator::Or => Ok(left_val == b"1" || right_val == b"1"),
                    BinaryOperator::Like
                    | BinaryOperator::ILike
                    | BinaryOperator::In
                    | BinaryOperator::Any
                    | BinaryOperator::All
                    | BinaryOperator::Some
                    | BinaryOperator::Between
                    | BinaryOperator::Add
                    | BinaryOperator::StringConcat => Err(ExecutorError::TypeMismatch(
                        "Operator not yet supported in executor".to_string(),
                    )),
                }
            }
            _ => Err(ExecutorError::TypeMismatch("Invalid predicate".to_string())),
        }
    }

    fn eval_expr(&self, expr: &Expr, tuple: &Tuple) -> Result<Vec<u8>, ExecutorError> {
        match expr {
            Expr::Column(name) => {
                tuple.get(name).cloned().ok_or_else(|| ExecutorError::ColumnNotFound(name.clone()))
            }
            Expr::Number(n) => Ok(n.to_string().into_bytes()),
            Expr::String(s) => Ok(s.as_bytes().to_vec()),
            _ => Err(ExecutorError::TypeMismatch("Unsupported expression".to_string())),
        }
    }
}

impl Executor for Filter {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.child.open()
    }

    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        loop {
            match self.child.next()? {
                None => return Ok(None),
                Some(tuple) => {
                    if self.eval_predicate(&tuple)? {
                        return Ok(Some(tuple));
                    }
                }
            }
        }
    }

    fn close(&mut self) -> Result<(), ExecutorError> {
        self.child.close()
    }
}

#[cfg(test)]
mod tests {
    use crate::executor::test_helpers::OldMockExecutor;
    use crate::executor::old_executor::SimpleTuple;
    use super::*;
    use crate::parser::Expr;
    use std::collections::HashMap;

    struct MockExecutor {
        tuples: Vec<Tuple>,
        index: usize,
    }

    impl MockExecutor {
        fn new(tuples: Vec<Tuple>) -> Self {
            Self { tuples, index: 0 }
        }
    }

    impl Executor for MockExecutor {
        fn open(&mut self) -> Result<(), ExecutorError> {
            self.index = 0;
            Ok(())
        }

        fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
            if self.index < self.tuples.len() {
                let tuple = self.tuples[self.index].clone();
                self.index += 1;
                Ok(Some(tuple))
            } else {
                Ok(None)
            }
        }

        fn close(&mut self) -> Result<(), ExecutorError> {
            Ok(())
        }
    }

    #[test]
    fn test_filter_equality() {
        let mut t1 = HashMap::new();
        t1.insert("id".to_string(), b"1".to_vec());
        let mut t2 = HashMap::new();
        t2.insert("id".to_string(), b"2".to_vec());

        let mock = OldMockExecutor::new(vec![t1, t2]);
        let predicate = Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: crate::parser::BinaryOperator::Equals,
            right: Box::new(Expr::Number(1)),
        };

        let mut filter = Filter::new(Box::new(mock), predicate);
        filter.open().unwrap();

        let result = filter.next().unwrap().unwrap();
        assert_eq!(result.get("id").unwrap(), b"1");
        assert!(filter.next().unwrap().is_none());
    }
}
