//! NestedLoopJoinExecutor - Implements nested loop join algorithm
//!
//! This executor performs a nested loop join by buffering the right side
//! and iterating through all left tuples, then for each left tuple,
//! iterating through all buffered right tuples.

use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};

pub struct NestedLoopJoinExecutor {
    left: Box<dyn Executor>,
    right_tuples: Vec<Tuple>,
    right_loaded: bool,
    current_left: Option<Tuple>,
    right_index: usize,
}

impl NestedLoopJoinExecutor {
    /// Create a new NestedLoopJoinExecutor
    ///
    /// # Arguments
    /// * `left` - Left child executor (outer loop)
    /// * `right` - Right child executor (inner loop)
    pub fn new(
        left: Box<dyn Executor>,
        mut right: Box<dyn Executor>,
    ) -> Result<Self, ExecutorError> {
        // Buffer all right tuples
        let mut right_tuples = Vec::new();
        while let Some(tuple) = right.next()? {
            right_tuples.push(tuple);
        }

        Ok(Self { left, right_tuples, right_loaded: true, current_left: None, right_index: 0 })
    }

    /// Merge two tuples into one
    ///
    /// For tuples with overlapping column names, right tuple columns take precedence
    fn merge_tuples(left: &Tuple, right: &Tuple) -> Tuple {
        let mut result = left.clone();
        for (key, value) in right {
            result.insert(key.clone(), value.clone());
        }
        result
    }
}

impl Executor for NestedLoopJoinExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if self.right_tuples.is_empty() {
            return Ok(None);
        }

        // Advance left tuple if needed
        if self.current_left.is_none() || self.right_index >= self.right_tuples.len() {
            self.current_left = self.left.next()?;
            self.right_index = 0;
        }

        // If left is exhausted, we are done
        if self.current_left.is_none() {
            return Ok(None);
        }

        // Produce the next joined tuple
        let right_tuple = &self.right_tuples[self.right_index];
        self.right_index += 1;

        let left_tuple = self.current_left.as_ref().unwrap();
        let result = Self::merge_tuples(left_tuple, right_tuple);

        Ok(Some(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::test_helpers::{MockExecutor, TupleBuilder};

    #[test]
    fn test_nested_loop_join_basic() {
        let left = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("left_id", 1).build(),
            TupleBuilder::new().with_int("left_id", 2).build(),
        ]);

        let right = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_text("right_val", "a").build(),
            TupleBuilder::new().with_text("right_val", "b").build(),
        ]);

        let mut join = NestedLoopJoinExecutor::new(Box::new(left), Box::new(right)).unwrap();

        // Should produce 2x2 = 4 tuples (cross product)
        let t1 = join.next().unwrap().unwrap();
        assert_eq!(t1.get("left_id"), Some(&crate::catalog::Value::Int(1)));
        assert_eq!(t1.get("right_val"), Some(&crate::catalog::Value::Text("a".to_string())));

        let t2 = join.next().unwrap().unwrap();
        assert_eq!(t2.get("left_id"), Some(&crate::catalog::Value::Int(1)));
        assert_eq!(t2.get("right_val"), Some(&crate::catalog::Value::Text("b".to_string())));

        let t3 = join.next().unwrap().unwrap();
        assert_eq!(t3.get("left_id"), Some(&crate::catalog::Value::Int(2)));
        assert_eq!(t3.get("right_val"), Some(&crate::catalog::Value::Text("a".to_string())));

        let t4 = join.next().unwrap().unwrap();
        assert_eq!(t4.get("left_id"), Some(&crate::catalog::Value::Int(2)));
        assert_eq!(t4.get("right_val"), Some(&crate::catalog::Value::Text("b".to_string())));

        assert!(join.next().unwrap().is_none());
    }

    #[test]
    fn test_nested_loop_join_empty_left() {
        let left = MockExecutor::empty();
        let right = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("val", 1).build()]);

        let mut join = NestedLoopJoinExecutor::new(Box::new(left), Box::new(right)).unwrap();
        assert!(join.next().unwrap().is_none());
    }

    #[test]
    fn test_nested_loop_join_empty_right() {
        let left = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("val", 1).build()]);
        let right = MockExecutor::empty();

        let mut join = NestedLoopJoinExecutor::new(Box::new(left), Box::new(right)).unwrap();
        assert!(join.next().unwrap().is_none());
    }

    #[test]
    fn test_nested_loop_join_both_empty() {
        let left = MockExecutor::empty();
        let right = MockExecutor::empty();

        let mut join = NestedLoopJoinExecutor::new(Box::new(left), Box::new(right)).unwrap();
        assert!(join.next().unwrap().is_none());
    }

    #[test]
    fn test_nested_loop_join_single_tuple_each() {
        let left = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("l", 1).build()]);
        let right = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("r", 2).build()]);

        let mut join = NestedLoopJoinExecutor::new(Box::new(left), Box::new(right)).unwrap();

        let result = join.next().unwrap().unwrap();
        assert_eq!(result.get("l"), Some(&crate::catalog::Value::Int(1)));
        assert_eq!(result.get("r"), Some(&crate::catalog::Value::Int(2)));

        assert!(join.next().unwrap().is_none());
    }
}
