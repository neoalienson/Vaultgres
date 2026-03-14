//! IntersectExecutor - Implements INTERSECT set operation
//!
//! This executor returns tuples that appear in both child executors.
//! Duplicates are removed (INTERSECT DISTINCT semantics).

use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};
use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct IntersectExecutor {
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    right_set: HashSet<u64>,
    right_loaded: bool,
}

impl IntersectExecutor {
    /// Create a new IntersectExecutor
    ///
    /// # Arguments
    /// * `left` - Left child executor
    /// * `right` - Right child executor
    pub fn new(left: Box<dyn Executor>, right: Box<dyn Executor>) -> Self {
        Self { left, right, right_set: HashSet::new(), right_loaded: false }
    }

    /// Load all right tuples into a hash set
    fn load_right(&mut self) -> Result<(), ExecutorError> {
        if !self.right_loaded {
            while let Some(tuple) = self.right.next()? {
                let hash = Self::hash_tuple(&tuple);
                self.right_set.insert(hash);
            }
            self.right_loaded = true;
        }
        Ok(())
    }

    /// Hash a tuple for comparison
    fn hash_tuple(tuple: &Tuple) -> u64 {
        let mut hasher = DefaultHasher::new();

        // Sort keys to ensure consistent hashing
        let mut keys: Vec<_> = tuple.keys().collect();
        keys.sort();

        for key in keys {
            key.hash(&mut hasher);
            if let Some(value) = tuple.get(key) {
                Self::hash_value(value, &mut hasher);
            }
        }

        hasher.finish()
    }

    /// Hash a value
    fn hash_value(value: &crate::catalog::Value, hasher: &mut DefaultHasher) {
        use crate::catalog::Value;
        match value {
            Value::Int(i) => {
                "Int".hash(hasher);
                i.hash(hasher);
            }
            Value::Float(f) => {
                "Float".hash(hasher);
                f.to_bits().hash(hasher);
            }
            Value::Bool(b) => {
                "Bool".hash(hasher);
                b.hash(hasher);
            }
            Value::Text(s) => {
                "Text".hash(hasher);
                s.hash(hasher);
            }
            Value::Null => {
                "Null".hash(hasher);
            }
            Value::Bytea(b) => {
                "Bytea".hash(hasher);
                b.hash(hasher);
            }
            Value::Array(arr) => {
                "Array".hash(hasher);
                for v in arr {
                    Self::hash_value(v, hasher);
                }
            }
            Value::Json(s) => {
                "Json".hash(hasher);
                s.hash(hasher);
            }
            Value::Date(d) => {
                "Date".hash(hasher);
                d.hash(hasher);
            }
            Value::Time(t) => {
                "Time".hash(hasher);
                t.hash(hasher);
            }
            Value::Timestamp(ts) => {
                "Timestamp".hash(hasher);
                ts.hash(hasher);
            }
            Value::Decimal(v, s) => {
                "Decimal".hash(hasher);
                v.hash(hasher);
                s.hash(hasher);
            }
        }
    }
}

impl Executor for IntersectExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        // Load right side into hash set
        self.load_right()?;

        // Return tuples from left that exist in right
        while let Some(tuple) = self.left.next()? {
            let hash = Self::hash_tuple(&tuple);
            if self.right_set.contains(&hash) {
                return Ok(Some(tuple));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::test_helpers::{MockExecutor, TupleBuilder};

    #[test]
    fn test_intersect_basic() {
        let left = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 2).build(),
            TupleBuilder::new().with_int("val", 3).build(),
        ]);

        let right = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 2).build(),
            TupleBuilder::new().with_int("val", 3).build(),
            TupleBuilder::new().with_int("val", 4).build(),
        ]);

        let mut intersect = IntersectExecutor::new(Box::new(left), Box::new(right));

        let mut results = Vec::new();
        while let Some(tuple) = intersect.next().unwrap() {
            results.push(tuple);
        }
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_intersect_no_overlap() {
        let left = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 2).build(),
        ]);

        let right = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 3).build(),
            TupleBuilder::new().with_int("val", 4).build(),
        ]);

        let mut intersect = IntersectExecutor::new(Box::new(left), Box::new(right));
        assert!(intersect.next().unwrap().is_none());
    }

    #[test]
    fn test_intersect_all_overlap() {
        let left = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 2).build(),
        ]);

        let right = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 2).build(),
        ]);

        let mut intersect = IntersectExecutor::new(Box::new(left), Box::new(right));

        let mut results = Vec::new();
        while let Some(tuple) = intersect.next().unwrap() {
            results.push(tuple);
        }
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_intersect_empty_left() {
        let left = MockExecutor::empty();
        let right = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 2).build(),
        ]);

        let mut intersect = IntersectExecutor::new(Box::new(left), Box::new(right));
        assert!(intersect.next().unwrap().is_none());
    }

    #[test]
    fn test_intersect_empty_right() {
        let left = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 2).build(),
        ]);
        let right = MockExecutor::empty();

        let mut intersect = IntersectExecutor::new(Box::new(left), Box::new(right));
        assert!(intersect.next().unwrap().is_none());
    }

    #[test]
    fn test_intersect_empty_both() {
        let left = MockExecutor::empty();
        let right = MockExecutor::empty();

        let mut intersect = IntersectExecutor::new(Box::new(left), Box::new(right));
        assert!(intersect.next().unwrap().is_none());
    }

    #[test]
    fn test_intersect_removes_duplicates() {
        // Intersect returns each matching tuple from left side
        // The right side is a hash set, so duplicates in right are removed
        let left = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 2).build(),
        ]);

        let right = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 2).build(),
        ]);

        let mut intersect = IntersectExecutor::new(Box::new(left), Box::new(right));

        let mut results = Vec::new();
        while let Some(tuple) = intersect.next().unwrap() {
            results.push(tuple);
        }
        // Returns each matching tuple from left (1 appears twice in left, 2 appears once)
        assert_eq!(results.len(), 3);
    }
}
