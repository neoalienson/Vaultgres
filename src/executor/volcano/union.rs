//! UnionExecutor - Implements UNION and UNION ALL set operations
//!
//! This executor combines tuples from two child executors, optionally
//! removing duplicates for UNION (vs UNION ALL which keeps all).

use crate::catalog::Value;
use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};
use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Union type enumeration
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UnionType {
    /// UNION - removes duplicates
    Distinct,
    /// UNION ALL - keeps all tuples including duplicates
    All,
}

pub struct UnionExecutor {
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    union_type: UnionType,
    seen: HashSet<u64>,
    left_exhausted: bool,
}

impl UnionExecutor {
    /// Create a new UnionExecutor
    ///
    /// # Arguments
    /// * `left` - Left child executor
    /// * `right` - Right child executor
    /// * `union_type` - Type of union (Distinct or All)
    pub fn new(left: Box<dyn Executor>, right: Box<dyn Executor>, union_type: UnionType) -> Self {
        Self { left, right, union_type, seen: HashSet::new(), left_exhausted: false }
    }

    /// Create a UNION (distinct) executor
    pub fn distinct(left: Box<dyn Executor>, right: Box<dyn Executor>) -> Self {
        Self::new(left, right, UnionType::Distinct)
    }

    /// Create a UNION ALL executor
    pub fn all(left: Box<dyn Executor>, right: Box<dyn Executor>) -> Self {
        Self::new(left, right, UnionType::All)
    }

    /// Hash a tuple for duplicate detection
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
    fn hash_value(value: &Value, hasher: &mut DefaultHasher) {
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

impl Executor for UnionExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        // First, exhaust left side
        if !self.left_exhausted {
            while let Some(tuple) = self.left.next()? {
                if self.union_type == UnionType::All {
                    return Ok(Some(tuple));
                }

                let hash = Self::hash_tuple(&tuple);
                if self.seen.insert(hash) {
                    return Ok(Some(tuple));
                }
            }
            self.left_exhausted = true;
        }

        // Then, exhaust right side
        while let Some(tuple) = self.right.next()? {
            if self.union_type == UnionType::All {
                return Ok(Some(tuple));
            }

            let hash = Self::hash_tuple(&tuple);
            if self.seen.insert(hash) {
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
    fn test_union_basic() {
        let left = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 2).build(),
        ]);

        let right = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 3).build(),
            TupleBuilder::new().with_int("val", 4).build(),
        ]);

        let mut union = UnionExecutor::distinct(Box::new(left), Box::new(right));

        let mut results = Vec::new();
        while let Some(tuple) = union.next().unwrap() {
            results.push(tuple);
        }
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn test_union_removes_duplicates() {
        let left = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 2).build(),
        ]);

        let right = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 2).build(),
            TupleBuilder::new().with_int("val", 3).build(),
        ]);

        let mut union = UnionExecutor::distinct(Box::new(left), Box::new(right));

        let mut results = Vec::new();
        while let Some(tuple) = union.next().unwrap() {
            results.push(tuple);
        }
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_union_all_keeps_duplicates() {
        let left = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 2).build(),
        ]);

        let right = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 2).build(),
            TupleBuilder::new().with_int("val", 3).build(),
        ]);

        let mut union = UnionExecutor::all(Box::new(left), Box::new(right));

        let mut results = Vec::new();
        while let Some(tuple) = union.next().unwrap() {
            results.push(tuple);
        }
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn test_union_empty_left() {
        let left = MockExecutor::empty();
        let right = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 2).build(),
        ]);

        let mut union = UnionExecutor::distinct(Box::new(left), Box::new(right));

        let mut results = Vec::new();
        while let Some(tuple) = union.next().unwrap() {
            results.push(tuple);
        }
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_union_empty_right() {
        let left = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 2).build(),
        ]);
        let right = MockExecutor::empty();

        let mut union = UnionExecutor::distinct(Box::new(left), Box::new(right));

        let mut results = Vec::new();
        while let Some(tuple) = union.next().unwrap() {
            results.push(tuple);
        }
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_union_empty_both() {
        let left = MockExecutor::empty();
        let right = MockExecutor::empty();

        let mut union = UnionExecutor::distinct(Box::new(left), Box::new(right));
        assert!(union.next().unwrap().is_none());
    }

    #[test]
    fn test_union_all_duplicates() {
        let left = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 1).build(),
        ]);

        let right = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 1).build(),
        ]);

        let mut union = UnionExecutor::all(Box::new(left), Box::new(right));

        let mut results = Vec::new();
        while let Some(tuple) = union.next().unwrap() {
            results.push(tuple);
        }
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn test_union_distinct_all_duplicates() {
        let left = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 1).build(),
        ]);

        let right = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 1).build(),
        ]);

        let mut union = UnionExecutor::distinct(Box::new(left), Box::new(right));

        let mut results = Vec::new();
        while let Some(tuple) = union.next().unwrap() {
            results.push(tuple);
        }
        assert_eq!(results.len(), 1);
    }
}
