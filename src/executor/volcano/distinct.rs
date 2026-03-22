//! DistinctExecutor - Removes duplicate tuples

use crate::catalog::Value;
use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};
use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct DistinctExecutor {
    seen: HashSet<u64>,
    buffered_tuples: Vec<Tuple>,
    current_idx: usize,
}

impl DistinctExecutor {
    pub fn new(mut child: Box<dyn Executor>) -> Result<Self, ExecutorError> {
        // Buffer all tuples and filter duplicates
        let mut seen = HashSet::new();
        let mut buffered_tuples = Vec::new();

        while let Some(tuple) = child.next()? {
            let hash = Self::hash_tuple(&tuple);
            if seen.insert(hash) {
                buffered_tuples.push(tuple);
            }
        }

        Ok(Self { seen, buffered_tuples, current_idx: 0 })
    }

    /// Create a hash of a tuple for duplicate detection
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
            Value::Int(n) => {
                "int".hash(hasher);
                n.hash(hasher);
            }
            Value::Text(s) => {
                "text".hash(hasher);
                s.hash(hasher);
            }
            Value::Bool(b) => {
                "bool".hash(hasher);
                b.hash(hasher);
            }
            Value::Null => {
                "null".hash(hasher);
            }
            // For other types, use their debug representation
            _ => {
                format!("{:?}", value).hash(hasher);
            }
        }
    }
}

impl Executor for DistinctExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
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
    use crate::catalog::Value;
    use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};

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

    #[test]
    fn test_distinct_with_duplicates() {
        let tuples = vec![
            [("a".to_string(), Value::Int(1)), ("b".to_string(), Value::Text("x".to_string()))]
                .into(),
            [("a".to_string(), Value::Int(2)), ("b".to_string(), Value::Text("y".to_string()))]
                .into(),
            [("a".to_string(), Value::Int(1)), ("b".to_string(), Value::Text("x".to_string()))]
                .into(),
        ];
        let child = Box::new(MockExecutor::new(tuples));
        let mut distinct_executor = DistinctExecutor::new(child).unwrap();

        assert!(distinct_executor.next().unwrap().is_some());
        assert!(distinct_executor.next().unwrap().is_some());
        assert!(distinct_executor.next().unwrap().is_none());
    }

    #[test]
    fn test_distinct_no_duplicates() {
        let tuples = vec![
            [("a".to_string(), Value::Int(1))].into(),
            [("a".to_string(), Value::Int(2))].into(),
            [("a".to_string(), Value::Int(3))].into(),
        ];
        let child = Box::new(MockExecutor::new(tuples));
        let mut distinct_executor = DistinctExecutor::new(child).unwrap();

        assert!(distinct_executor.next().unwrap().is_some());
        assert!(distinct_executor.next().unwrap().is_some());
        assert!(distinct_executor.next().unwrap().is_some());
        assert!(distinct_executor.next().unwrap().is_none());
    }

    #[test]
    fn test_distinct_empty() {
        let tuples = vec![];
        let child = Box::new(MockExecutor::new(tuples));
        let mut distinct_executor = DistinctExecutor::new(child).unwrap();
        assert!(distinct_executor.next().unwrap().is_none());
    }

    #[test]
    fn test_distinct_different_column_order() {
        let tuples = vec![
            [("a".to_string(), Value::Int(1)), ("b".to_string(), Value::Text("x".to_string()))]
                .into(),
            [("b".to_string(), Value::Text("x".to_string())), ("a".to_string(), Value::Int(1))]
                .into(),
        ];
        let child = Box::new(MockExecutor::new(tuples));
        let mut distinct_executor = DistinctExecutor::new(child).unwrap();

        assert!(distinct_executor.next().unwrap().is_some());
        assert!(distinct_executor.next().unwrap().is_none());
    }

    #[test]
    fn test_distinct_with_nulls() {
        let tuples = vec![
            [("a".to_string(), Value::Null)].into(),
            [("a".to_string(), Value::Int(1))].into(),
            [("a".to_string(), Value::Null)].into(),
        ];
        let child = Box::new(MockExecutor::new(tuples));
        let mut distinct_executor = DistinctExecutor::new(child).unwrap();

        assert!(distinct_executor.next().unwrap().is_some());
        assert!(distinct_executor.next().unwrap().is_some());
        assert!(distinct_executor.next().unwrap().is_none());
    }
}
