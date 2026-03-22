//! HashJoinExecutor - Implements hash join algorithm
//!
//! This executor performs a hash join by building a hash table from the build side
//! and probing it with tuples from the probe side.

use crate::catalog::Value;
use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};
use std::collections::HashMap;

pub struct HashJoinExecutor {
    build: Box<dyn Executor>,
    probe: Box<dyn Executor>,
    hash_table: HashMap<Vec<u8>, Vec<Tuple>>,
    built: bool,
    probe_buffer: Vec<Tuple>,
    probe_index: usize,
    build_key: String,
    probe_key: String,
}

impl HashJoinExecutor {
    /// Create a new HashJoinExecutor
    ///
    /// # Arguments
    /// * `build` - Build side executor (used to build hash table)
    /// * `probe` - Probe side executor (used to probe hash table)
    /// * `build_key` - Column name for join key in build side
    /// * `probe_key` - Column name for join key in probe side
    pub fn new(
        build: Box<dyn Executor>,
        probe: Box<dyn Executor>,
        build_key: String,
        probe_key: String,
    ) -> Self {
        Self {
            build,
            probe,
            hash_table: HashMap::new(),
            built: false,
            probe_buffer: Vec::new(),
            probe_index: 0,
            build_key,
            probe_key,
        }
    }

    /// Create a new HashJoinExecutor with same key name on both sides
    pub fn with_key(build: Box<dyn Executor>, probe: Box<dyn Executor>, key: String) -> Self {
        Self::new(build, probe, key.clone(), key)
    }

    /// Build the hash table from the build side
    fn build_hash_table(&mut self) -> Result<(), ExecutorError> {
        while let Some(tuple) = self.build.next()? {
            let key = self.extract_key(&tuple, &self.build_key)?;
            self.hash_table.entry(key).or_default().push(tuple);
        }
        self.built = true;
        Ok(())
    }

    /// Extract the join key from a tuple
    fn extract_key(&self, tuple: &Tuple, key_column: &str) -> Result<Vec<u8>, ExecutorError> {
        match tuple.get(key_column) {
            Some(value) => Ok(self.value_to_key(value)),
            None => Err(ExecutorError::ColumnNotFound(key_column.to_string())),
        }
    }

    /// Convert a Value to a key for hash table lookup
    fn value_to_key(&self, value: &Value) -> Vec<u8> {
        match value {
            Value::Int(i) => i.to_le_bytes().to_vec(),
            Value::Float(f) => f.to_le_bytes().to_vec(),
            Value::Text(s) => s.as_bytes().to_vec(),
            Value::Bool(b) => vec![*b as u8],
            Value::Bytea(b) => b.clone(),
            _ => value.to_bytes(),
        }
    }

    /// Merge two tuples into one
    fn merge_tuples(build: &Tuple, probe: &Tuple) -> Tuple {
        let mut result = build.clone();
        for (key, value) in probe {
            result.insert(key.clone(), value.clone());
        }
        result
    }
}

impl Executor for HashJoinExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        // Build hash table on first call
        if !self.built {
            self.build_hash_table()?;
        }

        loop {
            // Return buffered probe results first
            if self.probe_index < self.probe_buffer.len() {
                let result = self.probe_buffer[self.probe_index].clone();
                self.probe_index += 1;
                return Ok(Some(result));
            }

            // Get next probe tuple
            let probe_tuple = match self.probe.next()? {
                Some(t) => t,
                None => return Ok(None), // No more probe tuples
            };

            // Extract key and look up in hash table
            let key = self.extract_key(&probe_tuple, &self.probe_key)?;

            if let Some(build_tuples) = self.hash_table.get(&key) {
                // Buffer all matching build tuples combined with this probe tuple
                self.probe_buffer = build_tuples
                    .iter()
                    .map(|build_tuple| Self::merge_tuples(build_tuple, &probe_tuple))
                    .collect();
                self.probe_index = 0;
            }
            // If no match, continue to next probe tuple (inner join behavior)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::test_helpers::{MockExecutor, TupleBuilder};

    #[test]
    fn test_hash_join_basic() {
        let build = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("id", 1).with_int("build_val", 10).build(),
            TupleBuilder::new().with_int("id", 2).with_int("build_val", 20).build(),
        ]);

        let probe = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("id", 1).with_int("probe_val", 100).build(),
            TupleBuilder::new().with_int("id", 2).with_int("probe_val", 200).build(),
        ]);

        let mut join =
            HashJoinExecutor::with_key(Box::new(build), Box::new(probe), "id".to_string());

        let mut results = Vec::new();
        while let Some(tuple) = join.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);

        // Check first result
        assert_eq!(results[0].get("id"), Some(&Value::Int(1)));
        assert_eq!(results[0].get("build_val"), Some(&Value::Int(10)));
        assert_eq!(results[0].get("probe_val"), Some(&Value::Int(100)));
    }

    #[test]
    fn test_hash_join_no_match() {
        let build = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("id", 1).build()]);

        let probe = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("id", 2).build()]);

        let mut join =
            HashJoinExecutor::with_key(Box::new(build), Box::new(probe), "id".to_string());
        assert!(join.next().unwrap().is_none());
    }

    #[test]
    fn test_hash_join_multiple_matches() {
        // Build side has multiple tuples with same key
        let build = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("id", 1).with_text("b", "x").build(),
            TupleBuilder::new().with_int("id", 1).with_text("b", "y").build(),
        ]);

        let probe = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("id", 1).with_text("p", "a").build(),
        ]);

        let mut join =
            HashJoinExecutor::with_key(Box::new(build), Box::new(probe), "id".to_string());

        let mut results = Vec::new();
        while let Some(tuple) = join.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get("b"), Some(&Value::Text("x".to_string())));
        assert_eq!(results[1].get("b"), Some(&Value::Text("y".to_string())));
    }

    #[test]
    fn test_hash_join_empty_build() {
        let build = MockExecutor::empty();
        let probe = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("id", 1).build()]);

        let mut join =
            HashJoinExecutor::with_key(Box::new(build), Box::new(probe), "id".to_string());
        assert!(join.next().unwrap().is_none());
    }

    #[test]
    fn test_hash_join_empty_probe() {
        let build = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("id", 1).build()]);
        let probe = MockExecutor::empty();

        let mut join =
            HashJoinExecutor::with_key(Box::new(build), Box::new(probe), "id".to_string());
        assert!(join.next().unwrap().is_none());
    }

    #[test]
    fn test_hash_join_text_key() {
        let build = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_text("name", "alice").with_int("age", 30).build(),
            TupleBuilder::new().with_text("name", "bob").with_int("age", 25).build(),
        ]);

        let probe = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_text("name", "alice").with_text("city", "NYC").build(),
            TupleBuilder::new().with_text("name", "charlie").with_text("city", "LA").build(),
        ]);

        let mut join =
            HashJoinExecutor::with_key(Box::new(build), Box::new(probe), "name".to_string());

        let results: Vec<_> =
            std::iter::from_fn(|| join.next().transpose()).collect::<Result<_, _>>().unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("name"), Some(&Value::Text("alice".to_string())));
        assert_eq!(results[0].get("age"), Some(&Value::Int(30)));
        assert_eq!(results[0].get("city"), Some(&Value::Text("NYC".to_string())));
    }
}
