//! JoinExecutor - Implements various join types (Inner, Left, Right, Full)
//!
//! This executor performs joins using a nested loop approach with support
//! for different join types and a custom join condition.
//!
//! Column Prefixing:
//! When joining tables, columns are prefixed with their table alias to avoid
//! ambiguity. For example, joining customers (alias c) and orders (alias o):
//! - Left tuple: {"c.id": 1, "c.name": "Alice"}
//! - Right tuple: {"o.id": 5, "o.total": 100}
//! - Merged: {"c.id": 1, "c.name": "Alice", "o.id": 5, "o.total": 100}

use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};

/// Join type enumeration
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

pub struct JoinExecutor {
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    join_type: JoinType,
    condition: Box<dyn Fn(&Tuple, &Tuple) -> bool + Send>,
    left_tuple: Option<Tuple>,
    right_tuples: Vec<Tuple>,
    right_index: usize,
    right_loaded: bool,
    found_match: bool,
    right_matched: Vec<bool>,
    emitting_unmatched_right: bool,
    unmatched_right_index: usize,
    // Table aliases for column prefixing
    left_alias: String,
    right_alias: String,
}

impl JoinExecutor {
    /// Create a new JoinExecutor with column prefixing
    ///
    /// # Arguments
    /// * `left` - Left child executor
    /// * `right` - Right child executor
    /// * `join_type` - Type of join (Inner, Left, Right, Full)
    /// * `condition` - Join condition function that takes left and right tuples
    /// * `left_alias` - Table alias for left side (for column prefixing)
    /// * `right_alias` - Table alias for right side (for column prefixing)
    pub fn new(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        join_type: JoinType,
        condition: Box<dyn Fn(&Tuple, &Tuple) -> bool + Send>,
        left_alias: String,
        right_alias: String,
    ) -> Self {
        Self {
            left,
            right,
            join_type,
            condition,
            left_tuple: None,
            right_tuples: Vec::new(),
            right_index: 0,
            right_loaded: false,
            found_match: false,
            right_matched: Vec::new(),
            emitting_unmatched_right: false,
            unmatched_right_index: 0,
            left_alias,
            right_alias,
        }
    }

    /// Create an Inner Join
    pub fn inner(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        condition: Box<dyn Fn(&Tuple, &Tuple) -> bool + Send>,
        left_alias: String,
        right_alias: String,
    ) -> Self {
        Self::new(left, right, JoinType::Inner, condition, left_alias, right_alias)
    }

    /// Create a Left Join
    pub fn left(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        condition: Box<dyn Fn(&Tuple, &Tuple) -> bool + Send>,
        left_alias: String,
        right_alias: String,
    ) -> Self {
        Self::new(left, right, JoinType::Left, condition, left_alias, right_alias)
    }

    /// Create a Right Join
    pub fn right(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        condition: Box<dyn Fn(&Tuple, &Tuple) -> bool + Send>,
        left_alias: String,
        right_alias: String,
    ) -> Self {
        Self::new(left, right, JoinType::Right, condition, left_alias, right_alias)
    }

    /// Create a Full Outer Join
    pub fn full(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        condition: Box<dyn Fn(&Tuple, &Tuple) -> bool + Send>,
        left_alias: String,
        right_alias: String,
    ) -> Self {
        Self::new(left, right, JoinType::Full, condition, left_alias, right_alias)
    }

    /// Load all right tuples into memory
    fn load_right(&mut self) -> Result<(), ExecutorError> {
        if !self.right_loaded {
            while let Some(tuple) = self.right.next()? {
                self.right_tuples.push(tuple);
            }
            self.right_matched = vec![false; self.right_tuples.len()];
            self.right_loaded = true;
        }
        Ok(())
    }

    /// Prefix tuple column names with table alias
    fn prefix_tuple(tuple: &Tuple, alias: &str) -> Tuple {
        tuple.iter().map(|(k, v)| (format!("{}.{}", alias, k), v.clone())).collect()
    }

    /// Merge two tuples into one with column prefixing
    fn merge_tuples(&self, left: &Tuple, right: &Tuple) -> Tuple {
        let mut result = Self::prefix_tuple(left, &self.left_alias);
        for (key, value) in right {
            result.insert(format!("{}.{}", self.right_alias, key), value.clone());
        }
        result
    }

    /// Create a tuple with NULL values for right side columns (with prefixing)
    fn left_only_tuple(&self, left: &Tuple, right_schema: &[String]) -> Tuple {
        let mut result = Self::prefix_tuple(left, &self.left_alias);
        for col in right_schema {
            result.insert(format!("{}.{}", self.right_alias, col), crate::catalog::Value::Null);
        }
        result
    }

    /// Create a tuple with NULL values for left side columns (with prefixing)
    fn right_only_tuple(&self, right: &Tuple, left_schema: &[String]) -> Tuple {
        let mut result: Tuple = left_schema
            .iter()
            .map(|col| (format!("{}.{}", self.left_alias, col), crate::catalog::Value::Null))
            .collect();
        for (key, value) in right {
            result.insert(format!("{}.{}", self.right_alias, key), value.clone());
        }
        result
    }

    /// Get column names from first right tuple (for schema inference)
    fn right_alias_columns(&self) -> Vec<String> {
        if let Some(tuple) = self.right_tuples.first() {
            tuple.keys().map(|k| format!("{}.{}", self.right_alias, k)).collect()
        } else {
            vec![]
        }
    }

    /// Get column names from first left tuple (for schema inference)
    fn left_alias_columns(&self) -> Vec<String> {
        if let Some(tuple) = self.left_tuple.as_ref() {
            tuple.keys().map(|k| format!("{}.{}", self.left_alias, k)).collect()
        } else {
            vec![]
        }
    }
}

impl Executor for JoinExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        self.load_right()?;

        // For RIGHT and FULL joins, emit unmatched right tuples after processing all left tuples
        if self.emitting_unmatched_right {
            while self.unmatched_right_index < self.right_tuples.len() {
                let idx = self.unmatched_right_index;
                self.unmatched_right_index += 1;
                if !self.right_matched[idx] {
                    // For FULL join, need to include NULL left columns
                    // Return right tuple with NULL left columns
                    let right = &self.right_tuples[idx];
                    let left_columns: Vec<String> = self.left_alias_columns();
                    return Ok(Some(self.right_only_tuple(right, &left_columns)));
                }
            }
            return Ok(None);
        }

        loop {
            // Get next left tuple if needed
            if self.left_tuple.is_none() {
                self.left_tuple = self.left.next()?;
                self.right_index = 0;
                self.found_match = false;

                if self.left_tuple.is_none() {
                    // For RIGHT and FULL joins, start emitting unmatched right tuples
                    if matches!(self.join_type, JoinType::Right | JoinType::Full) {
                        self.emitting_unmatched_right = true;
                        return self.next();
                    }
                    return Ok(None);
                }
            }

            let left = self.left_tuple.as_ref().unwrap();

            // Scan through right tuples looking for matches
            while self.right_index < self.right_tuples.len() {
                let right_idx = self.right_index;
                let right = &self.right_tuples[right_idx];
                self.right_index += 1;

                if (self.condition)(left, right) {
                    self.found_match = true;
                    self.right_matched[right_idx] = true;
                    return Ok(Some(self.merge_tuples(left, right)));
                }
            }

            // Handle LEFT/FULL joins - emit left tuple with NULL right if no match
            if matches!(self.join_type, JoinType::Left | JoinType::Full) && !self.found_match {
                // Return left tuple with NULL right columns
                let left_prefixed = Self::prefix_tuple(left, &self.left_alias);
                let right_columns: Vec<String> = self.right_alias_columns();
                let result = self.left_only_tuple(&left_prefixed, &right_columns);
                self.left_tuple = None;
                return Ok(Some(result));
            }

            // Move to next left tuple
            self.left_tuple = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::test_helpers::{MockExecutor, TupleBuilder};

    #[test]
    fn test_inner_join_basic() {
        let left = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("id", 1).with_text("lval", "a").build(),
            TupleBuilder::new().with_int("id", 2).with_text("lval", "b").build(),
        ]);

        let right = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("id", 1).with_text("rval", "x").build(),
            TupleBuilder::new().with_int("id", 3).with_text("rval", "y").build(),
        ]);

        let condition = |l: &Tuple, r: &Tuple| l.get("id") == r.get("id");

        let mut join = JoinExecutor::inner(
            Box::new(left),
            Box::new(right),
            Box::new(condition),
            "l".to_string(),
            "r".to_string(),
        );

        let results: Vec<_> =
            std::iter::from_fn(|| join.next().transpose()).collect::<Result<_, _>>().unwrap();

        assert_eq!(results.len(), 1);
        // With column prefixing, columns should be prefixed
        assert_eq!(results[0].get("l.id"), Some(&crate::catalog::Value::Int(1)));
        assert_eq!(results[0].get("l.lval"), Some(&crate::catalog::Value::Text("a".to_string())));
        assert_eq!(results[0].get("r.id"), Some(&crate::catalog::Value::Int(1)));
        assert_eq!(results[0].get("r.rval"), Some(&crate::catalog::Value::Text("x".to_string())));
    }

    #[test]
    fn test_column_prefixing_with_duplicate_names() {
        // Test that columns with same name from different tables are preserved
        let left = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("id", 1).with_text("name", "Alice").build(),
        ]);

        let right = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("id", 1).with_text("name", "Order1").build(),
        ]);

        // Condition should work with unprefixed columns (they get looked up in the combined tuple)
        let condition = |l: &Tuple, r: &Tuple| l.get("id") == r.get("id");

        let mut join = JoinExecutor::inner(
            Box::new(left),
            Box::new(right),
            Box::new(condition),
            "c".to_string(),
            "o".to_string(),
        );

        let results: Vec<_> =
            std::iter::from_fn(|| join.next().transpose()).collect::<Result<_, _>>().unwrap();

        assert_eq!(results.len(), 1);
        // Both id and name columns should be preserved with prefixes
        assert_eq!(results[0].get("c.id"), Some(&crate::catalog::Value::Int(1)));
        assert_eq!(
            results[0].get("c.name"),
            Some(&crate::catalog::Value::Text("Alice".to_string()))
        );
        assert_eq!(results[0].get("o.id"), Some(&crate::catalog::Value::Int(1)));
        assert_eq!(
            results[0].get("o.name"),
            Some(&crate::catalog::Value::Text("Order1".to_string()))
        );
    }

    #[test]
    fn test_prefix_tuple() {
        let mut tuple = Tuple::new();
        tuple.insert("id".to_string(), crate::catalog::Value::Int(1));
        tuple.insert("name".to_string(), crate::catalog::Value::Text("Alice".to_string()));

        let prefixed = JoinExecutor::prefix_tuple(&tuple, "c");

        assert_eq!(prefixed.get("c.id"), Some(&crate::catalog::Value::Int(1)));
        assert_eq!(prefixed.get("c.name"), Some(&crate::catalog::Value::Text("Alice".to_string())));
        assert_eq!(prefixed.get("id"), None); // Original should not exist
        assert_eq!(prefixed.get("name"), None);
    }

    #[test]
    fn test_left_join_with_unmatched() {
        let left = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("id", 1).build(),
            TupleBuilder::new().with_int("id", 2).build(),
        ]);

        let right = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("id", 1).build()]);

        let condition = |l: &Tuple, r: &Tuple| l.get("id") == r.get("id");

        let mut join = JoinExecutor::left(
            Box::new(left),
            Box::new(right),
            Box::new(condition),
            "l".to_string(),
            "r".to_string(),
        );

        let results: Vec<_> =
            std::iter::from_fn(|| join.next().transpose()).collect::<Result<_, _>>().unwrap();

        // Should have 2 results: matched id=1 and unmatched id=2
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_right_join_with_unmatched() {
        let left = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("id", 1).build()]);

        let right = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("id", 1).build(),
            TupleBuilder::new().with_int("id", 2).build(),
        ]);

        let condition = |l: &Tuple, r: &Tuple| l.get("id") == r.get("id");

        let mut join = JoinExecutor::right(
            Box::new(left),
            Box::new(right),
            Box::new(condition),
            "l".to_string(),
            "r".to_string(),
        );

        let results: Vec<_> =
            std::iter::from_fn(|| join.next().transpose()).collect::<Result<_, _>>().unwrap();

        // Should have 2 results: matched id=1 and unmatched right id=2
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_inner_join_no_matches() {
        let left = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("id", 1).build()]);

        let right = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("id", 2).build()]);

        let condition = |l: &Tuple, r: &Tuple| l.get("id") == r.get("id");

        let mut join = JoinExecutor::inner(
            Box::new(left),
            Box::new(right),
            Box::new(condition),
            "l".to_string(),
            "r".to_string(),
        );
        assert!(join.next().unwrap().is_none());
    }

    #[test]
    fn test_inner_join_empty_left() {
        let left = MockExecutor::empty();
        let right = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("id", 1).build()]);

        let condition = |_: &Tuple, _: &Tuple| true;

        let mut join = JoinExecutor::inner(
            Box::new(left),
            Box::new(right),
            Box::new(condition),
            "l".to_string(),
            "r".to_string(),
        );
        assert!(join.next().unwrap().is_none());
    }

    #[test]
    fn test_inner_join_empty_right() {
        let left = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("id", 1).build()]);
        let right = MockExecutor::empty();

        let condition = |_: &Tuple, _: &Tuple| true;

        let mut join = JoinExecutor::inner(
            Box::new(left),
            Box::new(right),
            Box::new(condition),
            "l".to_string(),
            "r".to_string(),
        );
        assert!(join.next().unwrap().is_none());
    }

    #[test]
    fn test_cross_join() {
        let left = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("l", 1).build(),
            TupleBuilder::new().with_int("l", 2).build(),
        ]);

        let right = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("r", 10).build(),
            TupleBuilder::new().with_int("r", 20).build(),
        ]);

        // Cross join: condition always true
        let condition = |_: &Tuple, _: &Tuple| true;

        let mut join = JoinExecutor::inner(
            Box::new(left),
            Box::new(right),
            Box::new(condition),
            "l".to_string(),
            "r".to_string(),
        );

        let results: Vec<_> =
            std::iter::from_fn(|| join.next().transpose()).collect::<Result<_, _>>().unwrap();

        // Cross product: 2x2 = 4 results
        assert_eq!(results.len(), 4);
    }
}
