//! Test helpers supporting both old and new executor models

use crate::catalog::Value;
use crate::executor::old_executor::{OldExecutor, OldExecutorError, SimpleTuple};
use crate::executor::{Executor, ExecutorError, Tuple};
use std::collections::HashMap;

/// Mock executor for the new Executor trait (Volcano model)
pub struct MockExecutor {
    pub tuples: Vec<Tuple>,
    pub position: usize,
}

impl MockExecutor {
    pub fn new(tuples: Vec<Tuple>) -> Self {
        Self { tuples, position: 0 }
    }

    /// Create a mock executor with simple integer tuples
    pub fn from_int_values(values: Vec<i64>) -> Self {
        let tuples = values
            .into_iter()
            .map(|v| {
                let mut tuple = Tuple::new();
                tuple.insert("value".to_string(), Value::Int(v));
                tuple
            })
            .collect();
        Self::new(tuples)
    }

    /// Create from old-style SimpleTuples for compatibility
    pub fn from_simple_tuples(simple_tuples: Vec<SimpleTuple>) -> Self {
        let tuples = simple_tuples
            .into_iter()
            .map(|st| {
                // Convert old SimpleTuple to new Tuple format
                let mut tuple = HashMap::new();
                tuple.insert("data".to_string(), Value::Text(format!("{:?}", st.data)));
                tuple
            })
            .collect();
        Self::new(tuples)
    }
}

impl Executor for MockExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if self.position < self.tuples.len() {
            let tuple = self.tuples[self.position].clone();
            self.position += 1;
            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }
}

/// Mock executor for the old OldExecutor trait
pub struct OldMockExecutor {
    pub tuples: Vec<SimpleTuple>,
    pub position: usize,
}

impl OldMockExecutor {
    pub fn new(tuples: Vec<SimpleTuple>) -> Self {
        Self { tuples, position: 0 }
    }
}

impl OldExecutor for OldMockExecutor {
    fn open(&mut self) -> Result<(), OldExecutorError> {
        self.position = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<SimpleTuple>, OldExecutorError> {
        if self.position < self.tuples.len() {
            let tuple = self.tuples[self.position].clone();
            self.position += 1;
            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }

    fn close(&mut self) -> Result<(), OldExecutorError> {
        Ok(())
    }
}

/// Helper function for new Executor trait
pub fn count_results<E: Executor>(executor: &mut E) -> Result<usize, ExecutorError> {
    let mut count = 0;
    while executor.next()?.is_some() {
        count += 1;
    }
    Ok(count)
}

/// Helper function for old OldExecutor trait
pub fn count_results_old<E: OldExecutor>(executor: &mut E) -> Result<usize, OldExecutorError> {
    let mut count = 0;
    executor.open()?;
    while executor.next()?.is_some() {
        count += 1;
    }
    executor.close()?;
    Ok(count)
}

/// Helper function for new Executor trait
pub fn run_executor<E: Executor>(executor: &mut E) -> Result<Vec<Tuple>, ExecutorError> {
    let mut results = Vec::new();
    while let Some(tuple) = executor.next()? {
        results.push(tuple);
    }
    Ok(results)
}

/// Helper function for old OldExecutor trait
pub fn run_executor_old<E: OldExecutor>(
    executor: &mut E,
) -> Result<Vec<SimpleTuple>, OldExecutorError> {
    let mut results = Vec::new();
    executor.open()?;
    while let Some(tuple) = executor.next()? {
        results.push(tuple);
    }
    executor.close()?;
    Ok(results)
}

/// Helper function for new Executor trait
pub fn test_executor_lifecycle<E: Executor>(
    executor: &mut E,
    expected_count: usize,
) -> Result<(), ExecutorError> {
    let count = count_results(executor)?;
    assert_eq!(count, expected_count);
    Ok(())
}

/// Helper function for old OldExecutor trait
pub fn test_executor_lifecycle_old<E: OldExecutor>(
    executor: &mut E,
    expected_count: usize,
) -> Result<(), OldExecutorError> {
    executor.open()?;
    let count = count_results_old(executor)?;
    assert_eq!(count, expected_count);
    executor.close()?;
    Ok(())
}
