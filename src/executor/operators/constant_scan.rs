//! ConstantScanExecutor - Returns constant/predefined tuples
//! Used for SELECT without FROM clause (e.g., SELECT 1+1, SELECT COALESCE(...))

use super::executor::{Executor, ExecutorError, Tuple};
use std::collections::VecDeque;

pub struct ConstantScanExecutor {
    tuples: VecDeque<Tuple>,
}

impl ConstantScanExecutor {
    pub fn new(tuples: Vec<Tuple>) -> Self {
        Self { tuples: VecDeque::from(tuples) }
    }
}

impl Executor for ConstantScanExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        Ok(self.tuples.pop_front())
    }
}
