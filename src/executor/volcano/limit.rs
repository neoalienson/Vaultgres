//! LimitExecutor - Limits the number of tuples returned

use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};

pub struct LimitExecutor {
    child: Box<dyn Executor>,
    limit: usize,
    offset: usize,
    current_count: usize,
    skipped_count: usize,
}

impl LimitExecutor {
    pub fn new(child: Box<dyn Executor>, limit: Option<usize>, offset: usize) -> Self {
        Self {
            child,
            limit: limit.unwrap_or(usize::MAX),
            offset,
            current_count: 0,
            skipped_count: 0,
        }
    }
}

impl Executor for LimitExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        // Skip offset tuples first
        while self.skipped_count < self.offset {
            match self.child.next()? {
                Some(_) => {
                    self.skipped_count += 1;
                }
                None => return Ok(None), // No more tuples
            }
        }

        // Then return up to limit tuples
        if self.current_count >= self.limit {
            return Ok(None);
        }

        match self.child.next()? {
            Some(tuple) => {
                self.current_count += 1;
                Ok(Some(tuple))
            }
            None => Ok(None),
        }
    }
}
