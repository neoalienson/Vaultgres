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

    fn create_tuples(n: usize) -> Vec<Tuple> {
        (0..n).map(|i| [("a".to_string(), Value::Int(i as i64))].into()).collect()
    }

    #[test]
    fn test_limit_only() {
        let tuples = create_tuples(10);
        let child = Box::new(MockExecutor::new(tuples));
        let mut executor = LimitExecutor::new(child, Some(5), 0);

        let mut count = 0;
        while executor.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 5);
    }

    #[test]
    fn test_offset_only() {
        let tuples = create_tuples(10);
        let child = Box::new(MockExecutor::new(tuples));
        let mut executor = LimitExecutor::new(child, None, 5);

        let mut results = vec![];
        while let Some(tuple) = executor.next().unwrap() {
            results.push(tuple);
        }
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].get("a"), Some(&Value::Int(5)));
    }

    #[test]
    fn test_limit_and_offset() {
        let tuples = create_tuples(10);
        let child = Box::new(MockExecutor::new(tuples));
        let mut executor = LimitExecutor::new(child, Some(3), 2);

        let mut results = vec![];
        while let Some(tuple) = executor.next().unwrap() {
            results.push(tuple);
        }
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].get("a"), Some(&Value::Int(2)));
    }

    #[test]
    fn test_limit_greater_than_tuples() {
        let tuples = create_tuples(5);
        let child = Box::new(MockExecutor::new(tuples));
        let mut executor = LimitExecutor::new(child, Some(10), 0);

        let mut count = 0;
        while executor.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 5);
    }

    #[test]
    fn test_offset_greater_than_tuples() {
        let tuples = create_tuples(5);
        let child = Box::new(MockExecutor::new(tuples));
        let mut executor = LimitExecutor::new(child, None, 10);
        assert!(executor.next().unwrap().is_none());
    }

    #[test]
    fn test_empty_input() {
        let tuples = create_tuples(0);
        let child = Box::new(MockExecutor::new(tuples));
        let mut executor = LimitExecutor::new(child, Some(5), 0);
        assert!(executor.next().unwrap().is_none());
    }

    #[test]
    fn test_zero_limit() {
        let tuples = create_tuples(10);
        let child = Box::new(MockExecutor::new(tuples));
        let mut executor = LimitExecutor::new(child, Some(0), 0);
        assert!(executor.next().unwrap().is_none());
    }
}
