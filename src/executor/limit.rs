use super::old_executor::{OldExecutor as Executor, OldExecutorError as ExecutorError, Tuple};

pub struct Limit {
    input: Box<dyn Executor>,
    limit: Option<usize>,
    offset: Option<usize>,
    current: usize,
}

impl Limit {
    pub fn new(input: Box<dyn Executor>, limit: Option<usize>, offset: Option<usize>) -> Self {
        Self { input, limit, offset, current: 0 }
    }
}

impl Executor for Limit {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.input.open()?;
        self.current = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        let offset = self.offset.unwrap_or(0);

        // Skip offset rows
        while self.current < offset {
            if self.input.next()?.is_none() {
                return Ok(None);
            }
            self.current += 1;
        }

        // Check limit
        if let Some(limit) = self.limit {
            if self.current >= offset + limit {
                return Ok(None);
            }
        }

        // Return next tuple
        if let Some(tuple) = self.input.next()? {
            self.current += 1;
            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }

    fn close(&mut self) -> Result<(), ExecutorError> {
        self.input.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct TestExecutor {
        tuples: Vec<HashMap<String, Vec<u8>>>,
        index: usize,
    }

    impl TestExecutor {
        fn new(tuples: Vec<HashMap<String, Vec<u8>>>) -> Self {
            Self { tuples, index: 0 }
        }
    }

    impl Executor for TestExecutor {
        fn open(&mut self) -> Result<(), ExecutorError> {
            self.index = 0;
            Ok(())
        }

        fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
            if self.index < self.tuples.len() {
                let tuple = self.tuples[self.index].clone();
                self.index += 1;
                Ok(Some(tuple))
            } else {
                Ok(None)
            }
        }

        fn close(&mut self) -> Result<(), ExecutorError> {
            Ok(())
        }
    }

    fn make_tuple(id: u8) -> HashMap<String, Vec<u8>> {
        let mut map = HashMap::new();
        map.insert("id".to_string(), vec![id]);
        map
    }

    #[test]
    fn test_limit_only() {
        let tuples =
            vec![make_tuple(1), make_tuple(2), make_tuple(3), make_tuple(4), make_tuple(5)];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), Some(3), None);

        limit.open().unwrap();
        assert!(limit.next().unwrap().is_some());
        assert!(limit.next().unwrap().is_some());
        assert!(limit.next().unwrap().is_some());
        assert!(limit.next().unwrap().is_none());
        limit.close().unwrap();
    }

    #[test]
    fn test_offset_only() {
        let tuples =
            vec![make_tuple(1), make_tuple(2), make_tuple(3), make_tuple(4), make_tuple(5)];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), None, Some(2));

        limit.open().unwrap();
        let t1 = limit.next().unwrap().unwrap();
        assert_eq!(t1.get("id").unwrap(), &vec![3]);
        let t2 = limit.next().unwrap().unwrap();
        assert_eq!(t2.get("id").unwrap(), &vec![4]);
        let t3 = limit.next().unwrap().unwrap();
        assert_eq!(t3.get("id").unwrap(), &vec![5]);
        assert!(limit.next().unwrap().is_none());
        limit.close().unwrap();
    }

    #[test]
    fn test_limit_and_offset() {
        let tuples =
            vec![make_tuple(1), make_tuple(2), make_tuple(3), make_tuple(4), make_tuple(5)];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), Some(2), Some(1));

        limit.open().unwrap();
        let t1 = limit.next().unwrap().unwrap();
        assert_eq!(t1.get("id").unwrap(), &vec![2]);
        let t2 = limit.next().unwrap().unwrap();
        assert_eq!(t2.get("id").unwrap(), &vec![3]);
        assert!(limit.next().unwrap().is_none());
        limit.close().unwrap();
    }

    #[test]
    fn test_offset_beyond_data() {
        let tuples = vec![make_tuple(1), make_tuple(2)];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), None, Some(10));

        limit.open().unwrap();
        assert!(limit.next().unwrap().is_none());
        limit.close().unwrap();
    }

    #[test]
    fn test_zero_limit() {
        let tuples = vec![make_tuple(1), make_tuple(2)];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), Some(0), None);

        limit.open().unwrap();
        assert!(limit.next().unwrap().is_none());
        limit.close().unwrap();
    }
}
