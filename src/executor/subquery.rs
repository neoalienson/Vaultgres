use crate::executor::old_executor::{
    OldExecutor as SimpleExecutor, OldExecutorError as ExecutorError, SimpleTuple,
};
use std::collections::HashSet;

pub struct Subquery {
    input: Box<dyn SimpleExecutor>,
    results: Vec<SimpleTuple>,
    position: usize,
    executed: bool,
}

impl Subquery {
    pub fn new(input: Box<dyn SimpleExecutor>) -> Self {
        Self { input, results: Vec::new(), position: 0, executed: false }
    }

    pub fn execute_scalar(&mut self) -> Option<Vec<u8>> {
        if !self.executed {
            let _ = self.open();
            while let Ok(Some(tuple)) = self.next() {
                self.results.push(tuple);
            }
            let _ = self.close();
            self.executed = true;
        }

        self.results.first().map(|t| t.data.clone())
    }

    pub fn execute_set(&mut self) -> HashSet<Vec<u8>> {
        if !self.executed {
            let _ = self.open();
            while let Ok(Some(tuple)) = self.next() {
                self.results.push(tuple);
            }
            let _ = self.close();
            self.executed = true;
        }

        self.results.iter().map(|t| t.data.clone()).collect()
    }
}

impl SimpleExecutor for Subquery {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.input.open()?;
        self.position = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<crate::executor::SimpleTuple>, ExecutorError> {
        self.input.next()
    }

    fn close(&mut self) -> Result<(), ExecutorError> {
        self.input.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::old_executor::SimpleTuple;
    use crate::executor::test_helpers::OldMockExecutor;

    struct MockExecutor {
        tuples: Vec<SimpleTuple>,
        position: usize,
    }

    impl MockExecutor {
        fn new(tuples: Vec<SimpleTuple>) -> Self {
            Self { tuples, position: 0 }
        }
    }

    impl SimpleExecutor for MockExecutor {
        fn open(&mut self) -> Result<(), ExecutorError> {
            self.position = 0;
            Ok(())
        }

        fn next(&mut self) -> Result<Option<SimpleTuple>, ExecutorError> {
            if self.position < self.tuples.len() {
                let tuple = self.tuples[self.position].clone();
                self.position += 1;
                Ok(Some(tuple))
            } else {
                Ok(None)
            }
        }

        fn close(&mut self) -> Result<(), ExecutorError> {
            Ok(())
        }
    }

    #[test]
    fn test_subquery_scalar() {
        let tuples = vec![SimpleTuple { data: vec![42] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut subquery = Subquery::new(mock);

        let result = subquery.execute_scalar();
        assert_eq!(result, Some(vec![42]));
    }

    #[test]
    fn test_subquery_scalar_empty() {
        let mock = Box::new(OldMockExecutor::new(vec![]));
        let mut subquery = Subquery::new(mock);

        let result = subquery.execute_scalar();
        assert_eq!(result, None);
    }

    #[test]
    fn test_subquery_set() {
        let tuples = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
        ];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut subquery = Subquery::new(mock);

        let result = subquery.execute_set();
        assert_eq!(result.len(), 3);
        assert!(result.contains(&vec![1]));
        assert!(result.contains(&vec![2]));
        assert!(result.contains(&vec![3]));
    }

    #[test]
    fn test_subquery_set_duplicates() {
        let tuples = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![2] },
        ];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut subquery = Subquery::new(mock);

        let result = subquery.execute_set();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_subquery_set_empty() {
        let mock = Box::new(OldMockExecutor::new(vec![]));
        let mut subquery = Subquery::new(mock);

        let result = subquery.execute_set();
        assert!(result.is_empty());
    }

    #[test]
    fn test_subquery_caching() {
        let tuples = vec![SimpleTuple { data: vec![10] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut subquery = Subquery::new(mock);

        let result1 = subquery.execute_scalar();
        let result2 = subquery.execute_scalar();
        assert_eq!(result1, result2);
    }

    #[test]
    fn test_subquery_iterator() {
        let tuples = vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut subquery = Subquery::new(mock);

        subquery.open().unwrap();
        assert_eq!(subquery.next().unwrap().unwrap().data, vec![1]);
        assert_eq!(subquery.next().unwrap().unwrap().data, vec![2]);
        assert!(subquery.next().unwrap().is_none());
        subquery.close().unwrap();
    }
}
