use crate::executor::old_executor::{
    OldExecutor as SimpleExecutor, OldExecutorError as ExecutorError, SimpleTuple,
};
use std::collections::HashMap;

pub struct CTE {
    cte_results: HashMap<String, Vec<SimpleTuple>>,
    main_query: Box<dyn SimpleExecutor>,
    position: usize,
    results: Vec<SimpleTuple>,
    executed: bool,
}

impl CTE {
    pub fn new(
        cte_results: HashMap<String, Vec<SimpleTuple>>,
        main_query: Box<dyn SimpleExecutor>,
    ) -> Self {
        Self { cte_results, main_query, position: 0, results: Vec::new(), executed: false }
    }

    pub fn get_cte(&self, name: &str) -> Option<&Vec<SimpleTuple>> {
        self.cte_results.get(name)
    }
}

impl SimpleExecutor for CTE {
    fn open(&mut self) -> Result<(), ExecutorError> {
        if !self.executed {
            self.main_query.open()?;
            while let Some(tuple) = self.main_query.next()? {
                self.results.push(tuple);
            }
            self.main_query.close()?;
            self.executed = true;
        }
        self.position = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<SimpleTuple>, ExecutorError> {
        if self.position < self.results.len() {
            let tuple = self.results[self.position].clone();
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
    fn test_cte_basic() {
        let tuples = vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut cte = CTE::new(HashMap::new(), mock);

        cte.open().unwrap();
        assert_eq!(cte.next().unwrap().unwrap().data, vec![1]);
        assert_eq!(cte.next().unwrap().unwrap().data, vec![2]);
        assert!(cte.next().unwrap().is_none());
        cte.close().unwrap();
    }

    #[test]
    fn test_cte_with_results() {
        let mut cte_results = HashMap::new();
        cte_results.insert("temp".to_string(), vec![SimpleTuple { data: vec![10] }]);

        let tuples = vec![SimpleTuple { data: vec![1] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let cte = CTE::new(cte_results, mock);

        assert_eq!(cte.get_cte("temp").unwrap().len(), 1);
        assert_eq!(cte.get_cte("temp").unwrap()[0].data, vec![10]);
    }

    #[test]
    fn test_cte_empty() {
        let mock = Box::new(OldMockExecutor::new(vec![]));
        let mut cte = CTE::new(HashMap::new(), mock);

        cte.open().unwrap();
        assert!(cte.next().unwrap().is_none());
        cte.close().unwrap();
    }

    #[test]
    fn test_cte_multiple_opens() {
        let tuples = vec![SimpleTuple { data: vec![1] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut cte = CTE::new(HashMap::new(), mock);

        cte.open().unwrap();
        assert_eq!(cte.next().unwrap().unwrap().data, vec![1]);
        cte.close().unwrap();

        cte.open().unwrap();
        assert_eq!(cte.next().unwrap().unwrap().data, vec![1]);
        cte.close().unwrap();
    }

    #[test]
    fn test_cte_get_nonexistent() {
        let mock = Box::new(OldMockExecutor::new(vec![]));
        let cte = CTE::new(HashMap::new(), mock);

        assert!(cte.get_cte("nonexistent").is_none());
    }

    #[test]
    fn test_cte_multiple_ctes() {
        let mut cte_results = HashMap::new();
        cte_results.insert("cte1".to_string(), vec![SimpleTuple { data: vec![1] }]);
        cte_results.insert("cte2".to_string(), vec![SimpleTuple { data: vec![2] }]);

        let mock = Box::new(OldMockExecutor::new(vec![]));
        let cte = CTE::new(cte_results, mock);

        assert_eq!(cte.get_cte("cte1").unwrap()[0].data, vec![1]);
        assert_eq!(cte.get_cte("cte2").unwrap()[0].data, vec![2]);
    }

    #[test]
    fn test_cte_large_result() {
        let tuples: Vec<SimpleTuple> = (0..100).map(|i| SimpleTuple { data: vec![i] }).collect();
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut cte = CTE::new(HashMap::new(), mock);

        cte.open().unwrap();
        let mut count = 0;
        while cte.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 100);
        cte.close().unwrap();
    }
}
