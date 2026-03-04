#[cfg(test)]
mod tests {
    use crate::executor::test_helpers::OldMockExecutor;
    use crate::executor::old_executor::SimpleTuple;
    use crate::executor::cte::CTE;
    use crate::executor::{ExecutorError, SimpleExecutor, SimpleTuple};
    use std::collections::HashMap;

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
    fn test_cte_single_row() {
        let tuples = vec![SimpleTuple { data: vec![42] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut cte = CTE::new(HashMap::new(), mock);

        cte.open().unwrap();
        assert_eq!(cte.next().unwrap().unwrap().data, vec![42]);
        assert!(cte.next().unwrap().is_none());
        cte.close().unwrap();
    }

    #[test]
    fn test_cte_empty_results() {
        let mock = Box::new(OldMockExecutor::new(vec![]));
        let mut cte = CTE::new(HashMap::new(), mock);

        cte.open().unwrap();
        assert!(cte.next().unwrap().is_none());
        cte.close().unwrap();
    }

    #[test]
    fn test_cte_many_rows() {
        let tuples: Vec<SimpleTuple> =
            (0..1000).map(|i| SimpleTuple { data: vec![i as u8] }).collect();
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut cte = CTE::new(HashMap::new(), mock);

        cte.open().unwrap();
        let mut count = 0;
        while cte.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 1000);
        cte.close().unwrap();
    }

    #[test]
    fn test_cte_reopen() {
        let tuples = vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut cte = CTE::new(HashMap::new(), mock);

        cte.open().unwrap();
        cte.next().unwrap();
        cte.close().unwrap();

        cte.open().unwrap();
        assert_eq!(cte.next().unwrap().unwrap().data, vec![1]);
        cte.close().unwrap();
    }

    #[test]
    fn test_cte_with_empty_name() {
        let mut cte_results = HashMap::new();
        cte_results.insert("".to_string(), vec![SimpleTuple { data: vec![1] }]);

        let mock = Box::new(OldMockExecutor::new(vec![]));
        let cte = CTE::new(cte_results, mock);

        assert!(cte.get_cte("").is_some());
    }

    #[test]
    fn test_cte_with_long_name() {
        let mut cte_results = HashMap::new();
        let long_name = "a".repeat(1000);
        cte_results.insert(long_name.clone(), vec![SimpleTuple { data: vec![1] }]);

        let mock = Box::new(OldMockExecutor::new(vec![]));
        let cte = CTE::new(cte_results, mock);

        assert!(cte.get_cte(&long_name).is_some());
    }

    #[test]
    fn test_cte_case_sensitive_names() {
        let mut cte_results = HashMap::new();
        cte_results.insert("CTE".to_string(), vec![SimpleTuple { data: vec![1] }]);
        cte_results.insert("cte".to_string(), vec![SimpleTuple { data: vec![2] }]);

        let mock = Box::new(OldMockExecutor::new(vec![]));
        let cte = CTE::new(cte_results, mock);

        assert_eq!(cte.get_cte("CTE").unwrap()[0].data, vec![1]);
        assert_eq!(cte.get_cte("cte").unwrap()[0].data, vec![2]);
    }

    #[test]
    fn test_cte_overwrite_same_name() {
        let mut cte_results = HashMap::new();
        cte_results.insert("temp".to_string(), vec![SimpleTuple { data: vec![1] }]);
        cte_results.insert("temp".to_string(), vec![SimpleTuple { data: vec![2] }]);

        let mock = Box::new(OldMockExecutor::new(vec![]));
        let cte = CTE::new(cte_results, mock);

        assert_eq!(cte.get_cte("temp").unwrap()[0].data, vec![2]);
    }

    #[test]
    fn test_cte_large_data() {
        let tuples = vec![SimpleTuple { data: vec![255; 10000] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut cte = CTE::new(HashMap::new(), mock);

        cte.open().unwrap();
        let result = cte.next().unwrap().unwrap();
        assert_eq!(result.data.len(), 10000);
        cte.close().unwrap();
    }

    #[test]
    fn test_cte_zero_byte_data() {
        let tuples = vec![SimpleTuple { data: vec![0] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut cte = CTE::new(HashMap::new(), mock);

        cte.open().unwrap();
        assert_eq!(cte.next().unwrap().unwrap().data, vec![0]);
        cte.close().unwrap();
    }

    #[test]
    fn test_cte_max_byte_data() {
        let tuples = vec![SimpleTuple { data: vec![255] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut cte = CTE::new(HashMap::new(), mock);

        cte.open().unwrap();
        assert_eq!(cte.next().unwrap().unwrap().data, vec![255]);
        cte.close().unwrap();
    }

    #[test]
    fn test_cte_multiple_cte_results() {
        let mut cte_results = HashMap::new();
        for i in 0..10 {
            cte_results.insert(format!("cte{}", i), vec![SimpleTuple { data: vec![i] }]);
        }

        let mock = Box::new(OldMockExecutor::new(vec![]));
        let cte = CTE::new(cte_results, mock);

        for i in 0..10 {
            assert_eq!(cte.get_cte(&format!("cte{}", i)).unwrap()[0].data, vec![i]);
        }
    }

    #[test]
    fn test_cte_empty_data() {
        let tuples = vec![SimpleTuple { data: vec![] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut cte = CTE::new(HashMap::new(), mock);

        cte.open().unwrap();
        assert_eq!(cte.next().unwrap().unwrap().data, Vec::<u8>::new());
        cte.close().unwrap();
    }

    #[test]
    fn test_cte_multiple_empty_ctes() {
        let mut cte_results = HashMap::new();
        cte_results.insert("cte1".to_string(), vec![]);
        cte_results.insert("cte2".to_string(), vec![]);

        let mock = Box::new(OldMockExecutor::new(vec![]));
        let cte = CTE::new(cte_results, mock);

        assert_eq!(cte.get_cte("cte1").unwrap().len(), 0);
        assert_eq!(cte.get_cte("cte2").unwrap().len(), 0);
    }
}
