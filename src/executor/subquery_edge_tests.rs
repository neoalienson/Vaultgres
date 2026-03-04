#[cfg(test)]
mod tests {
    use crate::executor::test_helpers::OldMockExecutor;
    use crate::executor::old_executor::SimpleTuple;
    use crate::executor::subquery::Subquery;
    use crate::executor::{ExecutorError, SimpleExecutor, SimpleTuple};

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
    fn test_subquery_single_row_single_column() {
        let tuples = vec![SimpleTuple { data: vec![100] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut subquery = Subquery::new(mock);

        assert_eq!(subquery.execute_scalar(), Some(vec![100]));
    }

    #[test]
    fn test_subquery_multiple_rows_first_value() {
        let tuples = vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut subquery = Subquery::new(mock);

        assert_eq!(subquery.execute_scalar(), Some(vec![1]));
    }

    #[test]
    fn test_subquery_empty_result() {
        let mock = Box::new(OldMockExecutor::new(vec![]));
        let mut subquery = Subquery::new(mock);

        assert_eq!(subquery.execute_scalar(), None);
    }

    #[test]
    fn test_subquery_set_single_value() {
        let tuples = vec![SimpleTuple { data: vec![5] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut subquery = Subquery::new(mock);

        let result = subquery.execute_set();
        assert_eq!(result.len(), 1);
        assert!(result.contains(&vec![5]));
    }

    #[test]
    fn test_subquery_set_many_duplicates() {
        let tuples = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![1] },
        ];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut subquery = Subquery::new(mock);

        let result = subquery.execute_set();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_subquery_set_large_result() {
        let tuples: Vec<SimpleTuple> =
            (0..1000).map(|i| SimpleTuple { data: vec![i as u8] }).collect();
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut subquery = Subquery::new(mock);

        let result = subquery.execute_set();
        assert_eq!(result.len(), 256);
    }

    #[test]
    fn test_subquery_text_values() {
        let tuples = vec![SimpleTuple { data: vec![97] }, SimpleTuple { data: vec![98] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut subquery = Subquery::new(mock);

        let result = subquery.execute_set();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_subquery_null_value() {
        let tuples = vec![SimpleTuple { data: vec![0] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut subquery = Subquery::new(mock);

        assert_eq!(subquery.execute_scalar(), Some(vec![0]));
    }

    #[test]
    fn test_subquery_multiple_calls_cached() {
        let tuples = vec![SimpleTuple { data: vec![42] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut subquery = Subquery::new(mock);

        let r1 = subquery.execute_scalar();
        let r2 = subquery.execute_scalar();
        let r3 = subquery.execute_scalar();

        assert_eq!(r1, r2);
        assert_eq!(r2, r3);
    }

    #[test]
    fn test_subquery_set_then_scalar() {
        let tuples = vec![SimpleTuple { data: vec![10] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut subquery = Subquery::new(mock);

        let set = subquery.execute_set();
        let scalar = subquery.execute_scalar();

        assert_eq!(set.len(), 1);
        assert_eq!(scalar, Some(vec![10]));
    }

    #[test]
    fn test_subquery_zero_value() {
        let tuples = vec![SimpleTuple { data: vec![0] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut subquery = Subquery::new(mock);

        assert_eq!(subquery.execute_scalar(), Some(vec![0]));
    }

    #[test]
    fn test_subquery_negative_value() {
        let tuples = vec![SimpleTuple { data: vec![255] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut subquery = Subquery::new(mock);

        assert_eq!(subquery.execute_scalar(), Some(vec![255]));
    }

    #[test]
    fn test_subquery_max_i64() {
        let tuples = vec![SimpleTuple { data: vec![255, 255] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut subquery = Subquery::new(mock);

        assert_eq!(subquery.execute_scalar(), Some(vec![255, 255]));
    }

    #[test]
    fn test_subquery_min_i64() {
        let tuples = vec![SimpleTuple { data: vec![0, 0] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut subquery = Subquery::new(mock);

        assert_eq!(subquery.execute_scalar(), Some(vec![0, 0]));
    }
}
