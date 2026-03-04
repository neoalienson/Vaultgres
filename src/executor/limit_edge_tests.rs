//! Edge case tests for LIMIT/OFFSET operator - Updated for new Executor trait

#[cfg(test)]
mod tests {
    use crate::executor::{Executor, ExecutorError, Limit};
    use crate::catalog::Value;
    use std::collections::HashMap;

    struct TestExecutor {
        tuples: Vec<Tuple>,
        index: usize,
    }

    impl TestExecutor {
        fn new(tuples: Vec<Tuple>) -> Self {
            Self { tuples, index: 0 }
        }
    }

    impl Executor for TestExecutor {
        fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
            if self.index < self.tuples.len() {
                let tuple = self.tuples[self.index].clone();
                self.index += 1;
                Ok(Some(tuple))
            } else {
                Ok(None)
            }
        }
    }

    fn make_tuple(id: i64) -> Tuple {
        let mut map = HashMap::new();
        map.insert("id".to_string(), Value::Int(id));
        map
    }

    #[test]
    fn test_limit_larger_than_data() {
        let tuples = vec![make_tuple(1), make_tuple(2)];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), Some(100), 0);

        assert!(limit.next().unwrap().is_some());
        assert!(limit.next().unwrap().is_some());
        assert!(limit.next().unwrap().is_none());
    }

    #[test]
    fn test_offset_equals_data_size() {
        let tuples = vec![make_tuple(1), make_tuple(2), make_tuple(3)];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), None, 3);

        assert!(limit.next().unwrap().is_none());
    }

    #[test]
    fn test_limit_one() {
        let tuples = vec![make_tuple(1), make_tuple(2), make_tuple(3)];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), Some(1), 0);

        let t = limit.next().unwrap().unwrap();
        assert_eq!(t.get("id").unwrap(), &Value::Int(1));
        assert!(limit.next().unwrap().is_none());
    }

    #[test]
    fn test_offset_one() {
        let tuples = vec![make_tuple(1), make_tuple(2)];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), None, 1);

        let t = limit.next().unwrap().unwrap();
        assert_eq!(t.get("id").unwrap(), &Value::Int(2));
        assert!(limit.next().unwrap().is_none());
    }

    #[test]
    fn test_limit_offset_both_one() {
        let tuples = vec![make_tuple(1), make_tuple(2), make_tuple(3)];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), Some(1), 1);

        let t = limit.next().unwrap().unwrap();
        assert_eq!(t.get("id").unwrap(), &Value::Int(2));
        assert!(limit.next().unwrap().is_none());
    }

    #[test]
    fn test_empty_input_with_limit() {
        let tuples = vec![];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), Some(10), 0);

        assert!(limit.next().unwrap().is_none());
    }

    #[test]
    fn test_empty_input_with_offset() {
        let tuples = vec![];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), None, 5);

        assert!(limit.next().unwrap().is_none());
    }

    #[test]
    fn test_large_offset() {
        let tuples = vec![make_tuple(1), make_tuple(2)];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), None, 1000);

        assert!(limit.next().unwrap().is_none());
    }

    #[test]
    fn test_limit_offset_sum_equals_data() {
        let tuples = vec![make_tuple(1), make_tuple(2), make_tuple(3), make_tuple(4)];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), Some(2), 2);

        let t1 = limit.next().unwrap().unwrap();
        assert_eq!(t1.get("id").unwrap(), &Value::Int(3));
        let t2 = limit.next().unwrap().unwrap();
        assert_eq!(t2.get("id").unwrap(), &Value::Int(4));
        assert!(limit.next().unwrap().is_none());
    }

    #[test]
    fn test_limit_offset_sum_exceeds_data() {
        let tuples = vec![make_tuple(1), make_tuple(2), make_tuple(3)];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), Some(5), 2);

        let t = limit.next().unwrap().unwrap();
        assert_eq!(t.get("id").unwrap(), &Value::Int(3));
        assert!(limit.next().unwrap().is_none());
    }
}
