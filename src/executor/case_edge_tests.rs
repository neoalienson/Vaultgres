#[cfg(test)]
mod tests {
    use crate::executor::test_helpers::OldMockExecutor;
    use crate::executor::old_executor::SimpleTuple;
    use crate::executor::case::Case;
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
    fn test_case_zero_rows() {
        let mock = Box::new(OldMockExecutor::new(vec![]));
        let evaluator = Box::new(|_: &SimpleTuple| vec![0]);
        let mut case = Case::new(mock, evaluator);

        case.open().unwrap();
        assert!(case.next().unwrap().is_none());
        case.close().unwrap();
    }

    #[test]
    fn test_case_one_row() {
        let tuples = vec![SimpleTuple { data: vec![1] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let evaluator = Box::new(|_: &SimpleTuple| vec![99]);
        let mut case = Case::new(mock, evaluator);

        case.open().unwrap();
        let r = case.next().unwrap().unwrap();
        assert_eq!(r.data[r.data.len() - 1], 99);
        assert!(case.next().unwrap().is_none());
        case.close().unwrap();
    }

    #[test]
    fn test_case_all_same_result() {
        let tuples = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
        ];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let evaluator = Box::new(|_: &SimpleTuple| vec![42]);
        let mut case = Case::new(mock, evaluator);

        case.open().unwrap();
        for _ in 0..3 {
            let r = case.next().unwrap().unwrap();
            assert_eq!(r.data[r.data.len() - 1], 42);
        }
        case.close().unwrap();
    }

    #[test]
    fn test_case_all_different_results() {
        let tuples = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
        ];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let evaluator = Box::new(|t: &SimpleTuple| vec![t.data[0] * 10]);
        let mut case = Case::new(mock, evaluator);

        case.open().unwrap();
        assert_eq!(case.next().unwrap().unwrap().data.last(), Some(&10));
        assert_eq!(case.next().unwrap().unwrap().data.last(), Some(&20));
        assert_eq!(case.next().unwrap().unwrap().data.last(), Some(&30));
        case.close().unwrap();
    }

    #[test]
    fn test_case_boundary_values() {
        let tuples = vec![SimpleTuple { data: vec![0] }, SimpleTuple { data: vec![255] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let evaluator = Box::new(|t: &SimpleTuple| vec![t.data[0]]);
        let mut case = Case::new(mock, evaluator);

        case.open().unwrap();
        assert_eq!(case.next().unwrap().unwrap().data.last(), Some(&0));
        assert_eq!(case.next().unwrap().unwrap().data.last(), Some(&255));
        case.close().unwrap();
    }

    #[test]
    fn test_case_nested_conditions() {
        let tuples = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![50] },
            SimpleTuple { data: vec![100] },
        ];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let evaluator = Box::new(|t: &SimpleTuple| {
            if t.data[0] < 10 {
                vec![1]
            } else if t.data[0] < 75 {
                vec![2]
            } else {
                vec![3]
            }
        });
        let mut case = Case::new(mock, evaluator);

        case.open().unwrap();
        assert_eq!(case.next().unwrap().unwrap().data.last(), Some(&1));
        assert_eq!(case.next().unwrap().unwrap().data.last(), Some(&2));
        assert_eq!(case.next().unwrap().unwrap().data.last(), Some(&3));
        case.close().unwrap();
    }

    #[test]
    fn test_case_large_result_values() {
        let tuples = vec![SimpleTuple { data: vec![1] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let evaluator = Box::new(|_: &SimpleTuple| vec![255; 100]);
        let mut case = Case::new(mock, evaluator);

        case.open().unwrap();
        let r = case.next().unwrap().unwrap();
        assert_eq!(r.data.len(), 101);
        case.close().unwrap();
    }

    #[test]
    fn test_case_empty_input_data() {
        let tuples = vec![SimpleTuple { data: vec![] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let evaluator = Box::new(|_: &SimpleTuple| vec![1]);
        let mut case = Case::new(mock, evaluator);

        case.open().unwrap();
        let r = case.next().unwrap().unwrap();
        assert_eq!(r.data.len(), 1);
        case.close().unwrap();
    }

    #[test]
    fn test_case_many_rows() {
        let tuples: Vec<SimpleTuple> =
            (0..1000).map(|i| SimpleTuple { data: vec![i as u8] }).collect();
        let mock = Box::new(OldMockExecutor::new(tuples));
        let evaluator = Box::new(|t: &SimpleTuple| if t.data[0] < 128 { vec![0] } else { vec![1] });
        let mut case = Case::new(mock, evaluator);

        case.open().unwrap();
        let mut count = 0;
        while case.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 1000);
        case.close().unwrap();
    }

    #[test]
    fn test_case_reopen_multiple_times() {
        let tuples = vec![SimpleTuple { data: vec![1] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let evaluator = Box::new(|_: &SimpleTuple| vec![5]);
        let mut case = Case::new(mock, evaluator);

        for _ in 0..5 {
            case.open().unwrap();
            let r = case.next().unwrap().unwrap();
            assert_eq!(r.data[r.data.len() - 1], 5);
            case.close().unwrap();
        }
    }

    #[test]
    fn test_case_partial_read() {
        let tuples = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
        ];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let evaluator = Box::new(|_: &SimpleTuple| vec![0]);
        let mut case = Case::new(mock, evaluator);

        case.open().unwrap();
        case.next().unwrap();
        case.close().unwrap();

        case.open().unwrap();
        assert!(case.next().unwrap().is_some());
        case.close().unwrap();
    }

    #[test]
    fn test_case_complex_evaluation() {
        let tuples = vec![SimpleTuple { data: vec![10, 20] }, SimpleTuple { data: vec![30, 40] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let evaluator =
            Box::new(
                |t: &SimpleTuple| {
                    if t.data.len() >= 2 {
                        vec![t.data[0] + t.data[1]]
                    } else {
                        vec![0]
                    }
                },
            );
        let mut case = Case::new(mock, evaluator);

        case.open().unwrap();
        assert_eq!(case.next().unwrap().unwrap().data.last(), Some(&30));
        assert_eq!(case.next().unwrap().unwrap().data.last(), Some(&70));
        case.close().unwrap();
    }

    #[test]
    fn test_case_zero_result() {
        let tuples = vec![SimpleTuple { data: vec![1] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let evaluator = Box::new(|_: &SimpleTuple| vec![0]);
        let mut case = Case::new(mock, evaluator);

        case.open().unwrap();
        let r = case.next().unwrap().unwrap();
        assert_eq!(r.data[r.data.len() - 1], 0);
        case.close().unwrap();
    }
}
