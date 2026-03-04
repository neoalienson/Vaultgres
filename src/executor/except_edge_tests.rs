#[cfg(test)]
mod tests {
    use crate::executor::test_helpers::OldMockExecutor;
    use crate::executor::old_executor::SimpleTuple;
    use crate::executor::mock::MockExecutor;
    use crate::executor::{Except, SimpleExecutor, SimpleTuple as Tuple};

    #[test]
    fn test_except_single_difference() {
        let left = OldMockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![2] },
            Tuple { data: vec![3] },
        ]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![2] }, Tuple { data: vec![3] }]);
        let mut except = Except::new(Box::new(left), Box::new(right));
        except.open().unwrap();

        let result = except.next().unwrap().unwrap();
        assert_eq!(result.data[0], 1);
        assert!(except.next().unwrap().is_none());
        except.close().unwrap();
    }

    #[test]
    fn test_except_duplicates_in_left() {
        let left = OldMockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![1] },
            Tuple { data: vec![2] },
        ]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![2] }]);
        let mut except = Except::new(Box::new(left), Box::new(right));
        except.open().unwrap();

        let mut count = 0;
        while except.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 2);
        except.close().unwrap();
    }

    #[test]
    fn test_except_large_left_small_right() {
        let mut left_tuples = Vec::new();
        for i in 0..100 {
            left_tuples.push(Tuple { data: vec![(i % 10) as u8] });
        }
        let right = OldMockExecutor::new(vec![Tuple { data: vec![5] }]);
        let mut except = Except::new(Box::new(OldMockExecutor::new(left_tuples)), Box::new(right));
        except.open().unwrap();

        let mut count = 0;
        while except.next().unwrap().is_some() {
            count += 1;
        }
        assert!(count > 0);
        except.close().unwrap();
    }

    #[test]
    fn test_except_wide_tuples() {
        let left = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 2, 3, 4, 5] },
            Tuple { data: vec![6, 7, 8, 9, 10] },
        ]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1, 2, 3, 4, 5] }]);
        let mut except = Except::new(Box::new(left), Box::new(right));
        except.open().unwrap();

        let result = except.next().unwrap().unwrap();
        assert_eq!(result.data[0], 6);
        assert!(except.next().unwrap().is_none());
        except.close().unwrap();
    }

    #[test]
    fn test_except_subset() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let right = OldMockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![2] },
            Tuple { data: vec![3] },
        ]);
        let mut except = Except::new(Box::new(left), Box::new(right));
        except.open().unwrap();
        assert!(except.next().unwrap().is_none());
        except.close().unwrap();
    }

    #[test]
    fn test_except_superset() {
        let left = OldMockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![2] },
            Tuple { data: vec![3] },
        ]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let mut except = Except::new(Box::new(left), Box::new(right));
        except.open().unwrap();

        let result = except.next().unwrap().unwrap();
        assert_eq!(result.data[0], 3);
        assert!(except.next().unwrap().is_none());
        except.close().unwrap();
    }
}
