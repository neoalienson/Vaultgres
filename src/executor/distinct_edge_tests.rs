#[cfg(test)]
mod tests {
    use crate::executor::test_helpers::OldMockExecutor;
    use crate::executor::old_executor::SimpleTuple;
    use crate::executor::test_helpers::{count_results, MockExecutor};
    use crate::executor::{Distinct, SimpleExecutor, SimpleTuple as Tuple};

    #[test]
    fn test_distinct_empty_input() {
        let input = OldMockExecutor::new(vec![]);
        let mut distinct = Distinct::new(Box::new(input));
        distinct.open().unwrap();
        assert!(distinct.next().unwrap().is_none());
        distinct.close().unwrap();
    }

    #[test]
    fn test_distinct_single_row() {
        let input = OldMockExecutor::new(vec![Tuple { data: vec![42] }]);
        let mut distinct = Distinct::new(Box::new(input));
        distinct.open().unwrap();
        let result = distinct.next().unwrap().unwrap();
        assert_eq!(result.data[0], 42);
        assert!(distinct.next().unwrap().is_none());
        distinct.close().unwrap();
    }

    #[test]
    fn test_distinct_two_identical_rows() {
        let input =
            OldMockExecutor::new(vec![Tuple { data: vec![1, 2, 3] }, Tuple { data: vec![1, 2, 3] }]);
        let mut distinct = Distinct::new(Box::new(input));
        distinct.open().unwrap();
        assert!(distinct.next().unwrap().is_some());
        assert!(distinct.next().unwrap().is_none());
        distinct.close().unwrap();
    }

    #[test]
    fn test_distinct_many_duplicates() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![1] },
            Tuple { data: vec![1] },
            Tuple { data: vec![1] },
            Tuple { data: vec![1] },
        ]);
        let mut distinct = Distinct::new(Box::new(input));
        distinct.open().unwrap();
        assert_eq!(count_results(&mut distinct).unwrap(), 1);
        distinct.close().unwrap();
    }

    #[test]
    fn test_distinct_all_unique() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![2] },
            Tuple { data: vec![3] },
            Tuple { data: vec![4] },
            Tuple { data: vec![5] },
        ]);
        let mut distinct = Distinct::new(Box::new(input));
        distinct.open().unwrap();
        assert_eq!(count_results(&mut distinct).unwrap(), 5);
        distinct.close().unwrap();
    }

    #[test]
    fn test_distinct_zero_values() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![0] },
            Tuple { data: vec![0] },
            Tuple { data: vec![1] },
        ]);
        let mut distinct = Distinct::new(Box::new(input));
        distinct.open().unwrap();
        assert_eq!(count_results(&mut distinct).unwrap(), 2);
        distinct.close().unwrap();
    }

    #[test]
    fn test_distinct_max_values() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![255] },
            Tuple { data: vec![255] },
            Tuple { data: vec![254] },
        ]);
        let mut distinct = Distinct::new(Box::new(input));
        distinct.open().unwrap();
        assert_eq!(count_results(&mut distinct).unwrap(), 2);
        distinct.close().unwrap();
    }

    #[test]
    fn test_distinct_large_dataset() {
        let mut tuples = Vec::new();
        for i in 0..100 {
            tuples.push(Tuple { data: vec![(i % 10) as u8] });
        }
        let input = OldMockExecutor::new(tuples);
        let mut distinct = Distinct::new(Box::new(input));
        distinct.open().unwrap();
        assert_eq!(count_results(&mut distinct).unwrap(), 10);
        distinct.close().unwrap();
    }

    #[test]
    fn test_distinct_alternating_pattern() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![2] },
            Tuple { data: vec![1] },
            Tuple { data: vec![2] },
            Tuple { data: vec![1] },
        ]);
        let mut distinct = Distinct::new(Box::new(input));
        distinct.open().unwrap();
        assert_eq!(count_results(&mut distinct).unwrap(), 2);
        distinct.close().unwrap();
    }

    #[test]
    fn test_distinct_consecutive_duplicates() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![1] },
            Tuple { data: vec![2] },
            Tuple { data: vec![2] },
            Tuple { data: vec![3] },
            Tuple { data: vec![3] },
        ]);
        let mut distinct = Distinct::new(Box::new(input));
        distinct.open().unwrap();
        assert_eq!(count_results(&mut distinct).unwrap(), 3);
        distinct.close().unwrap();
    }

    #[test]
    fn test_distinct_wide_rows() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 2, 3, 4, 5] },
            Tuple { data: vec![1, 2, 3, 4, 5] },
            Tuple { data: vec![1, 2, 3, 4, 6] },
        ]);
        let mut distinct = Distinct::new(Box::new(input));
        distinct.open().unwrap();
        assert_eq!(count_results(&mut distinct).unwrap(), 2);
        distinct.close().unwrap();
    }

    #[test]
    fn test_distinct_empty_rows() {
        let input = OldMockExecutor::new(vec![Tuple { data: vec![] }, Tuple { data: vec![] }]);
        let mut distinct = Distinct::new(Box::new(input));
        distinct.open().unwrap();
        assert_eq!(count_results(&mut distinct).unwrap(), 1);
        distinct.close().unwrap();
    }

    #[test]
    fn test_distinct_order_preservation() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![5] },
            Tuple { data: vec![3] },
            Tuple { data: vec![5] },
            Tuple { data: vec![1] },
            Tuple { data: vec![3] },
        ]);
        let mut distinct = Distinct::new(Box::new(input));
        distinct.open().unwrap();

        let t1 = distinct.next().unwrap().unwrap();
        assert_eq!(t1.data[0], 5);
        let t2 = distinct.next().unwrap().unwrap();
        assert_eq!(t2.data[0], 3);
        let t3 = distinct.next().unwrap().unwrap();
        assert_eq!(t3.data[0], 1);
        assert!(distinct.next().unwrap().is_none());
        distinct.close().unwrap();
    }

    #[test]
    fn test_distinct_reopen() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![1] },
            Tuple { data: vec![2] },
        ]);
        let mut distinct = Distinct::new(Box::new(input));

        distinct.open().unwrap();
        assert_eq!(count_results(&mut distinct).unwrap(), 2);
        distinct.close().unwrap();
    }
}
