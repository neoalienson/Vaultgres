#[cfg(test)]
mod tests {
    use crate::executor::test_helpers::OldMockExecutor;
    use crate::executor::old_executor::SimpleTuple;
    use crate::executor::test_helpers::{count_results, MockExecutor};
    use crate::executor::{SimpleExecutor, SimpleTuple as Tuple, Union};

    #[test]
    fn test_union_single_row_each() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![2] }]);
        let mut union = Union::new(Box::new(left), Box::new(right), false);
        union.open().unwrap();
        assert_eq!(count_results(&mut union).unwrap(), 2);
        union.close().unwrap();
    }

    #[test]
    fn test_union_all_same_values() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![1] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![1] }]);
        let mut union = Union::new(Box::new(left), Box::new(right), false);
        union.open().unwrap();
        assert_eq!(count_results(&mut union).unwrap(), 1);
        union.close().unwrap();
    }

    #[test]
    fn test_union_all_with_all_same() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![1] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![1] }]);
        let mut union = Union::new(Box::new(left), Box::new(right), true);
        union.open().unwrap();
        assert_eq!(count_results(&mut union).unwrap(), 4);
        union.close().unwrap();
    }

    #[test]
    fn test_union_large_left_small_right() {
        let mut left_tuples = Vec::new();
        for i in 0..100 {
            left_tuples.push(Tuple { data: vec![(i % 10) as u8] });
        }
        let right = OldMockExecutor::new(vec![Tuple { data: vec![5] }]);
        let mut union =
            Union::new(Box::new(OldMockExecutor::new(left_tuples)), Box::new(right), false);
        union.open().unwrap();
        assert_eq!(count_results(&mut union).unwrap(), 10);
        union.close().unwrap();
    }

    #[test]
    fn test_union_all_large_dataset() {
        let mut left_tuples = Vec::new();
        for i in 0..50 {
            left_tuples.push(Tuple { data: vec![(i % 10) as u8] });
        }
        let mut right_tuples = Vec::new();
        for i in 0..50 {
            right_tuples.push(Tuple { data: vec![(i % 10) as u8] });
        }
        let mut union = Union::new(
            Box::new(OldMockExecutor::new(left_tuples)),
            Box::new(OldMockExecutor::new(right_tuples)),
            true,
        );
        union.open().unwrap();
        assert_eq!(count_results(&mut union).unwrap(), 100);
        union.close().unwrap();
    }

    #[test]
    fn test_union_wide_tuples() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1, 2, 3, 4, 5] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1, 2, 3, 4, 5] }]);
        let mut union = Union::new(Box::new(left), Box::new(right), false);
        union.open().unwrap();
        assert_eq!(count_results(&mut union).unwrap(), 1);
        union.close().unwrap();
    }

    #[test]
    fn test_union_all_wide_tuples() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1, 2, 3, 4, 5] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1, 2, 3, 4, 5] }]);
        let mut union = Union::new(Box::new(left), Box::new(right), true);
        union.open().unwrap();
        assert_eq!(count_results(&mut union).unwrap(), 2);
        union.close().unwrap();
    }

    #[test]
    fn test_union_partial_overlap() {
        let left = OldMockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![2] },
            Tuple { data: vec![3] },
        ]);
        let right = OldMockExecutor::new(vec![
            Tuple { data: vec![2] },
            Tuple { data: vec![3] },
            Tuple { data: vec![4] },
        ]);
        let mut union = Union::new(Box::new(left), Box::new(right), false);
        union.open().unwrap();
        assert_eq!(count_results(&mut union).unwrap(), 4);
        union.close().unwrap();
    }

    #[test]
    fn test_union_all_partial_overlap() {
        let left = OldMockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![2] },
            Tuple { data: vec![3] },
        ]);
        let right = OldMockExecutor::new(vec![
            Tuple { data: vec![2] },
            Tuple { data: vec![3] },
            Tuple { data: vec![4] },
        ]);
        let mut union = Union::new(Box::new(left), Box::new(right), true);
        union.open().unwrap();
        assert_eq!(count_results(&mut union).unwrap(), 6);
        union.close().unwrap();
    }

    #[test]
    fn test_union_no_overlap() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![3] }, Tuple { data: vec![4] }]);
        let mut union = Union::new(Box::new(left), Box::new(right), false);
        union.open().unwrap();
        assert_eq!(count_results(&mut union).unwrap(), 4);
        union.close().unwrap();
    }

    #[test]
    fn test_union_preserves_order() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![3] }, Tuple { data: vec![4] }]);
        let mut union = Union::new(Box::new(left), Box::new(right), true);
        union.open().unwrap();

        let r1 = union.next().unwrap().unwrap();
        assert_eq!(r1.data[0], 1);
        let r2 = union.next().unwrap().unwrap();
        assert_eq!(r2.data[0], 2);
        let r3 = union.next().unwrap().unwrap();
        assert_eq!(r3.data[0], 3);
        let r4 = union.next().unwrap().unwrap();
        assert_eq!(r4.data[0], 4);
        assert!(union.next().unwrap().is_none());
        union.close().unwrap();
    }
}
