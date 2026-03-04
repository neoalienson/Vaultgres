#[cfg(test)]
mod tests {
    use crate::executor::test_helpers::OldMockExecutor;
    use crate::executor::old_executor::SimpleTuple;
    use crate::executor::test_helpers::{count_results, MockExecutor};
    use crate::executor::{Join, JoinType, SimpleExecutor, SimpleTuple as Tuple};

    #[test]
    fn test_join_empty_both() {
        let left = OldMockExecutor::new(vec![]);
        let right = OldMockExecutor::new(vec![]);
        let mut join =
            Join::new(Box::new(left), Box::new(right), JoinType::Inner, Box::new(|_, _| true));
        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_join_single_row_match() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1] }]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Inner,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();
        let result = join.next().unwrap().unwrap();
        assert_eq!(result.data, vec![1, 1]);
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_join_single_row_no_match() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![2] }]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Inner,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_join_cartesian_product() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![3] }, Tuple { data: vec![4] }]);
        let mut join =
            Join::new(Box::new(left), Box::new(right), JoinType::Inner, Box::new(|_, _| true));
        join.open().unwrap();
        assert_eq!(count_results(&mut join).unwrap(), 4);
        join.close().unwrap();
    }

    #[test]
    fn test_join_one_to_many() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }]);
        let right = OldMockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![1] },
            Tuple { data: vec![1] },
        ]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Inner,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();
        assert_eq!(count_results(&mut join).unwrap(), 3);
        join.close().unwrap();
    }

    #[test]
    fn test_join_many_to_one() {
        let left = OldMockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![1] },
            Tuple { data: vec![1] },
        ]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1] }]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Inner,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();
        assert_eq!(count_results(&mut join).unwrap(), 3);
        join.close().unwrap();
    }

    #[test]
    fn test_left_join_all_match() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Left,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();
        assert_eq!(count_results(&mut join).unwrap(), 2);
        join.close().unwrap();
    }

    #[test]
    fn test_left_join_no_match() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![3] }]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Left,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();
        assert_eq!(count_results(&mut join).unwrap(), 2);
        join.close().unwrap();
    }

    #[test]
    fn test_left_join_empty_right() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let right = OldMockExecutor::new(vec![]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Left,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();
        assert_eq!(count_results(&mut join).unwrap(), 2);
        join.close().unwrap();
    }

    #[test]
    fn test_join_wide_tuples() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1, 2, 3, 4] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1, 5, 6, 7] }]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Inner,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();
        let result = join.next().unwrap().unwrap();
        assert_eq!(result.data.len(), 8);
        join.close().unwrap();
    }

    #[test]
    fn test_join_complex_condition() {
        let left =
            OldMockExecutor::new(vec![Tuple { data: vec![1, 10] }, Tuple { data: vec![2, 20] }]);
        let right =
            OldMockExecutor::new(vec![Tuple { data: vec![1, 15] }, Tuple { data: vec![2, 25] }]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Inner,
            Box::new(|l, r| l.data[0] == r.data[0] && l.data[1] < r.data[1]),
        );
        join.open().unwrap();
        assert_eq!(count_results(&mut join).unwrap(), 2);
        join.close().unwrap();
    }

    #[test]
    fn test_join_large_dataset() {
        let mut left_tuples = Vec::new();
        for i in 0..50 {
            left_tuples.push(Tuple { data: vec![(i % 10) as u8] });
        }
        let mut right_tuples = Vec::new();
        for i in 0..50 {
            right_tuples.push(Tuple { data: vec![(i % 10) as u8] });
        }
        let left = OldMockExecutor::new(left_tuples);
        let right = OldMockExecutor::new(right_tuples);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Inner,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();
        assert!(count_results(&mut join).unwrap() > 0);
        join.close().unwrap();
    }

    #[test]
    fn test_right_join_all_match() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Right,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();
        assert_eq!(count_results(&mut join).unwrap(), 2);
        join.close().unwrap();
    }

    #[test]
    fn test_right_join_no_match() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![2] }, Tuple { data: vec![3] }]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Right,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();
        assert_eq!(count_results(&mut join).unwrap(), 2);
        join.close().unwrap();
    }

    #[test]
    fn test_right_join_empty_left() {
        let left = OldMockExecutor::new(vec![]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Right,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();
        assert_eq!(count_results(&mut join).unwrap(), 2);
        join.close().unwrap();
    }

    #[test]
    fn test_full_join_all_match() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Full,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();
        assert_eq!(count_results(&mut join).unwrap(), 2);
        join.close().unwrap();
    }

    #[test]
    fn test_full_join_no_match() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![3] }, Tuple { data: vec![4] }]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Full,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();
        assert_eq!(count_results(&mut join).unwrap(), 4);
        join.close().unwrap();
    }

    #[test]
    fn test_full_join_partial_match() {
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
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Full,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();
        assert_eq!(count_results(&mut join).unwrap(), 4);
        join.close().unwrap();
    }

    #[test]
    fn test_full_join_empty_both() {
        let left = OldMockExecutor::new(vec![]);
        let right = OldMockExecutor::new(vec![]);
        let mut join =
            Join::new(Box::new(left), Box::new(right), JoinType::Full, Box::new(|_, _| true));
        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }
}
