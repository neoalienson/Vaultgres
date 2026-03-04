#[cfg(test)]
mod tests {
    use crate::executor::test_helpers::OldMockExecutor;
    use crate::executor::old_executor::SimpleTuple;
    use crate::executor::merge_join::MergeJoin;
    use crate::executor::test_helpers::{count_results, MockExecutor};
    use crate::executor::{SimpleExecutor, SimpleTuple};

    #[test]
    fn test_merge_join_both_empty() {
        let mut join = MergeJoin::new(
            Box::new(OldMockExecutor::new(vec![])),
            Box::new(OldMockExecutor::new(vec![])),
        );

        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_large_left() {
        let left: Vec<SimpleTuple> = (0..100).map(|i| SimpleTuple { data: vec![i] }).collect();
        let right = vec![SimpleTuple { data: vec![50] }];

        let mut join =
            MergeJoin::new(Box::new(OldMockExecutor::new(left)), Box::new(OldMockExecutor::new(right)));

        join.open().unwrap();
        assert_eq!(join.next().unwrap().unwrap().data, vec![50, 50]);
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_large_right() {
        let left = vec![SimpleTuple { data: vec![50] }];
        let right: Vec<SimpleTuple> = (0..100).map(|i| SimpleTuple { data: vec![i] }).collect();

        let mut join =
            MergeJoin::new(Box::new(OldMockExecutor::new(left)), Box::new(OldMockExecutor::new(right)));

        join.open().unwrap();
        assert_eq!(join.next().unwrap().unwrap().data, vec![50, 50]);
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_many_duplicates_left() {
        let left = vec![SimpleTuple { data: vec![1] }; 10];
        let right = vec![SimpleTuple { data: vec![1] }];

        let mut join =
            MergeJoin::new(Box::new(OldMockExecutor::new(left)), Box::new(OldMockExecutor::new(right)));

        join.open().unwrap();
        assert_eq!(count_results(&mut join).unwrap(), 10);
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_many_duplicates_right() {
        let left = vec![SimpleTuple { data: vec![1] }];
        let right = vec![SimpleTuple { data: vec![1] }; 10];

        let mut join =
            MergeJoin::new(Box::new(OldMockExecutor::new(left)), Box::new(OldMockExecutor::new(right)));

        join.open().unwrap();
        assert_eq!(count_results(&mut join).unwrap(), 10);
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_many_duplicates_both() {
        let left = vec![SimpleTuple { data: vec![1] }; 5];
        let right = vec![SimpleTuple { data: vec![1] }; 5];

        let mut join =
            MergeJoin::new(Box::new(OldMockExecutor::new(left)), Box::new(OldMockExecutor::new(right)));

        join.open().unwrap();
        assert_eq!(count_results(&mut join).unwrap(), 25);
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_all_left_smaller() {
        let left = vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }];
        let right = vec![SimpleTuple { data: vec![3] }, SimpleTuple { data: vec![4] }];

        let mut join =
            MergeJoin::new(Box::new(OldMockExecutor::new(left)), Box::new(OldMockExecutor::new(right)));

        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_all_right_smaller() {
        let left = vec![SimpleTuple { data: vec![3] }, SimpleTuple { data: vec![4] }];
        let right = vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }];

        let mut join =
            MergeJoin::new(Box::new(OldMockExecutor::new(left)), Box::new(OldMockExecutor::new(right)));

        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_interleaved() {
        let left = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![3] },
            SimpleTuple { data: vec![5] },
        ];
        let right = vec![
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
            SimpleTuple { data: vec![4] },
        ];

        let mut join =
            MergeJoin::new(Box::new(OldMockExecutor::new(left)), Box::new(OldMockExecutor::new(right)));

        join.open().unwrap();
        assert_eq!(join.next().unwrap().unwrap().data, vec![3, 3]);
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_multiple_matches() {
        let left = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
        ];
        let right = vec![
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
        ];

        let mut join =
            MergeJoin::new(Box::new(OldMockExecutor::new(left)), Box::new(OldMockExecutor::new(right)));

        join.open().unwrap();
        assert_eq!(count_results(&mut join).unwrap(), 5);
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_single_left_many_right() {
        let left = vec![SimpleTuple { data: vec![5] }];
        let right = vec![
            SimpleTuple { data: vec![5] },
            SimpleTuple { data: vec![5] },
            SimpleTuple { data: vec![5] },
        ];

        let mut join =
            MergeJoin::new(Box::new(OldMockExecutor::new(left)), Box::new(OldMockExecutor::new(right)));

        join.open().unwrap();
        assert_eq!(count_results(&mut join).unwrap(), 3);
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_large_dataset() {
        let left: Vec<SimpleTuple> = (0..100).map(|i| SimpleTuple { data: vec![i] }).collect();
        let right: Vec<SimpleTuple> = (0..100).map(|i| SimpleTuple { data: vec![i] }).collect();

        let mut join =
            MergeJoin::new(Box::new(OldMockExecutor::new(left)), Box::new(OldMockExecutor::new(right)));

        join.open().unwrap();
        assert_eq!(count_results(&mut join).unwrap(), 100);
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_empty_data() {
        let left = vec![SimpleTuple { data: vec![] }];
        let right = vec![SimpleTuple { data: vec![] }];

        let mut join =
            MergeJoin::new(Box::new(OldMockExecutor::new(left)), Box::new(OldMockExecutor::new(right)));

        join.open().unwrap();
        assert!(join.next().unwrap().is_some());
        join.close().unwrap();
    }
}
