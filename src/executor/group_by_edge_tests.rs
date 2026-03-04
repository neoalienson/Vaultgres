#[cfg(test)]
mod tests {
    use crate::executor::test_helpers::OldMockExecutor;
    use crate::executor::old_executor::SimpleTuple;
    use crate::executor::mock::MockExecutor;
    use crate::executor::{GroupBy, SimpleExecutor, SimpleTuple as Tuple};

    #[test]
    fn test_group_by_empty_input() {
        let input = OldMockExecutor::new(vec![]);
        let mut group_by = GroupBy::new(Box::new(input), vec![0], vec![1]);
        group_by.open().unwrap();
        assert!(group_by.next().unwrap().is_none());
        group_by.close().unwrap();
    }

    #[test]
    fn test_group_by_single_row() {
        let input = OldMockExecutor::new(vec![Tuple { data: vec![1, 100] }]);
        let mut group_by = GroupBy::new(Box::new(input), vec![0], vec![1]);
        group_by.open().unwrap();

        let result = group_by.next().unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().data, vec![1, 100]);
        assert!(group_by.next().unwrap().is_none());
        group_by.close().unwrap();
    }

    #[test]
    fn test_group_by_all_same_group() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 10] },
            Tuple { data: vec![1, 20] },
            Tuple { data: vec![1, 30] },
            Tuple { data: vec![1, 40] },
        ]);
        let mut group_by = GroupBy::new(Box::new(input), vec![0], vec![1]);
        group_by.open().unwrap();

        let result = group_by.next().unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().data[1], 100);
        assert!(group_by.next().unwrap().is_none());
        group_by.close().unwrap();
    }

    #[test]
    fn test_group_by_all_different_groups() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 10] },
            Tuple { data: vec![2, 20] },
            Tuple { data: vec![3, 30] },
            Tuple { data: vec![4, 40] },
            Tuple { data: vec![5, 50] },
        ]);
        let mut group_by = GroupBy::new(Box::new(input), vec![0], vec![1]);
        group_by.open().unwrap();

        let mut count = 0;
        while group_by.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 5);
        group_by.close().unwrap();
    }

    #[test]
    fn test_group_by_zero_values() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 0] },
            Tuple { data: vec![1, 0] },
            Tuple { data: vec![2, 0] },
        ]);
        let mut group_by = GroupBy::new(Box::new(input), vec![0], vec![1]);
        group_by.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = group_by.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].data[1], 0);
        group_by.close().unwrap();
    }

    #[test]
    fn test_group_by_large_values() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 200] },
            Tuple { data: vec![1, 50] },
            Tuple { data: vec![2, 255] },
        ]);
        let mut group_by = GroupBy::new(Box::new(input), vec![0], vec![1]);
        group_by.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = group_by.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
        group_by.close().unwrap();
    }

    #[test]
    fn test_group_by_many_groups() {
        let mut tuples = Vec::new();
        for i in 0..100 {
            tuples.push(Tuple { data: vec![i, 1] });
        }
        let input = OldMockExecutor::new(tuples);
        let mut group_by = GroupBy::new(Box::new(input), vec![0], vec![1]);
        group_by.open().unwrap();

        let mut count = 0;
        while group_by.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 100);
        group_by.close().unwrap();
    }

    #[test]
    fn test_group_by_three_columns() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 1, 1, 10] },
            Tuple { data: vec![1, 1, 1, 20] },
            Tuple { data: vec![1, 1, 2, 30] },
        ]);
        let mut group_by = GroupBy::new(Box::new(input), vec![0, 1, 2], vec![3]);
        group_by.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = group_by.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
        group_by.close().unwrap();
    }

    #[test]
    fn test_group_by_no_aggregate_columns() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![1] },
            Tuple { data: vec![2] },
        ]);
        let mut group_by = GroupBy::new(Box::new(input), vec![0], vec![]);
        group_by.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = group_by.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
        group_by.close().unwrap();
    }

    #[test]
    fn test_group_by_reopen_multiple_times() {
        let input =
            OldMockExecutor::new(vec![Tuple { data: vec![1, 10] }, Tuple { data: vec![2, 20] }]);
        let mut group_by = GroupBy::new(Box::new(input), vec![0], vec![1]);
        group_by.open().unwrap();

        let mut count = 0;
        while group_by.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 2);

        group_by.close().unwrap();
    }

    #[test]
    fn test_group_by_interleaved_groups() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 10] },
            Tuple { data: vec![2, 20] },
            Tuple { data: vec![1, 30] },
            Tuple { data: vec![2, 40] },
            Tuple { data: vec![1, 50] },
        ]);
        let mut group_by = GroupBy::new(Box::new(input), vec![0], vec![1]);
        group_by.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = group_by.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
        group_by.close().unwrap();
    }

    #[test]
    fn test_group_by_multiple_aggregates_per_group() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 10, 5, 2] },
            Tuple { data: vec![1, 20, 3, 4] },
            Tuple { data: vec![2, 30, 7, 1] },
        ]);
        let mut group_by = GroupBy::new(Box::new(input), vec![0], vec![1, 2, 3]);
        group_by.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = group_by.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].data.len(), 4);
        group_by.close().unwrap();
    }

    #[test]
    fn test_group_by_overflow_protection() {
        let input =
            OldMockExecutor::new(vec![Tuple { data: vec![1, 255] }, Tuple { data: vec![1, 255] }]);
        let mut group_by = GroupBy::new(Box::new(input), vec![0], vec![1]);
        group_by.open().unwrap();

        let result = group_by.next().unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().data[1], 255);
        group_by.close().unwrap();
    }
}
