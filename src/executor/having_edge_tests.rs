#[cfg(test)]
mod tests {
    use crate::executor::test_helpers::OldMockExecutor;
    use crate::executor::old_executor::SimpleTuple;
    use crate::executor::mock::MockExecutor;
    use crate::executor::{Having, SimpleExecutor, SimpleTuple as Tuple};

    #[test]
    fn test_having_empty_input() {
        let input = OldMockExecutor::new(vec![]);
        let mut having = Having::new(Box::new(input), Box::new(|_| true));
        having.open().unwrap();
        assert!(having.next().unwrap().is_none());
        having.close().unwrap();
    }

    #[test]
    fn test_having_single_row_match() {
        let input = OldMockExecutor::new(vec![Tuple { data: vec![1, 100] }]);
        let mut having =
            Having::new(Box::new(input), Box::new(|t| t.data.get(1).copied().unwrap_or(0) > 50));
        having.open().unwrap();
        assert!(having.next().unwrap().is_some());
        assert!(having.next().unwrap().is_none());
        having.close().unwrap();
    }

    #[test]
    fn test_having_single_row_no_match() {
        let input = OldMockExecutor::new(vec![Tuple { data: vec![1, 10] }]);
        let mut having =
            Having::new(Box::new(input), Box::new(|t| t.data.get(1).copied().unwrap_or(0) > 50));
        having.open().unwrap();
        assert!(having.next().unwrap().is_none());
        having.close().unwrap();
    }

    #[test]
    fn test_having_all_match() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 100] },
            Tuple { data: vec![2, 200] },
            Tuple { data: vec![3, 150] },
        ]);
        let mut having =
            Having::new(Box::new(input), Box::new(|t| t.data.get(1).copied().unwrap_or(0) > 50));
        having.open().unwrap();

        let mut count = 0;
        while having.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
        having.close().unwrap();
    }

    #[test]
    fn test_having_none_match() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 10] },
            Tuple { data: vec![2, 20] },
            Tuple { data: vec![3, 30] },
        ]);
        let mut having =
            Having::new(Box::new(input), Box::new(|t| t.data.get(1).copied().unwrap_or(0) > 100));
        having.open().unwrap();
        assert!(having.next().unwrap().is_none());
        having.close().unwrap();
    }

    #[test]
    fn test_having_zero_values() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 0] },
            Tuple { data: vec![2, 0] },
            Tuple { data: vec![3, 5] },
        ]);
        let mut having =
            Having::new(Box::new(input), Box::new(|t| t.data.get(1).copied().unwrap_or(0) == 0));
        having.open().unwrap();

        let mut count = 0;
        while having.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 2);
        having.close().unwrap();
    }

    #[test]
    fn test_having_boundary_values() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 255] },
            Tuple { data: vec![2, 0] },
            Tuple { data: vec![3, 128] },
        ]);
        let mut having =
            Having::new(Box::new(input), Box::new(|t| t.data.get(1).copied().unwrap_or(0) >= 128));
        having.open().unwrap();

        let mut count = 0;
        while having.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 2);
        having.close().unwrap();
    }

    #[test]
    fn test_having_large_dataset() {
        let mut tuples = Vec::new();
        for i in 0..200 {
            tuples.push(Tuple { data: vec![(i % 10) as u8, (i % 100) as u8] });
        }
        let input = OldMockExecutor::new(tuples);
        let mut having =
            Having::new(Box::new(input), Box::new(|t| t.data.get(1).copied().unwrap_or(0) > 50));
        having.open().unwrap();

        let mut count = 0;
        while having.next().unwrap().is_some() {
            count += 1;
        }
        assert!(count > 0);
        having.close().unwrap();
    }

    #[test]
    fn test_having_first_match() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 100] },
            Tuple { data: vec![2, 10] },
            Tuple { data: vec![3, 20] },
        ]);
        let mut having =
            Having::new(Box::new(input), Box::new(|t| t.data.get(1).copied().unwrap_or(0) > 50));
        having.open().unwrap();

        let result = having.next().unwrap().unwrap();
        assert_eq!(result.data[1], 100);
        assert!(having.next().unwrap().is_none());
        having.close().unwrap();
    }

    #[test]
    fn test_having_last_match() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 10] },
            Tuple { data: vec![2, 20] },
            Tuple { data: vec![3, 100] },
        ]);
        let mut having =
            Having::new(Box::new(input), Box::new(|t| t.data.get(1).copied().unwrap_or(0) > 50));
        having.open().unwrap();

        let result = having.next().unwrap().unwrap();
        assert_eq!(result.data[1], 100);
        assert!(having.next().unwrap().is_none());
        having.close().unwrap();
    }

    #[test]
    fn test_having_middle_match() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 10] },
            Tuple { data: vec![2, 100] },
            Tuple { data: vec![3, 20] },
        ]);
        let mut having =
            Having::new(Box::new(input), Box::new(|t| t.data.get(1).copied().unwrap_or(0) > 50));
        having.open().unwrap();

        let result = having.next().unwrap().unwrap();
        assert_eq!(result.data[1], 100);
        assert!(having.next().unwrap().is_none());
        having.close().unwrap();
    }

    #[test]
    fn test_having_alternating_matches() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 100] },
            Tuple { data: vec![2, 10] },
            Tuple { data: vec![3, 100] },
            Tuple { data: vec![4, 10] },
        ]);
        let mut having =
            Having::new(Box::new(input), Box::new(|t| t.data.get(1).copied().unwrap_or(0) > 50));
        having.open().unwrap();

        let mut count = 0;
        while having.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 2);
        having.close().unwrap();
    }

    #[test]
    fn test_having_multiple_columns() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 10, 20, 30] },
            Tuple { data: vec![2, 50, 60, 70] },
            Tuple { data: vec![3, 5, 10, 15] },
        ]);
        let mut having = Having::new(
            Box::new(input),
            Box::new(|t| {
                t.data.get(1).copied().unwrap_or(0) > 20 && t.data.get(2).copied().unwrap_or(0) > 30
            }),
        );
        having.open().unwrap();

        let mut count = 0;
        while having.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 1);
        having.close().unwrap();
    }

    #[test]
    fn test_having_equality_condition() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 42] },
            Tuple { data: vec![2, 42] },
            Tuple { data: vec![3, 42] },
            Tuple { data: vec![4, 43] },
        ]);
        let mut having =
            Having::new(Box::new(input), Box::new(|t| t.data.get(1).copied().unwrap_or(0) == 42));
        having.open().unwrap();

        let mut count = 0;
        while having.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
        having.close().unwrap();
    }
}
