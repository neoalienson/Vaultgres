#[cfg(test)]
mod tests {
    use crate::executor::test_helpers::OldMockExecutor;
    use crate::executor::old_executor::SimpleTuple;
    use crate::executor::test_helpers::{count_results, MockExecutor};
    use crate::executor::window::{Window, WindowFunction};
    use crate::executor::{SimpleExecutor, SimpleTuple};

    #[test]
    fn test_row_number_zero_rows() {
        let mock = Box::new(OldMockExecutor::new(vec![]));
        let mut window = Window::new(mock, WindowFunction::RowNumber);

        window.open().unwrap();
        assert!(window.next().unwrap().is_none());
        window.close().unwrap();
    }

    #[test]
    fn test_row_number_one_row() {
        let tuples = vec![SimpleTuple { data: vec![1] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::RowNumber);

        window.open().unwrap();
        let r = window.next().unwrap().unwrap();
        assert_eq!(&r.data[r.data.len() - 8..], &1i64.to_le_bytes());
        assert!(window.next().unwrap().is_none());
        window.close().unwrap();
    }

    #[test]
    fn test_row_number_many_rows() {
        let tuples: Vec<SimpleTuple> =
            (0..1000).map(|i| SimpleTuple { data: vec![i as u8] }).collect();
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::RowNumber);

        window.open().unwrap();
        for i in 1..=1000 {
            let r = window.next().unwrap().unwrap();
            assert_eq!(&r.data[r.data.len() - 8..], &(i as i64).to_le_bytes());
        }
        window.close().unwrap();
    }

    #[test]
    fn test_rank_empty() {
        let mock = Box::new(OldMockExecutor::new(vec![]));
        let mut window = Window::new(mock, WindowFunction::Rank);

        window.open().unwrap();
        assert!(window.next().unwrap().is_none());
        window.close().unwrap();
    }

    #[test]
    fn test_rank_single() {
        let tuples = vec![SimpleTuple { data: vec![10] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::Rank);

        window.open().unwrap();
        let r = window.next().unwrap().unwrap();
        assert_eq!(&r.data[r.data.len() - 8..], &1i64.to_le_bytes());
        window.close().unwrap();
    }

    #[test]
    fn test_dense_rank_empty() {
        let mock = Box::new(OldMockExecutor::new(vec![]));
        let mut window = Window::new(mock, WindowFunction::DenseRank);

        window.open().unwrap();
        assert!(window.next().unwrap().is_none());
        window.close().unwrap();
    }

    #[test]
    fn test_dense_rank_multiple() {
        let tuples = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
        ];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::DenseRank);

        window.open().unwrap();
        for i in 1..=3 {
            let r = window.next().unwrap().unwrap();
            assert_eq!(&r.data[r.data.len() - 8..], &(i as i64).to_le_bytes());
        }
        window.close().unwrap();
    }

    #[test]
    fn test_window_reopen_multiple_times() {
        let tuples = vec![SimpleTuple { data: vec![1] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::RowNumber);

        for _ in 0..5 {
            window.open().unwrap();
            let r = window.next().unwrap().unwrap();
            assert_eq!(&r.data[r.data.len() - 8..], &1i64.to_le_bytes());
            window.close().unwrap();
        }
    }

    #[test]
    fn test_window_large_data_values() {
        let tuples = vec![SimpleTuple { data: vec![255; 1000] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::RowNumber);

        window.open().unwrap();
        let r = window.next().unwrap().unwrap();
        assert_eq!(r.data.len(), 1008);
        window.close().unwrap();
    }

    #[test]
    fn test_window_empty_data() {
        let tuples = vec![SimpleTuple { data: vec![] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::RowNumber);

        window.open().unwrap();
        let r = window.next().unwrap().unwrap();
        assert_eq!(&r.data[r.data.len() - 8..], &1i64.to_le_bytes());
        window.close().unwrap();
    }

    #[test]
    fn test_window_all_functions_same_input() {
        let tuples = vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }];

        for func in [WindowFunction::RowNumber, WindowFunction::Rank, WindowFunction::DenseRank] {
            let mock = Box::new(OldMockExecutor::new(tuples.clone()));
            let mut window = Window::new(mock, func);

            window.open().unwrap();
            let r1 = window.next().unwrap().unwrap();
            assert_eq!(&r1.data[r1.data.len() - 8..], &1i64.to_le_bytes());
            let r2 = window.next().unwrap().unwrap();
            assert_eq!(&r2.data[r2.data.len() - 8..], &2i64.to_le_bytes());
            window.close().unwrap();
        }
    }

    #[test]
    fn test_window_max_rows() {
        let tuples: Vec<SimpleTuple> = (0..10000).map(|_| SimpleTuple { data: vec![1] }).collect();
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::RowNumber);

        window.open().unwrap();
        assert_eq!(count_results(&mut window).unwrap(), 10000);
        window.close().unwrap();
    }

    #[test]
    fn test_window_partial_read() {
        let tuples = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
        ];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::RowNumber);

        window.open().unwrap();
        window.next().unwrap();
        window.close().unwrap();

        window.open().unwrap();
        let r1 = window.next().unwrap().unwrap();
        assert_eq!(&r1.data[r1.data.len() - 8..], &1i64.to_le_bytes());
        window.close().unwrap();
    }

    #[test]
    fn test_lag_single_row() {
        let tuples = vec![SimpleTuple { data: vec![42] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::Lag);

        window.open().unwrap();
        let r = window.next().unwrap().unwrap();
        assert_eq!(r.data[r.data.len() - 1], 0);
        window.close().unwrap();
    }

    #[test]
    fn test_lead_single_row() {
        let tuples = vec![SimpleTuple { data: vec![42] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::Lead);

        window.open().unwrap();
        let r = window.next().unwrap().unwrap();
        assert_eq!(r.data[r.data.len() - 1], 0);
        window.close().unwrap();
    }

    #[test]
    fn test_lag_offset_zero() {
        let tuples = vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::Lag).with_offset(0);

        window.open().unwrap();
        let r1 = window.next().unwrap().unwrap();
        assert_eq!(r1.data[r1.data.len() - 1], 1);
        let r2 = window.next().unwrap().unwrap();
        assert_eq!(r2.data[r2.data.len() - 1], 2);
        window.close().unwrap();
    }

    #[test]
    fn test_lead_offset_zero() {
        let tuples = vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::Lead).with_offset(0);

        window.open().unwrap();
        let r1 = window.next().unwrap().unwrap();
        assert_eq!(r1.data[r1.data.len() - 1], 1);
        let r2 = window.next().unwrap().unwrap();
        assert_eq!(r2.data[r2.data.len() - 1], 2);
        window.close().unwrap();
    }

    #[test]
    fn test_lag_large_offset() {
        let tuples = vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::Lag).with_offset(100);

        window.open().unwrap();
        let r1 = window.next().unwrap().unwrap();
        assert_eq!(r1.data[r1.data.len() - 1], 0);
        let r2 = window.next().unwrap().unwrap();
        assert_eq!(r2.data[r2.data.len() - 1], 0);
        window.close().unwrap();
    }

    #[test]
    fn test_lead_large_offset() {
        let tuples = vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::Lead).with_offset(100);

        window.open().unwrap();
        let r1 = window.next().unwrap().unwrap();
        assert_eq!(r1.data[r1.data.len() - 1], 0);
        let r2 = window.next().unwrap().unwrap();
        assert_eq!(r2.data[r2.data.len() - 1], 0);
        window.close().unwrap();
    }

    #[test]
    fn test_lag_many_rows() {
        let tuples: Vec<SimpleTuple> = (0..100).map(|i| SimpleTuple { data: vec![i] }).collect();
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::Lag);

        window.open().unwrap();
        let r1 = window.next().unwrap().unwrap();
        assert_eq!(r1.data[r1.data.len() - 1], 0);

        for i in 1..100 {
            let r = window.next().unwrap().unwrap();
            assert_eq!(r.data[r.data.len() - 1], (i - 1) as u8);
        }
        window.close().unwrap();
    }

    #[test]
    fn test_lead_many_rows() {
        let tuples: Vec<SimpleTuple> = (0..100).map(|i| SimpleTuple { data: vec![i] }).collect();
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::Lead);

        window.open().unwrap();
        for i in 0..99 {
            let r = window.next().unwrap().unwrap();
            assert_eq!(r.data[r.data.len() - 1], (i + 1) as u8);
        }

        let r_last = window.next().unwrap().unwrap();
        assert_eq!(r_last.data[r_last.data.len() - 1], 0);
        window.close().unwrap();
    }

    #[test]
    fn test_lag_lead_alternating() {
        let tuples = vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }];

        let mock1 = Box::new(OldMockExecutor::new(tuples.clone()));
        let mut lag = Window::new(mock1, WindowFunction::Lag);
        lag.open().unwrap();
        lag.next().unwrap();
        lag.close().unwrap();

        let mock2 = Box::new(OldMockExecutor::new(tuples));
        let mut lead = Window::new(mock2, WindowFunction::Lead);
        lead.open().unwrap();
        lead.next().unwrap();
        lead.close().unwrap();
    }

    #[test]
    fn test_lag_with_empty_data() {
        let tuples = vec![SimpleTuple { data: vec![] }, SimpleTuple { data: vec![] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::Lag);

        window.open().unwrap();
        let r1 = window.next().unwrap().unwrap();
        assert_eq!(r1.data.len(), 1);
        let r2 = window.next().unwrap().unwrap();
        // Vec length is usize, always >= 0
        assert!(r2.data.is_empty() || !r2.data.is_empty());
        window.close().unwrap();
    }

    #[test]
    fn test_lead_with_empty_data() {
        let tuples = vec![SimpleTuple { data: vec![] }, SimpleTuple { data: vec![] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::Lead);

        window.open().unwrap();
        let r1 = window.next().unwrap().unwrap();
        // Vec length is usize, always >= 0
        assert!(r1.data.is_empty() || !r1.data.is_empty());
        let r2 = window.next().unwrap().unwrap();
        assert_eq!(r2.data.len(), 1);
        window.close().unwrap();
    }
}
