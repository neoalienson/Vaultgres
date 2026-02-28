//! Edge case tests for statistics collection

#[cfg(test)]
mod tests {
    use crate::statistics::histogram::Histogram;

    #[test]
    fn test_histogram_empty_values() {
        let mut hist = Histogram::new(10);
        hist.build(vec![]).unwrap();
        assert_eq!(hist.estimate_selectivity(50), 0.0);
    }

    #[test]
    fn test_histogram_single_value() {
        let mut hist = Histogram::new(10);
        hist.build(vec![42]).unwrap();
        assert!(hist.estimate_selectivity(42) > 0.0);
    }

    #[test]
    fn test_histogram_duplicate_values() {
        let mut hist = Histogram::new(10);
        hist.build(vec![1, 1, 1, 1, 1]).unwrap();
        assert!(hist.estimate_selectivity(1) > 0.0);
    }

    #[test]
    fn test_histogram_value_not_in_range() {
        let mut hist = Histogram::new(10);
        hist.build((0..100).collect()).unwrap();
        assert_eq!(hist.estimate_selectivity(200), 0.0);
    }

    #[test]
    fn test_histogram_negative_values() {
        let mut hist = Histogram::new(10);
        hist.build(vec![-100, -50, 0, 50, 100]).unwrap();
        assert!(hist.estimate_selectivity(-50) > 0.0);
    }

    #[test]
    fn test_histogram_one_bucket() {
        let mut hist = Histogram::new(1);
        hist.build((0..100).collect()).unwrap();
        assert!(hist.estimate_selectivity(50) > 0.0);
    }

    #[test]
    fn test_histogram_more_buckets_than_values() {
        let mut hist = Histogram::new(100);
        hist.build(vec![1, 2, 3]).unwrap();
        assert!(hist.estimate_selectivity(2) > 0.0);
    }

    #[test]
    fn test_histogram_large_values() {
        let mut hist = Histogram::new(10);
        hist.build(vec![i64::MAX - 1, i64::MAX]).unwrap();
        assert!(hist.estimate_selectivity(i64::MAX) > 0.0);
    }

    #[test]
    fn test_histogram_min_max_values() {
        let mut hist = Histogram::new(10);
        hist.build(vec![i64::MIN, 0, i64::MAX]).unwrap();
        assert!(hist.estimate_selectivity(i64::MIN) > 0.0);
        assert!(hist.estimate_selectivity(i64::MAX) > 0.0);
    }

    #[test]
    fn test_histogram_unsorted_input() {
        let mut hist = Histogram::new(10);
        hist.build(vec![50, 10, 90, 30, 70]).unwrap();
        assert!(hist.estimate_selectivity(50) > 0.0);
    }
}
