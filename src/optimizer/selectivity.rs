use crate::statistics::ColumnStats;

pub struct SelectivityEstimator;

impl SelectivityEstimator {
    pub fn new() -> Self {
        Self
    }

    pub fn estimate_equality(&self, stats: &ColumnStats) -> f64 {
        if stats.n_distinct > 0.0 { 1.0 / stats.n_distinct } else { 0.1 }
    }

    pub fn estimate_range(&self, _stats: &ColumnStats, _lower: i64, _upper: i64) -> f64 {
        0.33
    }

    pub fn estimate_like(&self, _pattern: &str) -> f64 {
        0.1
    }

    pub fn estimate_and(&self, left: f64, right: f64) -> f64 {
        left * right
    }

    pub fn estimate_or(&self, left: f64, right: f64) -> f64 {
        left + right - (left * right)
    }

    pub fn estimate_not(&self, selectivity: f64) -> f64 {
        1.0 - selectivity
    }
}

impl Default for SelectivityEstimator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::statistics::Histogram;

    #[test]
    fn test_equality_selectivity() {
        let estimator = SelectivityEstimator::new();
        let stats = ColumnStats {
            n_distinct: 100.0,
            null_frac: 0.0,
            most_common_vals: vec![],
            histogram: Histogram::new(10),
        };

        let sel = estimator.estimate_equality(&stats);
        assert_eq!(sel, 0.01);
    }

    #[test]
    fn test_and_selectivity() {
        let estimator = SelectivityEstimator::new();
        let sel = estimator.estimate_and(0.5, 0.5);
        assert_eq!(sel, 0.25);
    }

    #[test]
    fn test_or_selectivity() {
        let estimator = SelectivityEstimator::new();
        let sel = estimator.estimate_or(0.5, 0.5);
        assert_eq!(sel, 0.75);
    }

    #[test]
    fn test_not_selectivity() {
        let estimator = SelectivityEstimator::new();
        let sel = estimator.estimate_not(0.3);
        assert_eq!(sel, 0.7);
    }
}
