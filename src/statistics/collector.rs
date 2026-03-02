use super::error::{Result, StatisticsError};
use super::histogram::Histogram;
use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct TableStats {
    pub row_count: u64,
    pub page_count: u64,
    pub avg_row_size: u32,
}

#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub n_distinct: f64,
    pub null_frac: f64,
    pub most_common_vals: Vec<i64>,
    pub histogram: Histogram,
}

pub struct Analyzer {
    sample_rate: f64,
}

impl Analyzer {
    pub fn new(sample_rate: f64) -> Result<Self> {
        if sample_rate <= 0.0 || sample_rate > 1.0 {
            return Err(StatisticsError::InvalidSampleSize);
        }
        Ok(Self { sample_rate })
    }

    pub fn analyze_table(&self, rows: &[Vec<u8>]) -> Result<TableStats> {
        if rows.is_empty() {
            return Err(StatisticsError::EmptyTable);
        }

        let row_count = rows.len() as u64;
        let total_size: usize = rows.iter().map(|r| r.len()).sum();
        let avg_row_size = (total_size / rows.len()) as u32;
        let page_count = total_size.div_ceil(8192);

        Ok(TableStats { row_count, page_count: page_count as u64, avg_row_size })
    }

    pub fn analyze_column(&self, values: Vec<i64>) -> Result<ColumnStats> {
        if values.is_empty() {
            return Ok(ColumnStats {
                n_distinct: 0.0,
                null_frac: 0.0,
                most_common_vals: Vec::new(),
                histogram: Histogram::new(10),
            });
        }

        let mut freq_map: HashMap<i64, u64> = HashMap::new();
        for &val in &values {
            *freq_map.entry(val).or_insert(0) += 1;
        }

        let n_distinct = freq_map.len() as f64;
        let mut most_common: Vec<(i64, u64)> = freq_map.into_iter().collect();
        most_common.sort_by(|a, b| b.1.cmp(&a.1));
        let most_common_vals: Vec<i64> = most_common.iter().take(10).map(|(v, _)| *v).collect();

        let mut histogram = Histogram::new(10);
        histogram.build(values)?;

        Ok(ColumnStats { n_distinct, null_frac: 0.0, most_common_vals, histogram })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_analyzer_creation() {
        let analyzer = Analyzer::new(0.1).unwrap();
        assert_eq!(analyzer.sample_rate, 0.1);
    }

    #[test]
    fn test_analyze_table() {
        let analyzer = Analyzer::new(0.1).unwrap();
        let rows = vec![vec![1, 2, 3], vec![4, 5, 6]];
        let stats = analyzer.analyze_table(&rows).unwrap();
        assert_eq!(stats.row_count, 2);
    }

    #[test]
    fn test_analyze_column() {
        let analyzer = Analyzer::new(0.1).unwrap();
        let values: Vec<i64> = (0..100).collect();
        let stats = analyzer.analyze_column(values).unwrap();
        assert_eq!(stats.n_distinct, 100.0);
    }
}
