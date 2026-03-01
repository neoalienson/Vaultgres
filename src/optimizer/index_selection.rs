use super::{cost::CostModel, error::Result};
use crate::statistics::TableStats;

#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
}

pub struct IndexSelector {
    cost_model: CostModel,
}

impl IndexSelector {
    pub fn new() -> Self {
        Self { cost_model: CostModel::new() }
    }

    pub fn select_index(
        &self,
        stats: &TableStats,
        filter_columns: &[String],
        available_indexes: &[IndexInfo],
        selectivity: f64,
    ) -> Result<Option<String>> {
        let seq_cost = self.cost_model.estimate_seq_scan(stats, selectivity)?;
        let mut best_cost = seq_cost.total;
        let mut best_index = None;

        for index in available_indexes {
            if self.index_matches_filter(index, filter_columns) {
                let index_cost = self.cost_model.estimate_index_scan(stats, selectivity)?;
                if index_cost.total < best_cost {
                    best_cost = index_cost.total;
                    best_index = Some(index.name.clone());
                }
            }
        }

        Ok(best_index)
    }

    fn index_matches_filter(&self, index: &IndexInfo, filter_columns: &[String]) -> bool {
        filter_columns.iter().any(|col| index.columns.contains(col))
    }
}

impl Default for IndexSelector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_selector_creation() {
        let selector = IndexSelector::new();
        assert!(selector.cost_model.estimate_seq_scan(&TableStats::default(), 1.0).is_ok());
    }

    #[test]
    fn test_select_index_with_matching_index() {
        let selector = IndexSelector::new();
        let stats = TableStats { row_count: 10000, page_count: 100, avg_row_size: 100 };
        let indexes = vec![IndexInfo {
            name: "idx_user_id".to_string(),
            columns: vec!["user_id".to_string()],
        }];

        let result = selector.select_index(&stats, &["user_id".to_string()], &indexes, 0.01);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("idx_user_id".to_string()));
    }

    #[test]
    fn test_select_index_no_matching_index() {
        let selector = IndexSelector::new();
        let stats = TableStats { row_count: 1000, page_count: 10, avg_row_size: 100 };
        let indexes =
            vec![IndexInfo { name: "idx_email".to_string(), columns: vec!["email".to_string()] }];

        let result = selector.select_index(&stats, &["user_id".to_string()], &indexes, 0.5);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_select_index_seq_scan_cheaper() {
        let selector = IndexSelector::new();
        let stats = TableStats { row_count: 100, page_count: 1, avg_row_size: 100 };
        let indexes = vec![IndexInfo {
            name: "idx_user_id".to_string(),
            columns: vec!["user_id".to_string()],
        }];

        let result = selector.select_index(&stats, &["user_id".to_string()], &indexes, 0.9);
        assert!(result.is_ok());
    }

    #[test]
    fn test_select_best_index_from_multiple() {
        let selector = IndexSelector::new();
        let stats = TableStats { row_count: 10000, page_count: 100, avg_row_size: 100 };
        let indexes = vec![
            IndexInfo { name: "idx_user_id".to_string(), columns: vec!["user_id".to_string()] },
            IndexInfo { name: "idx_email".to_string(), columns: vec!["email".to_string()] },
        ];

        let result = selector.select_index(&stats, &["user_id".to_string()], &indexes, 0.01);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_index_matches_filter() {
        let selector = IndexSelector::new();
        let index = IndexInfo {
            name: "idx_user".to_string(),
            columns: vec!["user_id".to_string(), "email".to_string()],
        };

        assert!(selector.index_matches_filter(&index, &["user_id".to_string()]));
        assert!(selector.index_matches_filter(&index, &["email".to_string()]));
        assert!(!selector.index_matches_filter(&index, &["name".to_string()]));
    }
}
