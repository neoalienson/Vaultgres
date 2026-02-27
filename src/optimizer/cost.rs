use super::error::Result;
use crate::statistics::TableStats;

#[derive(Debug, Clone, Copy)]
pub struct Cost {
    pub startup: f64,
    pub total: f64,
    pub rows: f64,
}

impl Cost {
    pub fn new(startup: f64, total: f64, rows: f64) -> Self {
        Self { startup, total, rows }
    }
    
    pub fn zero() -> Self {
        Self { startup: 0.0, total: 0.0, rows: 0.0 }
    }
}

pub struct CostModel {
    seq_page_cost: f64,
    random_page_cost: f64,
    cpu_tuple_cost: f64,
    cpu_operator_cost: f64,
}

impl CostModel {
    pub fn new() -> Self {
        Self {
            seq_page_cost: 1.0,
            random_page_cost: 4.0,
            cpu_tuple_cost: 0.01,
            cpu_operator_cost: 0.0025,
        }
    }
    
    pub fn estimate_seq_scan(&self, stats: &TableStats, selectivity: f64) -> Result<Cost> {
        let page_cost = stats.page_count as f64 * self.seq_page_cost;
        let cpu_cost = stats.row_count as f64 * self.cpu_tuple_cost;
        let total = page_cost + cpu_cost;
        let rows = stats.row_count as f64 * selectivity;
        
        Ok(Cost::new(0.0, total, rows))
    }
    
    pub fn estimate_index_scan(&self, stats: &TableStats, selectivity: f64) -> Result<Cost> {
        let pages_to_fetch = (stats.page_count as f64 * selectivity).max(1.0);
        let page_cost = pages_to_fetch * self.random_page_cost;
        let cpu_cost = stats.row_count as f64 * selectivity * self.cpu_tuple_cost;
        let total = page_cost + cpu_cost;
        let rows = stats.row_count as f64 * selectivity;
        
        Ok(Cost::new(0.0, total, rows))
    }
    
    pub fn estimate_nested_loop_join(&self, left: &Cost, right: &Cost) -> Result<Cost> {
        let startup = left.startup + right.startup;
        let total = left.total + (left.rows * right.total);
        let rows = left.rows * right.rows;
        
        Ok(Cost::new(startup, total, rows))
    }
    
    pub fn estimate_hash_join(&self, left: &Cost, right: &Cost) -> Result<Cost> {
        let build_cost = right.total;
        let probe_cost = left.total + (left.rows * self.cpu_operator_cost);
        let total = build_cost + probe_cost;
        let rows = left.rows * right.rows * 0.1;
        
        Ok(Cost::new(build_cost, total, rows))
    }
}

impl Default for CostModel {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_cost_creation() {
        let cost = Cost::new(1.0, 10.0, 100.0);
        assert_eq!(cost.startup, 1.0);
        assert_eq!(cost.total, 10.0);
        assert_eq!(cost.rows, 100.0);
    }
    
    #[test]
    fn test_seq_scan_cost() {
        let model = CostModel::new();
        let stats = TableStats {
            row_count: 1000,
            page_count: 10,
            avg_row_size: 100,
        };
        
        let cost = model.estimate_seq_scan(&stats, 1.0).unwrap();
        assert!(cost.total > 0.0);
        assert_eq!(cost.rows, 1000.0);
    }
    
    #[test]
    fn test_index_scan_cost() {
        let model = CostModel::new();
        let stats = TableStats {
            row_count: 1000,
            page_count: 10,
            avg_row_size: 100,
        };
        
        let cost = model.estimate_index_scan(&stats, 0.1).unwrap();
        assert!(cost.total > 0.0);
        assert_eq!(cost.rows, 100.0);
    }
    
    #[test]
    fn test_nested_loop_join_cost() {
        let model = CostModel::new();
        let left = Cost::new(0.0, 10.0, 100.0);
        let right = Cost::new(0.0, 5.0, 50.0);
        
        let cost = model.estimate_nested_loop_join(&left, &right).unwrap();
        assert!(cost.total > 0.0);
        assert_eq!(cost.rows, 5000.0);
    }
    
    #[test]
    fn test_hash_join_cost() {
        let model = CostModel::new();
        let left = Cost::new(0.0, 10.0, 100.0);
        let right = Cost::new(0.0, 5.0, 50.0);
        
        let cost = model.estimate_hash_join(&left, &right).unwrap();
        assert!(cost.total > 0.0);
    }
}
