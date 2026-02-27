use rustgres::optimizer::{CostModel, SelectivityEstimator};
use rustgres::statistics::{TableStats, ColumnStats, Histogram};

#[test]
fn test_cost_model_creation() {
    let model = CostModel::new();
    assert!(true);
}

#[test]
fn test_seq_scan_cost_estimation() {
    let model = CostModel::new();
    let stats = TableStats {
        row_count: 10000,
        page_count: 100,
        avg_row_size: 100,
    };
    
    let cost = model.estimate_seq_scan(&stats, 1.0).unwrap();
    assert!(cost.total > 0.0);
    assert_eq!(cost.rows, 10000.0);
}

#[test]
fn test_index_scan_vs_seq_scan() {
    let model = CostModel::new();
    let stats = TableStats {
        row_count: 10000,
        page_count: 100,
        avg_row_size: 100,
    };
    
    let seq_cost = model.estimate_seq_scan(&stats, 0.01).unwrap();
    let idx_cost = model.estimate_index_scan(&stats, 0.01).unwrap();
    
    assert!(idx_cost.total < seq_cost.total);
}

#[test]
fn test_join_cost_estimation() {
    let model = CostModel::new();
    let stats1 = TableStats { row_count: 1000, page_count: 10, avg_row_size: 100 };
    let stats2 = TableStats { row_count: 100, page_count: 5, avg_row_size: 100 };
    
    let left = model.estimate_seq_scan(&stats1, 1.0).unwrap();
    let right = model.estimate_seq_scan(&stats2, 1.0).unwrap();
    
    let nl_cost = model.estimate_nested_loop_join(&left, &right).unwrap();
    let hash_cost = model.estimate_hash_join(&left, &right).unwrap();
    
    assert!(hash_cost.total < nl_cost.total);
}

#[test]
fn test_selectivity_equality() {
    let estimator = SelectivityEstimator::new();
    let stats = ColumnStats {
        n_distinct: 1000.0,
        null_frac: 0.0,
        most_common_vals: vec![],
        histogram: Histogram::new(10),
    };
    
    let sel = estimator.estimate_equality(&stats);
    assert_eq!(sel, 0.001);
}

#[test]
fn test_selectivity_and_or() {
    let estimator = SelectivityEstimator::new();
    
    let and_sel = estimator.estimate_and(0.5, 0.5);
    assert_eq!(and_sel, 0.25);
    
    let or_sel = estimator.estimate_or(0.5, 0.5);
    assert_eq!(or_sel, 0.75);
}

#[test]
fn test_selectivity_not() {
    let estimator = SelectivityEstimator::new();
    let sel = estimator.estimate_not(0.2);
    assert_eq!(sel, 0.8);
}

#[test]
fn test_high_selectivity_prefers_seq_scan() {
    let model = CostModel::new();
    let stats = TableStats {
        row_count: 10000,
        page_count: 100,
        avg_row_size: 100,
    };
    
    let seq_cost = model.estimate_seq_scan(&stats, 0.9).unwrap();
    let idx_cost = model.estimate_index_scan(&stats, 0.9).unwrap();
    
    assert!(seq_cost.total < idx_cost.total);
}

#[test]
fn test_low_selectivity_prefers_index_scan() {
    let model = CostModel::new();
    let stats = TableStats {
        row_count: 10000,
        page_count: 100,
        avg_row_size: 100,
    };
    
    let seq_cost = model.estimate_seq_scan(&stats, 0.01).unwrap();
    let idx_cost = model.estimate_index_scan(&stats, 0.01).unwrap();
    
    assert!(idx_cost.total < seq_cost.total);
}
