use rustgres::statistics::{Analyzer, TableStats, ColumnStats};

#[test]
fn test_analyzer_with_valid_sample_rate() {
    let analyzer = Analyzer::new(0.1).unwrap();
    assert!(true);
}

#[test]
fn test_analyzer_invalid_sample_rate() {
    assert!(Analyzer::new(0.0).is_err());
    assert!(Analyzer::new(1.5).is_err());
}

#[test]
fn test_table_stats_basic() {
    let analyzer = Analyzer::new(0.1).unwrap();
    let rows = vec![
        vec![1, 2, 3, 4],
        vec![5, 6, 7, 8],
        vec![9, 10, 11, 12],
    ];
    
    let stats = analyzer.analyze_table(&rows).unwrap();
    assert_eq!(stats.row_count, 3);
    assert_eq!(stats.avg_row_size, 4);
}

#[test]
fn test_table_stats_empty() {
    let analyzer = Analyzer::new(0.1).unwrap();
    let rows: Vec<Vec<u8>> = vec![];
    assert!(analyzer.analyze_table(&rows).is_err());
}

#[test]
fn test_column_stats_distinct() {
    let analyzer = Analyzer::new(0.1).unwrap();
    let values: Vec<i64> = vec![1, 2, 3, 4, 5, 1, 2, 3];
    
    let stats = analyzer.analyze_column(values).unwrap();
    assert_eq!(stats.n_distinct, 5.0);
}

#[test]
fn test_column_stats_most_common() {
    let analyzer = Analyzer::new(0.1).unwrap();
    let values: Vec<i64> = vec![1, 1, 1, 2, 2, 3];
    
    let stats = analyzer.analyze_column(values).unwrap();
    assert_eq!(stats.most_common_vals[0], 1);
}

#[test]
fn test_column_stats_empty() {
    let analyzer = Analyzer::new(0.1).unwrap();
    let values: Vec<i64> = vec![];
    
    let stats = analyzer.analyze_column(values).unwrap();
    assert_eq!(stats.n_distinct, 0.0);
}

#[test]
fn test_large_dataset() {
    let analyzer = Analyzer::new(0.1).unwrap();
    let values: Vec<i64> = (0..10000).collect();
    
    let stats = analyzer.analyze_column(values).unwrap();
    assert_eq!(stats.n_distinct, 10000.0);
}
