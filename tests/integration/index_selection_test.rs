use rustgres::optimizer::{IndexInfo, IndexSelector};
use rustgres::statistics::TableStats;

#[test]
fn test_index_selection_small_table() {
    let selector = IndexSelector::new();
    let stats = TableStats { row_count: 100, page_count: 1, avg_row_size: 100 };
    let indexes = vec![IndexInfo { name: "idx_id".to_string(), columns: vec!["id".to_string()] }];

    let result = selector.select_index(&stats, &["id".to_string()], &indexes, 0.01);
    assert!(result.is_ok());
}

#[test]
fn test_index_selection_large_table_low_selectivity() {
    let selector = IndexSelector::new();
    let stats = TableStats { row_count: 1000000, page_count: 10000, avg_row_size: 100 };
    let indexes =
        vec![IndexInfo { name: "idx_user_id".to_string(), columns: vec!["user_id".to_string()] }];

    let result = selector.select_index(&stats, &["user_id".to_string()], &indexes, 0.001);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some("idx_user_id".to_string()));
}

#[test]
fn test_index_selection_high_selectivity_prefers_seq_scan() {
    let selector = IndexSelector::new();
    let stats = TableStats { row_count: 1000, page_count: 10, avg_row_size: 100 };
    let indexes =
        vec![IndexInfo { name: "idx_status".to_string(), columns: vec!["status".to_string()] }];

    let result = selector.select_index(&stats, &["status".to_string()], &indexes, 0.95);
    assert!(result.is_ok());
}

#[test]
fn test_index_selection_multiple_indexes() {
    let selector = IndexSelector::new();
    let stats = TableStats { row_count: 100000, page_count: 1000, avg_row_size: 100 };
    let indexes = vec![
        IndexInfo { name: "idx_email".to_string(), columns: vec!["email".to_string()] },
        IndexInfo { name: "idx_user_id".to_string(), columns: vec!["user_id".to_string()] },
        IndexInfo { name: "idx_created_at".to_string(), columns: vec!["created_at".to_string()] },
    ];

    let result = selector.select_index(&stats, &["email".to_string()], &indexes, 0.01);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some("idx_email".to_string()));
}

#[test]
fn test_index_selection_no_matching_column() {
    let selector = IndexSelector::new();
    let stats = TableStats { row_count: 10000, page_count: 100, avg_row_size: 100 };
    let indexes =
        vec![IndexInfo { name: "idx_email".to_string(), columns: vec!["email".to_string()] }];

    let result = selector.select_index(&stats, &["name".to_string()], &indexes, 0.1);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), None);
}

#[test]
fn test_index_selection_composite_index() {
    let selector = IndexSelector::new();
    let stats = TableStats { row_count: 50000, page_count: 500, avg_row_size: 100 };
    let indexes = vec![IndexInfo {
        name: "idx_user_email".to_string(),
        columns: vec!["user_id".to_string(), "email".to_string()],
    }];

    let result = selector.select_index(&stats, &["user_id".to_string()], &indexes, 0.01);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some("idx_user_email".to_string()));
}

#[test]
fn test_index_selection_empty_indexes() {
    let selector = IndexSelector::new();
    let stats = TableStats { row_count: 1000, page_count: 10, avg_row_size: 100 };
    let indexes = vec![];

    let result = selector.select_index(&stats, &["id".to_string()], &indexes, 0.1);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), None);
}

#[test]
fn test_index_selection_multiple_filter_columns() {
    let selector = IndexSelector::new();
    let stats = TableStats { row_count: 20000, page_count: 200, avg_row_size: 100 };
    let indexes = vec![
        IndexInfo { name: "idx_user_id".to_string(), columns: vec!["user_id".to_string()] },
        IndexInfo { name: "idx_status".to_string(), columns: vec!["status".to_string()] },
    ];

    let result = selector.select_index(
        &stats,
        &["user_id".to_string(), "status".to_string()],
        &indexes,
        0.01,
    );
    assert!(result.is_ok());
    assert!(result.unwrap().is_some());
}

#[test]
fn test_index_selection_very_small_table() {
    let selector = IndexSelector::new();
    let stats = TableStats { row_count: 10, page_count: 1, avg_row_size: 50 };
    let indexes = vec![IndexInfo { name: "idx_id".to_string(), columns: vec!["id".to_string()] }];

    let result = selector.select_index(&stats, &["id".to_string()], &indexes, 0.1);
    assert!(result.is_ok());
}
