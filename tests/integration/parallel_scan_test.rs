use std::sync::Arc;
use vaultgres::catalog::Catalog;
use vaultgres::executor::parallel::config::ParallelConfig;
use vaultgres::executor::parallel::seq_scan::ParallelSeqScan;
use vaultgres::parser::ast::{ColumnDef, DataType, Expr};

#[test]
fn test_parallel_scan_small_table() {
    let catalog = Arc::new(Catalog::new());
    catalog
        .create_table("users".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
        .unwrap();

    for i in 0..10 {
        catalog.insert("users", vec![Expr::Number(i)]).unwrap();
    }

    let scan = ParallelSeqScan::new("users".to_string(), catalog);
    let config = ParallelConfig::new(4);
    let result = scan.execute(&config).unwrap();
    assert_eq!(result.len(), 10);
}

#[test]
fn test_parallel_scan_large_table() {
    let catalog = Arc::new(Catalog::new());
    catalog
        .create_table("data".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
        .unwrap();

    for i in 0..1000 {
        catalog.insert("data", vec![Expr::Number(i)]).unwrap();
    }

    let scan = ParallelSeqScan::new("data".to_string(), catalog);
    let config = ParallelConfig::new(8);
    let result = scan.execute(&config).unwrap();
    assert_eq!(result.len(), 1000);
}

#[test]
fn test_parallel_scan_with_different_worker_counts() {
    let catalog = Arc::new(Catalog::new());
    catalog
        .create_table("test".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
        .unwrap();

    for i in 0..100 {
        catalog.insert("test", vec![Expr::Number(i)]).unwrap();
    }

    let scan = ParallelSeqScan::new("test".to_string(), Arc::clone(&catalog));

    for num_workers in [1, 2, 4, 8, 16] {
        let config = ParallelConfig::new(num_workers);
        let result = scan.execute(&config).unwrap();
        assert_eq!(result.len(), 100, "Failed with {} workers", num_workers);
    }
}

#[test]
fn test_parallel_scan_empty_table() {
    let catalog = Arc::new(Catalog::new());
    catalog.create_table("empty".to_string(), vec![]).unwrap();

    let scan = ParallelSeqScan::new("empty".to_string(), catalog);
    let config = ParallelConfig::new(4);
    let result = scan.execute(&config).unwrap();
    assert_eq!(result.len(), 0);
}

#[test]
fn test_parallel_scan_single_row() {
    let catalog = Arc::new(Catalog::new());
    catalog
        .create_table("single".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
        .unwrap();

    catalog.insert("single", vec![Expr::Number(42)]).unwrap();

    let scan = ParallelSeqScan::new("single".to_string(), catalog);
    let config = ParallelConfig::new(4);
    let result = scan.execute(&config).unwrap();
    assert_eq!(result.len(), 1);
}

#[test]
fn test_parallel_scan_more_workers_than_rows() {
    let catalog = Arc::new(Catalog::new());
    catalog
        .create_table("few".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
        .unwrap();

    for i in 0..3 {
        catalog.insert("few", vec![Expr::Number(i)]).unwrap();
    }

    let scan = ParallelSeqScan::new("few".to_string(), catalog);
    let config = ParallelConfig::new(10);
    let result = scan.execute(&config).unwrap();
    assert_eq!(result.len(), 3);
}

#[test]
fn test_parallel_scan_with_config_from_default() {
    let catalog = Arc::new(Catalog::new());
    catalog
        .create_table("test".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
        .unwrap();

    for i in 0..50 {
        catalog.insert("test", vec![Expr::Number(i)]).unwrap();
    }

    let scan = ParallelSeqScan::new("test".to_string(), catalog);
    let config = ParallelConfig::default();
    let result = scan.execute(&config).unwrap();
    assert_eq!(result.len(), 50);
}

#[test]
fn test_parallel_scan_multiple_tables() {
    let catalog = Arc::new(Catalog::new());

    catalog
        .create_table("table1".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
        .unwrap();
    catalog
        .create_table("table2".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
        .unwrap();

    for i in 0..20 {
        catalog.insert("table1", vec![Expr::Number(i)]).unwrap();
    }
    for i in 0..30 {
        catalog.insert("table2", vec![Expr::Number(i)]).unwrap();
    }

    let config = ParallelConfig::new(4);

    let scan1 = ParallelSeqScan::new("table1".to_string(), Arc::clone(&catalog));
    let result1 = scan1.execute(&config).unwrap();
    assert_eq!(result1.len(), 20);

    let scan2 = ParallelSeqScan::new("table2".to_string(), catalog);
    let result2 = scan2.execute(&config).unwrap();
    assert_eq!(result2.len(), 30);
}

#[test]
fn test_parallel_scan_consistency() {
    let catalog = Arc::new(Catalog::new());
    catalog
        .create_table("test".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
        .unwrap();

    for i in 0..100 {
        catalog.insert("test", vec![Expr::Number(i)]).unwrap();
    }

    let scan = ParallelSeqScan::new("test".to_string(), catalog);

    // Run multiple times with same config
    let config = ParallelConfig::new(4);
    for _ in 0..5 {
        let result = scan.execute(&config).unwrap();
        assert_eq!(result.len(), 100);
    }
}
