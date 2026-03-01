use rustgres::catalog::Catalog;
use rustgres::executor::parallel::coordinator::ParallelCoordinator;
use rustgres::executor::parallel::hash_agg::ParallelHashAgg;
use rustgres::executor::parallel::hash_join::ParallelHashJoin;
use rustgres::executor::parallel::morsel::MorselGenerator;
use rustgres::executor::parallel::seq_scan::ParallelSeqScan;
use rustgres::executor::parallel::sort::ParallelSort;
use rustgres::parser::ast::{ColumnDef, DataType, Expr};
use std::sync::Arc;

#[test]
fn test_parallel_seq_scan_integration() {
    let catalog = Arc::new(Catalog::new());
    catalog
        .create_table(
            "test".to_string(),
            vec![ColumnDef { name: "id".to_string(), data_type: DataType::Int }],
        )
        .unwrap();

    for i in 0..1000 {
        catalog.insert("test", vec![Expr::Number(i)]).unwrap();
    }

    let coordinator = ParallelCoordinator::new(4);
    let scan = Arc::new(ParallelSeqScan::new("test".to_string(), catalog));
    let morsel_gen = Arc::new(MorselGenerator::new(1000, 100));

    let result = coordinator.execute_parallel(scan, morsel_gen).unwrap();
    assert_eq!(result.len(), 1000);
}

#[test]
fn test_parallel_hash_join_integration() {
    let catalog = Arc::new(Catalog::new());
    catalog
        .create_table(
            "left_table".to_string(),
            vec![ColumnDef { name: "id".to_string(), data_type: DataType::Int }],
        )
        .unwrap();
    catalog
        .create_table(
            "right_table".to_string(),
            vec![ColumnDef { name: "id".to_string(), data_type: DataType::Int }],
        )
        .unwrap();

    for i in 0..100 {
        catalog.insert("left_table", vec![Expr::Number(i % 10)]).unwrap();
        catalog.insert("right_table", vec![Expr::Number(i % 10)]).unwrap();
    }

    let left_scan = Arc::new(ParallelSeqScan::new("left_table".to_string(), Arc::clone(&catalog)));
    let right_scan = Arc::new(ParallelSeqScan::new("right_table".to_string(), catalog));

    let join = ParallelHashJoin::new(left_scan, right_scan, 4);

    let morsel_gen = Arc::new(MorselGenerator::new(100, 50));

    while let Some(range) = morsel_gen.next_morsel() {
        let morsel = rustgres::executor::parallel::morsel::Morsel {
            tuples: vec![],
            start_offset: range.start,
            end_offset: range.end,
            partition_id: 0,
        };
        join.build_phase(morsel).unwrap();
    }

    morsel_gen.reset();
    let mut total_results = 0;
    while let Some(range) = morsel_gen.next_morsel() {
        let morsel = rustgres::executor::parallel::morsel::Morsel {
            tuples: vec![],
            start_offset: range.start,
            end_offset: range.end,
            partition_id: 0,
        };
        let results = join.probe_phase(morsel).unwrap();
        total_results += results.len();
    }

    assert!(total_results > 0);
}

#[test]
fn test_parallel_aggregation_integration() {
    let catalog = Arc::new(Catalog::new());
    catalog
        .create_table(
            "agg_test".to_string(),
            vec![ColumnDef { name: "id".to_string(), data_type: DataType::Int }],
        )
        .unwrap();

    for i in 0..500 {
        catalog.insert("agg_test", vec![Expr::Number(i % 10)]).unwrap();
    }

    let scan = Arc::new(ParallelSeqScan::new("agg_test".to_string(), catalog));
    let agg = ParallelHashAgg::new(scan, 4);

    let morsel_gen = Arc::new(MorselGenerator::new(500, 100));

    let mut worker_id = 0;
    while let Some(range) = morsel_gen.next_morsel() {
        let morsel = rustgres::executor::parallel::morsel::Morsel {
            tuples: vec![],
            start_offset: range.start,
            end_offset: range.end,
            partition_id: 0,
        };
        agg.local_aggregate(morsel, worker_id % 4).unwrap();
        worker_id += 1;
    }

    let result = agg.global_combine().unwrap();
    assert!(result.len() > 0);
}

#[test]
fn test_parallel_sort_integration() {
    let catalog = Arc::new(Catalog::new());
    catalog
        .create_table(
            "sort_test".to_string(),
            vec![ColumnDef { name: "id".to_string(), data_type: DataType::Int }],
        )
        .unwrap();

    for i in (0..200).rev() {
        catalog.insert("sort_test", vec![Expr::Number(i)]).unwrap();
    }

    let scan = Arc::new(ParallelSeqScan::new("sort_test".to_string(), catalog));
    let sort = ParallelSort::new(scan, true);

    let morsel_gen = Arc::new(MorselGenerator::new(200, 50));
    let mut sorted_runs = Vec::new();

    while let Some(range) = morsel_gen.next_morsel() {
        let morsel = rustgres::executor::parallel::morsel::Morsel {
            tuples: vec![],
            start_offset: range.start,
            end_offset: range.end,
            partition_id: 0,
        };
        let sorted = sort.local_sort(morsel).unwrap();
        sorted_runs.push(sorted);
    }

    let final_result = sort.merge_sorted_runs(sorted_runs);
    assert_eq!(final_result.len(), 200);

    for i in 0..final_result.len() - 1 {
        assert!(final_result[i].data <= final_result[i + 1].data);
    }
}

#[test]
fn test_empty_table_parallel_scan() {
    let catalog = Arc::new(Catalog::new());
    catalog.create_table("empty".to_string(), vec![]).unwrap();

    let coordinator = ParallelCoordinator::new(2);
    let scan = Arc::new(ParallelSeqScan::new("empty".to_string(), catalog));
    let morsel_gen = Arc::new(MorselGenerator::new(0, 100));

    let result = coordinator.execute_parallel(scan, morsel_gen).unwrap();
    assert_eq!(result.len(), 0);
}

#[test]
fn test_single_worker_execution() {
    let catalog = Arc::new(Catalog::new());
    catalog
        .create_table(
            "single".to_string(),
            vec![ColumnDef { name: "id".to_string(), data_type: DataType::Int }],
        )
        .unwrap();

    for i in 0..50 {
        catalog.insert("single", vec![Expr::Number(i)]).unwrap();
    }

    let coordinator = ParallelCoordinator::new(1);
    let scan = Arc::new(ParallelSeqScan::new("single".to_string(), catalog));
    let morsel_gen = Arc::new(MorselGenerator::new(50, 10));

    let result = coordinator.execute_parallel(scan, morsel_gen).unwrap();
    assert_eq!(result.len(), 50);
}
