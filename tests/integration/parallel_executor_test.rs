use std::sync::Arc;
use vaultgres::catalog::{Catalog, Value};
use vaultgres::executor::operators::executor::Tuple;
use vaultgres::executor::parallel::coordinator::ParallelCoordinator;
use vaultgres::executor::parallel::hash_agg::ParallelHashAgg;
use vaultgres::executor::parallel::hash_join::{JoinType, ParallelHashJoin};
use vaultgres::executor::parallel::morsel::MorselGenerator;
use vaultgres::executor::parallel::operator::ParallelOperator;
use vaultgres::executor::parallel::seq_scan::ParallelSeqScan;
use vaultgres::executor::parallel::sort::ParallelSort;
use vaultgres::parser::ast::{ColumnDef, DataType, Expr};

#[test]
fn test_parallel_seq_scan_integration() {
    let catalog = Arc::new(Catalog::new());
    catalog
        .create_table("test".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
        .unwrap();

    for i in 0..1000 {
        catalog.insert("test", &[], vec![Expr::Number(i)]).unwrap();
    }

    let coordinator = ParallelCoordinator::new(4);
    let scan = Arc::new(ParallelSeqScan::new("test".to_string(), catalog));
    let morsel_gen = Arc::new(MorselGenerator::new(1000, 100));

    let result = coordinator.execute_parallel(scan, morsel_gen).unwrap();
    assert_eq!(result.len(), 1000);
}

#[test]
fn test_parallel_hash_join_integration() {
    use vaultgres::catalog::Value;
    use vaultgres::executor::operators::executor::Tuple;
    use vaultgres::executor::parallel::hash_join::JoinType;
    use vaultgres::executor::parallel::operator::ParallelOperator;

    let catalog = Arc::new(Catalog::new());
    catalog
        .create_table(
            "left_table".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("lval".to_string(), DataType::Text),
            ],
        )
        .unwrap();
    catalog
        .create_table(
            "right_table".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("rval".to_string(), DataType::Text),
            ],
        )
        .unwrap();

    for i in 0..100 {
        catalog
            .insert("left_table", &[], vec![Expr::Number(i % 10), Expr::String(format!("L{}", i))])
            .unwrap();
        catalog
            .insert("right_table", &[], vec![Expr::Number(i % 10), Expr::String(format!("R{}", i))])
            .unwrap();
    }

    let left_scan = Arc::new(ParallelSeqScan::new("left_table".to_string(), Arc::clone(&catalog)));
    let right_scan =
        Arc::new(ParallelSeqScan::new("right_table".to_string(), Arc::clone(&catalog)));

    let coordinator = ParallelCoordinator::new(4);
    let morsel_gen_left = Arc::new(MorselGenerator::new(100, 50));
    let morsel_gen_right = Arc::new(MorselGenerator::new(100, 50));

    let left_tuples = coordinator.execute_parallel(left_scan, morsel_gen_left).unwrap();
    let right_tuples = coordinator.execute_parallel(right_scan, morsel_gen_right).unwrap();

    let left_tuples_clone = left_tuples.clone();
    let right_tuples_clone = right_tuples.clone();

    let left_operator = Arc::new(MockParallelOperator { tuples: left_tuples });
    let right_operator = Arc::new(MockParallelOperator { tuples: right_tuples });

    let mut join = ParallelHashJoin::new(
        left_operator,
        right_operator,
        4,
        vec!["id".to_string()],
        vec!["id".to_string()],
        JoinType::Inner,
        "l".to_string(),
        "r".to_string(),
    );
    join.set_build_tuples(left_tuples_clone);
    join.set_probe_tuples(right_tuples_clone);

    let config = vaultgres::executor::parallel::config::ParallelConfig::new(4);
    let results = join.execute(&config, 100, 100).unwrap();

    assert!(!results.is_empty());
    assert_eq!(results.len(), 100);
}

struct MockParallelOperator {
    tuples: Vec<Tuple>,
}

impl ParallelOperator for MockParallelOperator {
    fn process_morsel(
        &self,
        mut morsel: vaultgres::executor::parallel::morsel::Morsel,
    ) -> Result<
        vaultgres::executor::parallel::morsel::Morsel,
        vaultgres::executor::operators::executor::ExecutorError,
    > {
        morsel.tuples = self.tuples.clone();
        Ok(morsel)
    }
}

struct MockParallelAggOperator {
    tuples: Vec<Tuple>,
}

impl ParallelOperator for MockParallelAggOperator {
    fn process_morsel(
        &self,
        mut morsel: vaultgres::executor::parallel::morsel::Morsel,
    ) -> Result<
        vaultgres::executor::parallel::morsel::Morsel,
        vaultgres::executor::operators::executor::ExecutorError,
    > {
        morsel.tuples = self.tuples.clone();
        Ok(morsel)
    }
}

#[test]
fn test_parallel_aggregation_integration() {
    use vaultgres::catalog::TableSchema;
    use vaultgres::parser::ast::AggregateFunc;

    let tuples: Vec<Tuple> = (0..500)
        .map(|i| {
            let mut tuple = std::collections::HashMap::new();
            tuple.insert("category".to_string(), Value::Int(i % 10));
            tuple.insert("value".to_string(), Value::Int(i));
            tuple
        })
        .collect();

    let child = Arc::new(MockParallelAggOperator { tuples });
    let group_by = vec![Expr::Column("category".to_string())];
    let aggregates = vec![Expr::Aggregate {
        func: AggregateFunc::Sum,
        arg: Box::new(Expr::Column("value".to_string())),
    }];
    let output_schema = TableSchema::new("agg".to_string(), vec![]);

    let agg = ParallelHashAgg::new(child, group_by, aggregates, output_schema, 8);

    let morsel = vaultgres::executor::parallel::morsel::Morsel {
        tuples: vec![],
        start_offset: 0,
        end_offset: 500,
        partition_id: 0,
    };

    agg.process_morsel(morsel).unwrap();
    let result = agg.global_combine().unwrap();

    assert_eq!(result.len(), 10);

    for r in &result {
        let cat = r.get("category").unwrap();
        let sum = r.get("sum(value)").unwrap();
        if let (Value::Int(cat_val), Value::Int(sum_val)) = (cat, sum) {
            let expected_sum: i64 = (0..500).filter(|&i| i % 10 == *cat_val).sum();
            assert_eq!(sum, &Value::Int(expected_sum));
        }
    }
}

#[test]
fn test_parallel_sort_integration() {
    use vaultgres::parser::ast::OrderByExpr;

    let catalog = Arc::new(Catalog::new());
    catalog
        .create_table(
            "sort_test".to_string(),
            vec![ColumnDef::new("id".to_string(), DataType::Int)],
        )
        .unwrap();

    for i in (0..200).rev() {
        catalog.insert("sort_test", &[], vec![Expr::Number(i)]).unwrap();
    }

    let scan = Arc::new(ParallelSeqScan::new("sort_test".to_string(), catalog));
    let sort =
        ParallelSort::new(scan, vec![OrderByExpr { column: "id".to_string(), ascending: true }]);

    let morsel_gen = Arc::new(MorselGenerator::new(200, 50));
    let mut sorted_runs = Vec::new();

    while let Some(range) = morsel_gen.next_morsel() {
        let morsel = vaultgres::executor::parallel::morsel::Morsel {
            tuples: vec![],
            start_offset: range.start,
            end_offset: range.end,
            partition_id: 0,
        };
        let sorted = sort.process_morsel(morsel).unwrap();
        sorted_runs.push(sorted.tuples);
    }

    let final_result = ParallelSort::multi_phase_merge(sorted_runs, &sort.sort_keys).unwrap();
    assert_eq!(final_result.len(), 200);

    // Tuples are sorted, verify ordering by checking first value
    for i in 0..final_result.len() - 1 {
        let val_i = final_result[i].values().next();
        let val_next = final_result[i + 1].values().next();
        if let (Some(a), Some(b)) = (val_i, val_next) {
            assert!(a <= b);
        }
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
        .create_table("single".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
        .unwrap();

    for i in 0..50 {
        catalog.insert("single", &[], vec![Expr::Number(i)]).unwrap();
    }

    let coordinator = ParallelCoordinator::new(1);
    let scan = Arc::new(ParallelSeqScan::new("single".to_string(), catalog));
    let morsel_gen = Arc::new(MorselGenerator::new(50, 10));

    let result = coordinator.execute_parallel(scan, morsel_gen).unwrap();
    assert_eq!(result.len(), 50);
}
