use vaultgres::executor::parallel::config::ParallelConfig;
use vaultgres::executor::parallel::hash_agg::ParallelHashAgg;
use vaultgres::executor::parallel::hash_join::ParallelHashJoin;
use vaultgres::executor::parallel::morsel::Morsel;
use vaultgres::executor::parallel::operator::ParallelOperator;
use vaultgres::executor::parallel::sort::ParallelSort;
use vaultgres::executor::{ExecutorError, SimpleTuple};
use std::sync::Arc;

struct MockOperator {
    tuples: Vec<SimpleTuple>,
}

impl ParallelOperator for MockOperator {
    fn process_morsel(&self, mut morsel: Morsel) -> Result<Morsel, ExecutorError> {
        let start = morsel.start_offset;
        let end = morsel.end_offset.min(self.tuples.len());
        morsel.tuples = self.tuples[start..end].to_vec();
        Ok(morsel)
    }
}

// Hash Join Tests
#[test]
fn test_parallel_hash_join_basic() {
    let build_tuples = vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }];
    let probe_tuples = vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }];

    let build_op: Arc<dyn ParallelOperator> = Arc::new(MockOperator { tuples: build_tuples });
    let probe_op: Arc<dyn ParallelOperator> = Arc::new(MockOperator { tuples: probe_tuples });

    let join = ParallelHashJoin::new(build_op, probe_op, 4);
    let config = ParallelConfig::new(2);
    let result = join.execute(&config, 2, 2).unwrap();
    assert_eq!(result.len(), 2);
}

#[test]
fn test_parallel_hash_join_no_matches() {
    let build_tuples = vec![SimpleTuple { data: vec![1] }];
    let probe_tuples = vec![SimpleTuple { data: vec![2] }];

    let build_op: Arc<dyn ParallelOperator> = Arc::new(MockOperator { tuples: build_tuples });
    let probe_op: Arc<dyn ParallelOperator> = Arc::new(MockOperator { tuples: probe_tuples });

    let join = ParallelHashJoin::new(build_op, probe_op, 4);
    let config = ParallelConfig::new(2);
    let result = join.execute(&config, 1, 1).unwrap();
    assert_eq!(result.len(), 0);
}

#[test]
fn test_parallel_hash_join_multiple_workers() {
    let build_tuples: Vec<_> = (0..20).map(|i| SimpleTuple { data: vec![i] }).collect();
    let probe_tuples: Vec<_> = (0..20).map(|i| SimpleTuple { data: vec![i] }).collect();

    let build_op: Arc<dyn ParallelOperator> = Arc::new(MockOperator { tuples: build_tuples });
    let probe_op: Arc<dyn ParallelOperator> = Arc::new(MockOperator { tuples: probe_tuples });

    let join = ParallelHashJoin::new(build_op, probe_op, 4);
    let config = ParallelConfig::new(4);
    let result = join.execute(&config, 20, 20).unwrap();
    assert_eq!(result.len(), 20);
}

// Hash Aggregation Tests
#[test]
fn test_parallel_hash_agg_basic() {
    let tuples = vec![
        SimpleTuple { data: vec![1] },
        SimpleTuple { data: vec![1] },
        SimpleTuple { data: vec![2] },
    ];

    let child: Arc<dyn ParallelOperator> = Arc::new(MockOperator { tuples });
    let agg = ParallelHashAgg::new(child, 2);
    let config = ParallelConfig::new(2);
    let result = agg.execute(&config, 3).unwrap();
    assert_eq!(result.len(), 2);
}

#[test]
fn test_parallel_hash_agg_single_group() {
    let tuples = vec![
        SimpleTuple { data: vec![1] },
        SimpleTuple { data: vec![1] },
        SimpleTuple { data: vec![1] },
    ];

    let child: Arc<dyn ParallelOperator> = Arc::new(MockOperator { tuples });
    let agg = ParallelHashAgg::new(child, 2);
    let config = ParallelConfig::new(2);
    let result = agg.execute(&config, 3).unwrap();
    assert_eq!(result.len(), 1);
}

#[test]
fn test_parallel_hash_agg_many_groups() {
    let tuples: Vec<_> = (0..100).map(|i| SimpleTuple { data: vec![i % 10] }).collect();

    let child: Arc<dyn ParallelOperator> = Arc::new(MockOperator { tuples });
    let agg = ParallelHashAgg::new(child, 4);
    let config = ParallelConfig::new(4);
    let result = agg.execute(&config, 100).unwrap();
    assert_eq!(result.len(), 10);
}

#[test]
fn test_parallel_hash_agg_empty() {
    let tuples = vec![];

    let child: Arc<dyn ParallelOperator> = Arc::new(MockOperator { tuples });
    let agg = ParallelHashAgg::new(child, 2);
    let config = ParallelConfig::new(2);
    let result = agg.execute(&config, 0).unwrap();
    assert_eq!(result.len(), 0);
}

// Parallel Sort Tests
#[test]
fn test_parallel_sort_ascending() {
    let tuples = vec![
        SimpleTuple { data: vec![3] },
        SimpleTuple { data: vec![1] },
        SimpleTuple { data: vec![2] },
    ];

    let child: Arc<dyn ParallelOperator> = Arc::new(MockOperator { tuples });
    let sort = ParallelSort::new(child, true);
    let config = ParallelConfig::new(2);
    let result = sort.execute(&config, 3).unwrap();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].data, vec![1]);
    assert_eq!(result[1].data, vec![2]);
    assert_eq!(result[2].data, vec![3]);
}

#[test]
fn test_parallel_sort_descending() {
    let tuples = vec![
        SimpleTuple { data: vec![1] },
        SimpleTuple { data: vec![3] },
        SimpleTuple { data: vec![2] },
    ];

    let child: Arc<dyn ParallelOperator> = Arc::new(MockOperator { tuples });
    let sort = ParallelSort::new(child, false);
    let config = ParallelConfig::new(2);
    let result = sort.execute(&config, 3).unwrap();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].data, vec![3]);
    assert_eq!(result[1].data, vec![2]);
    assert_eq!(result[2].data, vec![1]);
}

#[test]
fn test_parallel_sort_large_dataset() {
    let mut tuples: Vec<_> = (0..100).map(|i| SimpleTuple { data: vec![99 - i] }).collect();
    tuples.reverse();

    let child: Arc<dyn ParallelOperator> = Arc::new(MockOperator { tuples });
    let sort = ParallelSort::new(child, true);
    let config = ParallelConfig::new(4);
    let result = sort.execute(&config, 100).unwrap();

    assert_eq!(result.len(), 100);
    for i in 0..100 {
        assert_eq!(result[i].data, vec![i as u8]);
    }
}

#[test]
fn test_parallel_sort_empty() {
    let tuples = vec![];

    let child: Arc<dyn ParallelOperator> = Arc::new(MockOperator { tuples });
    let sort = ParallelSort::new(child, true);
    let config = ParallelConfig::new(2);
    let result = sort.execute(&config, 0).unwrap();
    assert_eq!(result.len(), 0);
}

#[test]
fn test_parallel_sort_single_element() {
    let tuples = vec![SimpleTuple { data: vec![42] }];

    let child: Arc<dyn ParallelOperator> = Arc::new(MockOperator { tuples });
    let sort = ParallelSort::new(child, true);
    let config = ParallelConfig::new(2);
    let result = sort.execute(&config, 1).unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].data, vec![42]);
}

#[test]
fn test_parallel_sort_with_different_worker_counts() {
    let tuples: Vec<_> = (0..50).map(|i| SimpleTuple { data: vec![49 - i] }).collect();

    for num_workers in [1, 2, 4, 8] {
        let child: Arc<dyn ParallelOperator> = Arc::new(MockOperator { tuples: tuples.clone() });
        let sort = ParallelSort::new(child, true);
        let config = ParallelConfig::new(num_workers);
        let result = sort.execute(&config, 50).unwrap();

        assert_eq!(result.len(), 50, "Failed with {} workers", num_workers);
        for i in 0..50 {
            assert_eq!(
                result[i].data,
                vec![i as u8],
                "Failed at index {} with {} workers",
                i,
                num_workers
            );
        }
    }
}
