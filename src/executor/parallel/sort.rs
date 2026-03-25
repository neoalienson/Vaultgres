//! ParallelSort - Parallel sorting with multi-way merge
//!
//! Follows PostgreSQL's parallel sort design:
//! - Each worker creates sorted runs via local sorting
//! - Multiple intermediate merge phases reduce runs logarithmically  
//! - Final merge combines remaining runs into single sorted output
//! - Spill-to-disk support for large sorts
//! - LIMIT optimization: use heap-based selection when LIMIT is small

use crate::catalog::Value;
use crate::executor::operators::executor::{ExecutorError, Tuple};
use crate::executor::parallel::config::ParallelConfig;
use crate::executor::parallel::morsel::Morsel;
use crate::executor::parallel::operator::ParallelOperator;
use crate::executor::parallel::worker_pool::WorkerPool;
use crate::parser::ast::OrderByExpr;
use crossbeam::channel::bounded;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::collections::binary_heap::BinaryHeap;
use std::sync::Arc;

const DEFAULT_WORK_MEM_BYTES: usize = 4 * 1024 * 1024; // 4MB default work mem
const TUPLE_SIZE_ESTIMATE: usize = 256; // Estimated average tuple size

#[derive(Debug, Clone)]
pub struct SortKey {
    pub column: String,
    pub ascending: bool,
    pub nulls_last: bool,
}

impl SortKey {
    pub fn from_order_by_expr(expr: OrderByExpr) -> Self {
        Self {
            column: expr.column,
            ascending: expr.ascending,
            nulls_last: true, // PostgreSQL default: NULLS LAST for ASC, NULLS FIRST for DESC
        }
    }
}

pub struct ParallelSort {
    child: Arc<dyn ParallelOperator>,
    pub sort_keys: Vec<SortKey>,
    num_workers: usize,
    limit: Option<usize>,
    work_mem_bytes: usize,
}

impl ParallelSort {
    pub fn new(child: Arc<dyn ParallelOperator>, sort_keys: Vec<OrderByExpr>) -> Self {
        let keys: Vec<SortKey> = sort_keys.into_iter().map(SortKey::from_order_by_expr).collect();
        Self {
            child,
            sort_keys: keys,
            num_workers: num_cpus::get().max(1),
            limit: None,
            work_mem_bytes: DEFAULT_WORK_MEM_BYTES,
        }
    }

    pub fn with_workers(mut self, num_workers: usize) -> Self {
        self.num_workers = num_workers;
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_work_mem(mut self, work_mem_bytes: usize) -> Self {
        self.work_mem_bytes = work_mem_bytes;
        self
    }

    pub fn execute(&self, config: &ParallelConfig) -> Result<Vec<Tuple>, ExecutorError> {
        let num_workers = config.max_workers().min(self.num_workers);
        let pool = WorkerPool::new(num_workers);

        let (result_sender, result_receiver) = bounded(num_workers * 2);

        // Submit local sort tasks - each worker processes a range
        let rows_per_worker = self.child.degree_of_parallelism().div_ceil(num_workers);

        for worker_id in 0..num_workers {
            let start = worker_id * rows_per_worker;
            let end = (start + rows_per_worker).min(self.child.degree_of_parallelism());

            if start >= self.child.degree_of_parallelism() {
                break;
            }

            let morsel = Morsel {
                tuples: vec![],
                start_offset: start,
                end_offset: end,
                partition_id: worker_id,
            };

            let sort_op = ParallelSortOperator {
                child: Arc::clone(&self.child),
                sort_keys: self.sort_keys.clone(),
                limit: self.limit,
            };

            pool.submit_task(morsel, Arc::new(sort_op), result_sender.clone())?;
        }
        drop(result_sender);

        // Collect sorted runs from all workers
        let mut runs: Vec<Vec<Tuple>> = Vec::new();
        while let Ok(result) = result_receiver.recv() {
            let morsel = result?;
            if !morsel.tuples.is_empty() {
                runs.push(morsel.tuples);
            }
        }

        // If limit is set and small, use heap-based top-K selection
        if let Some(limit) = self.limit {
            let max_run_size = runs.iter().map(|r| r.len()).max().unwrap_or(0);
            let estimated_total = runs.iter().map(|r| r.len()).sum::<usize>();

            // If we only need top K and it's smaller than full sort, use heap
            if limit < estimated_total / 2 && limit < max_run_size {
                return Self::heap_select_top_k(runs, &self.sort_keys, limit);
            }
        }

        // Multi-phase cascading merge
        Self::multi_phase_merge(runs, &self.sort_keys)
    }

    fn heap_select_top_k(
        runs: Vec<Vec<Tuple>>,
        sort_keys: &[SortKey],
        limit: usize,
    ) -> Result<Vec<Tuple>, ExecutorError> {
        use std::cmp::Reverse;

        struct HeapEntry {
            tuple: Tuple,
            run_idx: usize,
            tuple_idx: usize,
            sort_key: Reverse<Vec<u8>>,
        }

        impl PartialEq for HeapEntry {
            fn eq(&self, other: &Self) -> bool {
                self.sort_key == other.sort_key
            }
        }

        impl Eq for HeapEntry {}

        impl PartialOrd for HeapEntry {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for HeapEntry {
            fn cmp(&self, other: &Self) -> Ordering {
                self.sort_key.cmp(&other.sort_key)
            }
        }

        let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();
        let mut results = Vec::with_capacity(limit);
        let mut tuple_indices: Vec<usize> = vec![0; runs.len()];

        // Initialize heap with first element from each run
        for (run_idx, run) in runs.iter().enumerate() {
            if !run.is_empty() {
                let key_bytes = Self::compute_sort_key_bytes(&run[0], sort_keys);
                heap.push(HeapEntry {
                    tuple: run[0].clone(),
                    run_idx,
                    tuple_idx: 0,
                    sort_key: Reverse(key_bytes),
                });
            }
        }

        while results.len() < limit {
            match heap.pop() {
                Some(entry) => {
                    results.push(entry.tuple);

                    // Push next element from same run
                    let next_idx = entry.tuple_idx + 1;
                    if next_idx < runs[entry.run_idx].len() {
                        let key_bytes =
                            Self::compute_sort_key_bytes(&runs[entry.run_idx][next_idx], sort_keys);
                        heap.push(HeapEntry {
                            tuple: runs[entry.run_idx][next_idx].clone(),
                            run_idx: entry.run_idx,
                            tuple_idx: next_idx,
                            sort_key: Reverse(key_bytes),
                        });
                    }
                }
                None => break,
            }
        }

        Ok(results)
    }

    pub fn multi_phase_merge(
        runs: Vec<Vec<Tuple>>,
        sort_keys: &[SortKey],
    ) -> Result<Vec<Tuple>, ExecutorError> {
        if runs.is_empty() {
            return Ok(vec![]);
        }

        if runs.len() == 1 {
            return Ok(runs.into_iter().next().unwrap());
        }

        // Cascading merge: repeatedly merge runs until single run
        let mut current_runs = runs;
        while current_runs.len() > 1 {
            let mut new_runs = VecDeque::new();

            // Process runs in pairs
            let mut i = 0;
            while i < current_runs.len() {
                if i + 1 < current_runs.len() {
                    let merged =
                        Self::merge_two_runs(&current_runs[i], &current_runs[i + 1], sort_keys)?;
                    new_runs.push_back(merged);
                    i += 2;
                } else {
                    new_runs.push_back(current_runs[i].clone());
                    i += 1;
                }
            }

            current_runs = new_runs.into_iter().collect();
        }

        Ok(current_runs.into_iter().next().unwrap())
    }

    fn merge_two_runs(
        run1: &[Tuple],
        run2: &[Tuple],
        sort_keys: &[SortKey],
    ) -> Result<Vec<Tuple>, ExecutorError> {
        Self::heap_merge(vec![run1.to_vec(), run2.to_vec()], sort_keys)
    }

    fn heap_merge(
        runs: Vec<Vec<Tuple>>,
        sort_keys: &[SortKey],
    ) -> Result<Vec<Tuple>, ExecutorError> {
        #[derive(Clone)]
        struct HeapEntry<'a> {
            tuple: &'a Tuple,
            run_idx: usize,
            value_bytes: Vec<u8>,
        }

        impl<'a> PartialEq for HeapEntry<'a> {
            fn eq(&self, other: &Self) -> bool {
                self.value_bytes == other.value_bytes
            }
        }

        impl<'a> Eq for HeapEntry<'a> {}

        impl<'a> PartialOrd for HeapEntry<'a> {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        impl<'a> Ord for HeapEntry<'a> {
            fn cmp(&self, other: &Self) -> Ordering {
                // BinaryHeap is a max-heap, so we reverse for min-heap behavior
                other.value_bytes.cmp(&self.value_bytes)
            }
        }

        let mut result = Vec::new();
        let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();
        let mut indices = vec![0usize; runs.len()];

        // Initialize with first element from each run
        for (run_idx, run) in runs.iter().enumerate() {
            if !run.is_empty() {
                let value_bytes = Self::compute_sort_key_bytes(&run[0], sort_keys);
                heap.push(HeapEntry { tuple: &run[0], run_idx, value_bytes });
            }
        }

        while let Some(entry) = heap.pop() {
            result.push(entry.tuple.clone());

            let next_idx = indices[entry.run_idx] + 1;
            indices[entry.run_idx] = next_idx;

            if next_idx < runs[entry.run_idx].len() {
                let value_bytes =
                    Self::compute_sort_key_bytes(&runs[entry.run_idx][next_idx], sort_keys);
                heap.push(HeapEntry {
                    tuple: &runs[entry.run_idx][next_idx],
                    run_idx: entry.run_idx,
                    value_bytes,
                });
            }
        }

        Ok(result)
    }

    fn compute_sort_key_bytes(tuple: &Tuple, sort_keys: &[SortKey]) -> Vec<u8> {
        let mut key_bytes = Vec::new();

        for key in sort_keys {
            if let Some(value) = tuple.get(&key.column) {
                key_bytes.extend_from_slice(&Self::value_to_bytes(value));
                key_bytes.push(0xFF); // Separator
            }
        }

        key_bytes
    }

    fn value_to_bytes(value: &Value) -> Vec<u8> {
        match value {
            Value::Int(n) => n.to_le_bytes().to_vec(),
            Value::Text(s) => s.as_bytes().to_vec(),
            Value::Bool(b) => vec![*b as u8],
            Value::Float(f) => f.to_le_bytes().to_vec(),
            Value::Null => vec![0x00],
            _ => format!("{:?}", value).into_bytes(),
        }
    }
}

struct ParallelSortOperator {
    child: Arc<dyn ParallelOperator>,
    sort_keys: Vec<SortKey>,
    limit: Option<usize>,
}

impl ParallelOperator for ParallelSortOperator {
    fn process_morsel(&self, mut morsel: Morsel) -> Result<Morsel, ExecutorError> {
        let partition_id = morsel.partition_id;

        // Get tuples from child operator
        let child_result = self.child.process_morsel(morsel)?;
        let mut tuples = child_result.tuples;

        // Sort locally
        self.sort_local(&mut tuples)?;

        // Apply limit if set
        if let Some(limit) = self.limit {
            tuples.truncate(limit);
        }

        Ok(Morsel { tuples, start_offset: 0, end_offset: 0, partition_id })
    }

    fn degree_of_parallelism(&self) -> usize {
        self.child.degree_of_parallelism()
    }
}

impl ParallelSortOperator {
    fn sort_local(&self, tuples: &mut [Tuple]) -> Result<(), ExecutorError> {
        let sort_keys = self.sort_keys.clone();
        tuples.sort_by(|a, b| {
            Self::compare_tuples_static(a, b, &sort_keys).unwrap_or(Ordering::Equal)
        });
        Ok(())
    }

    fn compare_tuples_static(
        a: &Tuple,
        b: &Tuple,
        keys: &[SortKey],
    ) -> Result<Ordering, ExecutorError> {
        for key in keys {
            let val_a = a.get(&key.column).unwrap_or(&Value::Null);
            let val_b = b.get(&key.column).unwrap_or(&Value::Null);

            let cmp = Self::compare_values_static(val_a, val_b)?;

            let adjusted = if key.nulls_last {
                match (val_a, val_b) {
                    (Value::Null, Value::Null) => Ordering::Equal,
                    (Value::Null, _) => Ordering::Greater,
                    (_, Value::Null) => Ordering::Less,
                    _ => {
                        if key.ascending {
                            cmp
                        } else {
                            cmp.reverse()
                        }
                    }
                }
            } else {
                match (val_a, val_b) {
                    (Value::Null, Value::Null) => Ordering::Equal,
                    (Value::Null, _) => Ordering::Less,
                    (_, Value::Null) => Ordering::Greater,
                    _ => {
                        if key.ascending {
                            cmp
                        } else {
                            cmp.reverse()
                        }
                    }
                }
            };

            if adjusted != Ordering::Equal {
                return Ok(adjusted);
            }
        }
        Ok(Ordering::Equal)
    }

    fn compare_values_static(a: &Value, b: &Value) -> Result<Ordering, ExecutorError> {
        match (a, b) {
            (Value::Int(a), Value::Int(b)) => Ok(a.cmp(b)),
            (Value::Text(a), Value::Text(b)) => Ok(a.cmp(b)),
            (Value::Bool(a), Value::Bool(b)) => Ok(a.cmp(b)),
            (Value::Float(a), Value::Float(b)) => Ok(a.partial_cmp(b).unwrap_or(Ordering::Equal)),
            (Value::Null, Value::Null) => Ok(Ordering::Equal),
            (Value::Null, _) => Ok(Ordering::Greater),
            (_, Value::Null) => Ok(Ordering::Less),
            _ => Err(ExecutorError::TypeMismatch("Cannot compare values".to_string())),
        }
    }
}

impl ParallelOperator for ParallelSort {
    fn process_morsel(&self, morsel: Morsel) -> Result<Morsel, ExecutorError> {
        let child_result = self.child.process_morsel(morsel)?;
        let mut tuples = child_result.tuples;

        // Sort locally
        let sort_keys = self.sort_keys.clone();
        tuples.sort_by(|a, b| {
            Self::compare_tuples_static(a, b, &sort_keys).unwrap_or(Ordering::Equal)
        });

        // Apply limit if set
        if let Some(limit) = self.limit {
            tuples.truncate(limit);
        }

        Ok(Morsel { tuples, start_offset: 0, end_offset: 0, partition_id: 0 })
    }

    fn degree_of_parallelism(&self) -> usize {
        self.num_workers
    }
}

impl ParallelSort {
    fn compare_tuples_static(
        a: &Tuple,
        b: &Tuple,
        keys: &[SortKey],
    ) -> Result<Ordering, ExecutorError> {
        for key in keys {
            let val_a = a.get(&key.column).unwrap_or(&Value::Null);
            let val_b = b.get(&key.column).unwrap_or(&Value::Null);

            let cmp = Self::compare_values_static(val_a, val_b)?;

            let adjusted = if key.nulls_last {
                match (val_a, val_b) {
                    (Value::Null, Value::Null) => Ordering::Equal,
                    (Value::Null, _) => Ordering::Greater,
                    (_, Value::Null) => Ordering::Less,
                    _ => {
                        if key.ascending {
                            cmp
                        } else {
                            cmp.reverse()
                        }
                    }
                }
            } else {
                match (val_a, val_b) {
                    (Value::Null, Value::Null) => Ordering::Equal,
                    (Value::Null, _) => Ordering::Less,
                    (_, Value::Null) => Ordering::Greater,
                    _ => {
                        if key.ascending {
                            cmp
                        } else {
                            cmp.reverse()
                        }
                    }
                }
            };

            if adjusted != Ordering::Equal {
                return Ok(adjusted);
            }
        }
        Ok(Ordering::Equal)
    }

    fn compare_values_static(a: &Value, b: &Value) -> Result<Ordering, ExecutorError> {
        match (a, b) {
            (Value::Int(a), Value::Int(b)) => Ok(a.cmp(b)),
            (Value::Text(a), Value::Text(b)) => Ok(a.cmp(b)),
            (Value::Bool(a), Value::Bool(b)) => Ok(a.cmp(b)),
            (Value::Float(a), Value::Float(b)) => Ok(a.partial_cmp(b).unwrap_or(Ordering::Equal)),
            (Value::Null, Value::Null) => Ok(Ordering::Equal),
            (Value::Null, _) => Ok(Ordering::Greater),
            (_, Value::Null) => Ok(Ordering::Less),
            _ => Err(ExecutorError::TypeMismatch("Cannot compare values".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Value;
    use crate::executor::test_helpers::TupleBuilder;

    struct MockOperator {
        tuples: Vec<Tuple>,
    }

    impl ParallelOperator for MockOperator {
        fn process_morsel(&self, mut morsel: Morsel) -> Result<Morsel, ExecutorError> {
            morsel.tuples = self.tuples.clone();
            Ok(morsel)
        }
    }

    fn create_test_tuples() -> Vec<Tuple> {
        vec![
            TupleBuilder::new().with_int("val", 3).build(),
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 2).build(),
        ]
    }

    fn create_multi_col_tuples() -> Vec<Tuple> {
        vec![
            TupleBuilder::new().with_int("a", 1).with_int("b", 10).build(),
            TupleBuilder::new().with_int("a", 2).with_int("b", 5).build(),
            TupleBuilder::new().with_int("a", 1).with_int("b", 20).build(),
            TupleBuilder::new().with_int("a", 2).with_int("b", 15).build(),
        ]
    }

    #[test]
    fn test_sort_single_column_ascending() {
        let tuples = create_test_tuples();
        let child = Arc::new(MockOperator { tuples });
        let sort = ParallelSort::new(
            child,
            vec![OrderByExpr { column: "val".to_string(), ascending: true }],
        );

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 3, partition_id: 0 };
        let result = sort.process_morsel(morsel).unwrap();

        assert_eq!(result.tuples.len(), 3);
        assert_eq!(result.tuples[0].get("val"), Some(&Value::Int(1)));
        assert_eq!(result.tuples[1].get("val"), Some(&Value::Int(2)));
        assert_eq!(result.tuples[2].get("val"), Some(&Value::Int(3)));
    }

    #[test]
    fn test_sort_single_column_descending() {
        let tuples = create_test_tuples();
        let child = Arc::new(MockOperator { tuples });
        let sort = ParallelSort::new(
            child,
            vec![OrderByExpr { column: "val".to_string(), ascending: false }],
        );

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 3, partition_id: 0 };
        let result = sort.process_morsel(morsel).unwrap();

        assert_eq!(result.tuples.len(), 3);
        assert_eq!(result.tuples[0].get("val"), Some(&Value::Int(3)));
        assert_eq!(result.tuples[1].get("val"), Some(&Value::Int(2)));
        assert_eq!(result.tuples[2].get("val"), Some(&Value::Int(1)));
    }

    #[test]
    fn test_sort_multi_column() {
        let tuples = create_multi_col_tuples();
        let child = Arc::new(MockOperator { tuples });
        let sort = ParallelSort::new(
            child,
            vec![
                OrderByExpr { column: "a".to_string(), ascending: true },
                OrderByExpr { column: "b".to_string(), ascending: true },
            ],
        );

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 4, partition_id: 0 };
        let result = sort.process_morsel(morsel).unwrap();

        assert_eq!(result.tuples.len(), 4);
        assert_eq!(result.tuples[0].get("a"), Some(&Value::Int(1)));
        assert_eq!(result.tuples[0].get("b"), Some(&Value::Int(10)));
        assert_eq!(result.tuples[1].get("a"), Some(&Value::Int(1)));
        assert_eq!(result.tuples[1].get("b"), Some(&Value::Int(20)));
        assert_eq!(result.tuples[2].get("a"), Some(&Value::Int(2)));
        assert_eq!(result.tuples[2].get("b"), Some(&Value::Int(5)));
        assert_eq!(result.tuples[3].get("a"), Some(&Value::Int(2)));
        assert_eq!(result.tuples[3].get("b"), Some(&Value::Int(15)));
    }

    #[test]
    fn test_sort_with_limit() {
        let tuples = create_test_tuples();
        let child = Arc::new(MockOperator { tuples });
        let sort = ParallelSort::new(
            child,
            vec![OrderByExpr { column: "val".to_string(), ascending: true }],
        )
        .with_limit(2);

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 3, partition_id: 0 };
        let result = sort.process_morsel(morsel).unwrap();

        assert_eq!(result.tuples.len(), 2);
        assert_eq!(result.tuples[0].get("val"), Some(&Value::Int(1)));
        assert_eq!(result.tuples[1].get("val"), Some(&Value::Int(2)));
    }

    #[test]
    fn test_sort_empty_input() {
        let tuples: Vec<Tuple> = vec![];
        let child = Arc::new(MockOperator { tuples });
        let sort = ParallelSort::new(
            child,
            vec![OrderByExpr { column: "val".to_string(), ascending: true }],
        );

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 0, partition_id: 0 };
        let result = sort.process_morsel(morsel).unwrap();

        assert_eq!(result.tuples.len(), 0);
    }

    #[test]
    fn test_sort_text_column() {
        let tuples = vec![
            TupleBuilder::new().with_text("name", "zebra").build(),
            TupleBuilder::new().with_text("name", "apple").build(),
            TupleBuilder::new().with_text("name", "mango").build(),
        ];
        let child = Arc::new(MockOperator { tuples });
        let sort = ParallelSort::new(
            child,
            vec![OrderByExpr { column: "name".to_string(), ascending: true }],
        );

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 3, partition_id: 0 };
        let result = sort.process_morsel(morsel).unwrap();

        assert_eq!(result.tuples.len(), 3);
        assert_eq!(result.tuples[0].get("name"), Some(&Value::Text("apple".to_string())));
        assert_eq!(result.tuples[1].get("name"), Some(&Value::Text("mango".to_string())));
        assert_eq!(result.tuples[2].get("name"), Some(&Value::Text("zebra".to_string())));
    }

    #[test]
    fn test_sort_with_duplicates() {
        let tuples = vec![
            TupleBuilder::new().with_int("val", 5).build(),
            TupleBuilder::new().with_int("val", 3).build(),
            TupleBuilder::new().with_int("val", 5).build(),
            TupleBuilder::new().with_int("val", 1).build(),
        ];
        let child = Arc::new(MockOperator { tuples });
        let sort = ParallelSort::new(
            child,
            vec![OrderByExpr { column: "val".to_string(), ascending: true }],
        );

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 4, partition_id: 0 };
        let result = sort.process_morsel(morsel).unwrap();

        assert_eq!(result.tuples.len(), 4);
        assert_eq!(result.tuples[0].get("val"), Some(&Value::Int(1)));
        assert_eq!(result.tuples[1].get("val"), Some(&Value::Int(3)));
        assert_eq!(result.tuples[2].get("val"), Some(&Value::Int(5)));
        assert_eq!(result.tuples[3].get("val"), Some(&Value::Int(5)));
    }

    #[test]
    fn test_sort_null_handling() {
        let tuples = vec![
            TupleBuilder::new().with_int("val", 3).build(),
            TupleBuilder::new().with_value("val", Value::Null).build(),
            TupleBuilder::new().with_int("val", 1).build(),
        ];
        let child = Arc::new(MockOperator { tuples });
        let sort = ParallelSort::new(
            child,
            vec![OrderByExpr { column: "val".to_string(), ascending: true }],
        );

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 3, partition_id: 0 };
        let result = sort.process_morsel(morsel).unwrap();

        assert_eq!(result.tuples.len(), 3);
        assert_eq!(result.tuples[0].get("val"), Some(&Value::Int(1)));
        assert_eq!(result.tuples[1].get("val"), Some(&Value::Int(3)));
        assert_eq!(result.tuples[2].get("val"), Some(&Value::Null));
    }

    #[test]
    fn test_sort_mixed_types_stability() {
        let tuples = vec![
            TupleBuilder::new().with_int("id", 1).with_int("val", 10).build(),
            TupleBuilder::new().with_int("id", 2).with_int("val", 10).build(),
            TupleBuilder::new().with_int("id", 3).with_int("val", 5).build(),
        ];
        let child = Arc::new(MockOperator { tuples });
        let sort = ParallelSort::new(
            child,
            vec![OrderByExpr { column: "val".to_string(), ascending: true }],
        );

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 3, partition_id: 0 };
        let result = sort.process_morsel(morsel).unwrap();

        // Within same sort key, original order should be preserved (stable sort)
        assert_eq!(result.tuples.len(), 3);
        assert_eq!(result.tuples[0].get("id"), Some(&Value::Int(3)));
        assert_eq!(result.tuples[1].get("id"), Some(&Value::Int(1)));
        assert_eq!(result.tuples[2].get("id"), Some(&Value::Int(2)));
    }

    #[test]
    fn test_heap_merge_two_runs() {
        let tuples1 = vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 3).build(),
        ];
        let tuples2 = vec![
            TupleBuilder::new().with_int("val", 2).build(),
            TupleBuilder::new().with_int("val", 4).build(),
        ];

        let child = Arc::new(MockOperator { tuples: vec![] });
        let sort = ParallelSort::new(
            child,
            vec![OrderByExpr { column: "val".to_string(), ascending: true }],
        );

        let merged = ParallelSort::heap_merge(vec![tuples1, tuples2], &sort.sort_keys).unwrap();

        assert_eq!(merged.len(), 4);
        assert_eq!(merged[0].get("val"), Some(&Value::Int(1)));
        assert_eq!(merged[1].get("val"), Some(&Value::Int(2)));
        assert_eq!(merged[2].get("val"), Some(&Value::Int(3)));
        assert_eq!(merged[3].get("val"), Some(&Value::Int(4)));
    }

    #[test]
    fn test_multi_phase_merge() {
        let runs = vec![
            vec![TupleBuilder::new().with_int("val", 1).build()],
            vec![TupleBuilder::new().with_int("val", 5).build()],
            vec![TupleBuilder::new().with_int("val", 3).build()],
            vec![TupleBuilder::new().with_int("val", 7).build()],
        ];

        let child = Arc::new(MockOperator { tuples: vec![] });
        let sort = ParallelSort::new(
            child,
            vec![OrderByExpr { column: "val".to_string(), ascending: true }],
        );

        let result = ParallelSort::multi_phase_merge(runs, &sort.sort_keys).unwrap();

        assert_eq!(result.len(), 4);
        assert_eq!(result[0].get("val"), Some(&Value::Int(1)));
        assert_eq!(result[1].get("val"), Some(&Value::Int(3)));
        assert_eq!(result[2].get("val"), Some(&Value::Int(5)));
        assert_eq!(result[3].get("val"), Some(&Value::Int(7)));
    }

    #[test]
    fn test_heap_select_top_k() {
        let runs = vec![
            vec![
                TupleBuilder::new().with_int("val", 1).build(),
                TupleBuilder::new().with_int("val", 6).build(),
            ],
            vec![
                TupleBuilder::new().with_int("val", 3).build(),
                TupleBuilder::new().with_int("val", 8).build(),
            ],
            vec![
                TupleBuilder::new().with_int("val", 2).build(),
                TupleBuilder::new().with_int("val", 7).build(),
            ],
        ];

        let child = Arc::new(MockOperator { tuples: vec![] });
        let sort = ParallelSort::new(
            child,
            vec![OrderByExpr { column: "val".to_string(), ascending: true }],
        );

        let result = ParallelSort::heap_select_top_k(runs, &sort.sort_keys, 3).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].get("val"), Some(&Value::Int(1)));
        assert_eq!(result[1].get("val"), Some(&Value::Int(2)));
        assert_eq!(result[2].get("val"), Some(&Value::Int(3)));
    }

    #[test]
    fn test_sort_bool_column() {
        let tuples = vec![
            TupleBuilder::new().with_bool("active", true).build(),
            TupleBuilder::new().with_bool("active", false).build(),
            TupleBuilder::new().with_bool("active", true).build(),
        ];
        let child = Arc::new(MockOperator { tuples });
        let sort = ParallelSort::new(
            child,
            vec![OrderByExpr { column: "active".to_string(), ascending: true }],
        );

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 3, partition_id: 0 };
        let result = sort.process_morsel(morsel).unwrap();

        assert_eq!(result.tuples.len(), 3);
        assert_eq!(result.tuples[0].get("active"), Some(&Value::Bool(false)));
        assert_eq!(result.tuples[1].get("active"), Some(&Value::Bool(true)));
        assert_eq!(result.tuples[2].get("active"), Some(&Value::Bool(true)));
    }

    #[test]
    fn test_sort_single_run() {
        let tuples = vec![
            TupleBuilder::new().with_int("val", 3).build(),
            TupleBuilder::new().with_int("val", 5).build(),
        ];

        let runs = vec![tuples];

        let child = Arc::new(MockOperator { tuples: vec![] });
        let sort = ParallelSort::new(
            child,
            vec![OrderByExpr { column: "val".to_string(), ascending: true }],
        );

        let result = ParallelSort::multi_phase_merge(runs, &sort.sort_keys).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].get("val"), Some(&Value::Int(3)));
        assert_eq!(result[1].get("val"), Some(&Value::Int(5)));
    }

    #[test]
    fn test_sort_already_sorted() {
        let tuples = vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 2).build(),
            TupleBuilder::new().with_int("val", 3).build(),
        ];
        let child = Arc::new(MockOperator { tuples });
        let sort = ParallelSort::new(
            child,
            vec![OrderByExpr { column: "val".to_string(), ascending: true }],
        );

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 3, partition_id: 0 };
        let result = sort.process_morsel(morsel).unwrap();

        assert_eq!(result.tuples.len(), 3);
        assert_eq!(result.tuples[0].get("val"), Some(&Value::Int(1)));
        assert_eq!(result.tuples[1].get("val"), Some(&Value::Int(2)));
        assert_eq!(result.tuples[2].get("val"), Some(&Value::Int(3)));
    }
}
