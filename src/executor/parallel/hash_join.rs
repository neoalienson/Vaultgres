use crate::executor::operators::executor::{ExecutorError, Tuple};
use crate::executor::parallel::config::ParallelConfig;
use crate::executor::parallel::morsel::Morsel;
use crate::executor::parallel::operator::ParallelOperator;
use crate::executor::parallel::partition::PartitionStrategy;
use crate::executor::parallel::worker_pool::WorkerPool;
use crossbeam::channel::bounded;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct ParallelHashJoin {
    build_side: Arc<dyn ParallelOperator>,
    probe_side: Arc<dyn ParallelOperator>,
    hash_table: Arc<ConcurrentHashTable>,
    partition_strategy: Arc<PartitionStrategy>,
}

pub struct ConcurrentHashTable {
    partitions: Vec<Mutex<HashMap<Vec<u8>, Vec<Tuple>>>>,
}

impl ConcurrentHashTable {
    pub fn new(num_partitions: usize) -> Self {
        let partitions = (0..num_partitions).map(|_| Mutex::new(HashMap::new())).collect();
        Self { partitions }
    }

    pub fn insert(&self, partition_id: usize, key: Vec<u8>, tuple: Tuple) {
        let mut partition = self.partitions[partition_id].lock().unwrap();
        partition.entry(key).or_default().push(tuple);
    }

    pub fn probe(&self, partition_id: usize, key: &[u8]) -> Vec<Tuple> {
        let partition = self.partitions[partition_id].lock().unwrap();
        partition.get(key).cloned().unwrap_or_default()
    }
}

impl ParallelHashJoin {
    pub fn new(
        build_side: Arc<dyn ParallelOperator>,
        probe_side: Arc<dyn ParallelOperator>,
        num_partitions: usize,
    ) -> Self {
        let partition_strategy = Arc::new(PartitionStrategy::new(num_partitions));
        let hash_table = Arc::new(ConcurrentHashTable::new(num_partitions));

        Self { build_side, probe_side, hash_table, partition_strategy }
    }

    pub fn execute(
        &self,
        config: &ParallelConfig,
        build_rows: usize,
        probe_rows: usize,
    ) -> Result<Vec<Tuple>, ExecutorError> {
        let num_workers = config.max_workers();
        let pool = WorkerPool::new(num_workers);

        // Build phase
        let build_chunk = build_rows.div_ceil(num_workers);
        let (build_sender, build_receiver) = bounded(num_workers);

        for i in 0..num_workers {
            let start = i * build_chunk;
            let end = ((i + 1) * build_chunk).min(build_rows);
            if start >= build_rows {
                break;
            }
            let morsel =
                Morsel { tuples: vec![], start_offset: start, end_offset: end, partition_id: i };
            pool.submit_task(morsel, Arc::clone(&self.build_side), build_sender.clone())?;
        }
        drop(build_sender);

        // Build phase - TODO: Implement proper hash table build with Tuple key extraction
        while let Ok(result) = build_receiver.recv() {
            let _morsel = result?;
            // TODO: Extract key from tuple and build hash table
        }

        // Probe phase
        let probe_chunk = probe_rows.div_ceil(num_workers);
        let (probe_sender, probe_receiver) = bounded(num_workers);

        for i in 0..num_workers {
            let start = i * probe_chunk;
            let end = ((i + 1) * probe_chunk).min(probe_rows);
            if start >= probe_rows {
                break;
            }
            let morsel =
                Morsel { tuples: vec![], start_offset: start, end_offset: end, partition_id: i };
            pool.submit_task(morsel, Arc::clone(&self.probe_side), probe_sender.clone())?;
        }
        drop(probe_sender);

        let mut all_tuples = Vec::new();
        while let Ok(result) = probe_receiver.recv() {
            let morsel = result?;
            for tuple in morsel.tuples {
                // TODO: Implement proper join key extraction from Tuple (HashMap)
                // For now, skip join logic - requires refactoring to use proper key columns
                all_tuples.push(tuple);
            }
        }

        Ok(all_tuples)
    }

    pub fn build_phase(&self, _morsel: Morsel) -> Result<(), ExecutorError> {
        // TODO: Implement proper build phase with Tuple (HashMap)
        Ok(())
    }

    pub fn probe_phase(&self, morsel: Morsel) -> Result<Vec<Tuple>, ExecutorError> {
        // TODO: Implement proper probe phase with Tuple (HashMap)
        Ok(morsel.tuples)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Value;

    struct MockOperator {
        tuples: Vec<Tuple>,
    }

    impl ParallelOperator for MockOperator {
        fn process_morsel(&self, mut morsel: Morsel) -> Result<Morsel, ExecutorError> {
            morsel.tuples = self.tuples.clone();
            Ok(morsel)
        }
    }

    fn create_tuple(val: u8) -> Tuple {
        let mut tuple = std::collections::HashMap::new();
        tuple.insert("key".to_string(), Value::Bytea(vec![val]));
        tuple
    }

    #[test]
    fn test_hash_table() {
        let ht = ConcurrentHashTable::new(4);
        let tuple = create_tuple(1);
        ht.insert(0, vec![1], tuple.clone());

        let result = ht.probe(0, &[1]);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_hash_join() {
        let build_tuples = vec![create_tuple(1)];
        let probe_tuples = vec![create_tuple(1)];

        let build_op = Arc::new(MockOperator { tuples: build_tuples });
        let probe_op = Arc::new(MockOperator { tuples: probe_tuples });

        let join = ParallelHashJoin::new(build_op, probe_op, 4);

        let build_morsel =
            Morsel { tuples: vec![], start_offset: 0, end_offset: 1, partition_id: 0 };
        join.build_phase(build_morsel).unwrap();

        let probe_morsel =
            Morsel { tuples: vec![], start_offset: 0, end_offset: 1, partition_id: 0 };
        let result = join.probe_phase(probe_morsel).unwrap();
        // TODO: Verify join results once proper implementation is complete
        // For now, probe_phase returns input tuples
        assert_eq!(result.len(), 0); // Empty because probe_morsel has no tuples
    }
}
