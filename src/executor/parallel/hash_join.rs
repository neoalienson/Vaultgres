use crate::executor::old_executor::{OldExecutorError as ExecutorError, SimpleTuple};
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
    partitions: Vec<Mutex<HashMap<Vec<u8>, Vec<SimpleTuple>>>>,
}

impl ConcurrentHashTable {
    pub fn new(num_partitions: usize) -> Self {
        let partitions = (0..num_partitions).map(|_| Mutex::new(HashMap::new())).collect();
        Self { partitions }
    }

    pub fn insert(&self, partition_id: usize, key: Vec<u8>, tuple: SimpleTuple) {
        let mut partition = self.partitions[partition_id].lock().unwrap();
        partition.entry(key).or_default().push(tuple);
    }

    pub fn probe(&self, partition_id: usize, key: &[u8]) -> Vec<SimpleTuple> {
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
    ) -> Result<Vec<SimpleTuple>, ExecutorError> {
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

        while let Ok(result) = build_receiver.recv() {
            let morsel = result?;
            for tuple in morsel.tuples {
                let key = tuple.data.clone();
                let partition_id = self.partition_strategy.partition_key(&key);
                self.hash_table.insert(partition_id, key, tuple);
            }
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
                let key = &tuple.data;
                let partition_id = self.partition_strategy.partition_key(key);
                let matches = self.hash_table.probe(partition_id, key);
                for matched in matches {
                    let mut joined = SimpleTuple { data: matched.data.clone() };
                    joined.data.extend_from_slice(&tuple.data);
                    all_tuples.push(joined);
                }
            }
        }

        Ok(all_tuples)
    }

    pub fn build_phase(&self, morsel: Morsel) -> Result<(), ExecutorError> {
        let build_result = self.build_side.process_morsel(morsel)?;

        for tuple in build_result.tuples {
            let key = tuple.data.clone();
            let partition_id = self.partition_strategy.partition_key(&key);
            self.hash_table.insert(partition_id, key, tuple);
        }

        Ok(())
    }

    pub fn probe_phase(&self, morsel: Morsel) -> Result<Vec<SimpleTuple>, ExecutorError> {
        let probe_result = self.probe_side.process_morsel(morsel)?;
        let mut joined_tuples = Vec::new();

        for tuple in probe_result.tuples {
            let key = &tuple.data;
            let partition_id = self.partition_strategy.partition_key(key);
            let matches = self.hash_table.probe(partition_id, key);

            for matched in matches {
                let mut joined = SimpleTuple { data: matched.data.clone() };
                joined.data.extend_from_slice(&tuple.data);
                joined_tuples.push(joined);
            }
        }

        Ok(joined_tuples)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockOperator {
        tuples: Vec<SimpleTuple>,
    }

    impl ParallelOperator for MockOperator {
        fn process_morsel(&self, mut morsel: Morsel) -> Result<Morsel, ExecutorError> {
            morsel.tuples = self.tuples.clone();
            Ok(morsel)
        }
    }

    #[test]
    fn test_hash_table() {
        let ht = ConcurrentHashTable::new(4);
        let tuple = SimpleTuple { data: vec![1, 2, 3] };
        ht.insert(0, vec![1], tuple.clone());

        let result = ht.probe(0, &[1]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].data, vec![1, 2, 3]);
    }

    #[test]
    fn test_hash_join() {
        let build_tuples = vec![SimpleTuple { data: vec![1] }];
        let probe_tuples = vec![SimpleTuple { data: vec![1] }];

        let build_op = Arc::new(MockOperator { tuples: build_tuples });
        let probe_op = Arc::new(MockOperator { tuples: probe_tuples });

        let join = ParallelHashJoin::new(build_op, probe_op, 4);

        let build_morsel =
            Morsel { tuples: vec![], start_offset: 0, end_offset: 1, partition_id: 0 };
        join.build_phase(build_morsel).unwrap();

        let probe_morsel =
            Morsel { tuples: vec![], start_offset: 0, end_offset: 1, partition_id: 0 };
        let result = join.probe_phase(probe_morsel).unwrap();
        assert_eq!(result.len(), 1);
    }
}
