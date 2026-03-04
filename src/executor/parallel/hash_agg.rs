use crate::executor::old_executor::{OldExecutorError as ExecutorError, SimpleTuple};
use crate::executor::parallel::config::ParallelConfig;
use crate::executor::parallel::morsel::Morsel;
use crate::executor::parallel::operator::ParallelOperator;
use crate::executor::parallel::worker_pool::WorkerPool;
use crossbeam::channel::bounded;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct AggregateState {
    pub count: u64,
    pub sum: f64,
    pub min: Option<Vec<u8>>,
    pub max: Option<Vec<u8>>,
}

impl Default for AggregateState {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateState {
    pub fn new() -> Self {
        Self { count: 0, sum: 0.0, min: None, max: None }
    }

    pub fn update(&mut self, value: &[u8]) {
        self.count += 1;

        if let Some(&first_byte) = value.first() {
            self.sum += first_byte as f64;
        }

        match &self.min {
            None => self.min = Some(value.to_vec()),
            Some(min) if value < min.as_slice() => self.min = Some(value.to_vec()),
            _ => {}
        }

        match &self.max {
            None => self.max = Some(value.to_vec()),
            Some(max) if value > max.as_slice() => self.max = Some(value.to_vec()),
            _ => {}
        }
    }

    pub fn merge(&mut self, other: &AggregateState) {
        self.count += other.count;
        self.sum += other.sum;

        if let Some(other_min) = &other.min {
            match &self.min {
                None => self.min = Some(other_min.clone()),
                Some(min) if other_min < min => self.min = Some(other_min.clone()),
                _ => {}
            }
        }

        if let Some(other_max) = &other.max {
            match &self.max {
                None => self.max = Some(other_max.clone()),
                Some(max) if other_max > max => self.max = Some(other_max.clone()),
                _ => {}
            }
        }
    }
}

pub struct ParallelHashAgg {
    child: Arc<dyn ParallelOperator>,
    hash_tables: Vec<Mutex<HashMap<Vec<u8>, AggregateState>>>,
}

impl ParallelHashAgg {
    pub fn new(child: Arc<dyn ParallelOperator>, num_workers: usize) -> Self {
        let hash_tables = (0..num_workers).map(|_| Mutex::new(HashMap::new())).collect();

        Self { child, hash_tables }
    }

    pub fn execute(
        &self,
        config: &ParallelConfig,
        row_count: usize,
    ) -> Result<Vec<SimpleTuple>, ExecutorError> {
        let num_workers = config.max_workers();
        let pool = WorkerPool::new(num_workers);
        let chunk_size = row_count.div_ceil(num_workers);

        let (result_sender, result_receiver) = bounded(num_workers);

        for i in 0..num_workers {
            let start = i * chunk_size;
            let end = ((i + 1) * chunk_size).min(row_count);
            if start >= row_count {
                break;
            }
            let morsel =
                Morsel { tuples: vec![], start_offset: start, end_offset: end, partition_id: i };
            pool.submit_task(morsel, Arc::clone(&self.child), result_sender.clone())?;
        }
        drop(result_sender);

        let mut worker_id = 0;
        while let Ok(result) = result_receiver.recv() {
            let morsel = result?;
            let mut hash_table =
                self.hash_tables[worker_id % self.hash_tables.len()].lock().unwrap();
            for tuple in morsel.tuples {
                let key = tuple.data.clone();
                let state = hash_table.entry(key.clone()).or_default();
                state.update(&key);
            }
            worker_id += 1;
        }

        self.global_combine()
    }

    pub fn local_aggregate(&self, morsel: Morsel, worker_id: usize) -> Result<(), ExecutorError> {
        let result = self.child.process_morsel(morsel)?;
        let mut hash_table = self.hash_tables[worker_id].lock().unwrap();

        for tuple in result.tuples {
            let key = tuple.data.clone();
            let state = hash_table.entry(key.clone()).or_default();
            state.update(&key);
        }

        Ok(())
    }

    pub fn global_combine(&self) -> Result<Vec<SimpleTuple>, ExecutorError> {
        let mut global_map: HashMap<Vec<u8>, AggregateState> = HashMap::new();

        for hash_table in &self.hash_tables {
            let local_map = hash_table.lock().unwrap();
            for (key, state) in local_map.iter() {
                let global_state = global_map.entry(key.clone()).or_default();
                global_state.merge(state);
            }
        }

        let result = global_map
            .into_iter()
            .map(|(key, state)| {
                let mut data = key;
                data.extend_from_slice(&state.count.to_le_bytes());
                SimpleTuple { data }
            })
            .collect();

        Ok(result)
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
    fn test_aggregate_state() {
        let mut state = AggregateState::new();
        state.update(&[1, 2, 3]);
        state.update(&[4, 5, 6]);

        assert_eq!(state.count, 2);
        assert_eq!(state.sum, 5.0);
    }

    #[test]
    fn test_aggregate_merge() {
        let mut state1 = AggregateState::new();
        state1.update(&[1]);

        let mut state2 = AggregateState::new();
        state2.update(&[2]);

        state1.merge(&state2);
        assert_eq!(state1.count, 2);
    }

    #[test]
    fn test_parallel_hash_agg() {
        let tuples = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![2] },
        ];

        let child = Arc::new(MockOperator { tuples });
        let agg = ParallelHashAgg::new(child, 2);

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 3, partition_id: 0 };

        agg.local_aggregate(morsel, 0).unwrap();
        let result = agg.global_combine().unwrap();
        assert_eq!(result.len(), 2);
    }
}
