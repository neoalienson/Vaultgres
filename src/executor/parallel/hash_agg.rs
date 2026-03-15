use crate::executor::operators::executor::{ExecutorError, Tuple};
use crate::executor::parallel::config::ParallelConfig;
use crate::executor::parallel::morsel::Morsel;
use crate::executor::parallel::operator::ParallelOperator;
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
        _config: &ParallelConfig,
        _row_count: usize,
    ) -> Result<Vec<Tuple>, ExecutorError> {
        // TODO: Implement proper parallel aggregation with Tuple (HashMap)
        // For now, return empty results
        Ok(Vec::new())
    }

    pub fn local_aggregate(&self, _morsel: Morsel, _worker_id: usize) -> Result<(), ExecutorError> {
        // TODO: Implement proper local aggregation with Tuple (HashMap)
        Ok(())
    }

    pub fn global_combine(&self) -> Result<Vec<Tuple>, ExecutorError> {
        // TODO: Implement proper global combine with Tuple (HashMap)
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockOperator {
        tuples: Vec<Tuple>,
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
        use crate::catalog::Value;
        use crate::executor::test_helpers::TupleBuilder;

        let tuples = vec![
            TupleBuilder::new().with_int("key", 1).build(),
            TupleBuilder::new().with_int("key", 1).build(),
            TupleBuilder::new().with_int("key", 2).build(),
        ];

        let child = Arc::new(MockOperator { tuples });
        let agg = ParallelHashAgg::new(child, 2);

        let morsel = Morsel { tuples: vec![], start_offset: 0, end_offset: 3, partition_id: 0 };

        agg.local_aggregate(morsel, 0).unwrap();
        let result = agg.global_combine().unwrap();
        // TODO: Verify results once proper aggregation is implemented
        assert_eq!(result.len(), 0); // Currently returns empty due to TODO implementation
    }
}
