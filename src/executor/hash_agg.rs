use super::old_executor::{
    OldExecutor as SimpleExecutor, OldExecutorError as ExecutorError, SimpleTuple as Tuple,
};
use std::collections::HashMap;

pub struct HashAgg {
    input: Box<dyn SimpleExecutor>,
    groups: HashMap<i64, AggState>,
    results: Vec<Tuple>,
    index: usize,
    aggregated: bool,
}

#[derive(Clone)]
struct AggState {
    count: u64,
    sum: i64,
}

impl HashAgg {
    pub fn new(input: Box<dyn SimpleExecutor>) -> Self {
        Self { input, groups: HashMap::new(), results: Vec::new(), index: 0, aggregated: false }
    }

    fn aggregate(&mut self) -> Result<(), ExecutorError> {
        while let Some(tuple) = self.input.next()? {
            let key = tuple.data.first().copied().unwrap_or(0) as i64;
            let value = tuple.data.get(1).copied().unwrap_or(0) as i64;

            let state = self.groups.entry(key).or_insert(AggState { count: 0, sum: 0 });
            state.count += 1;
            state.sum += value;
        }

        for (key, state) in &self.groups {
            self.results.push(Tuple { data: vec![*key as u8, state.count as u8, state.sum as u8] });
        }

        self.aggregated = true;
        Ok(())
    }
}

impl SimpleExecutor for HashAgg {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.input.open()
    }

    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if !self.aggregated {
            self.aggregate()?;
        }

        if self.index < self.results.len() {
            let tuple = self.results[self.index].clone();
            self.index += 1;
            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }

    fn close(&mut self) -> Result<(), ExecutorError> {
        self.input.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::mock::MockExecutor;
    use crate::executor::old_executor::SimpleTuple;
    use crate::executor::test_helpers::OldMockExecutor;

    #[test]
    fn test_hash_agg_basic() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 10] },
            Tuple { data: vec![1, 20] },
            Tuple { data: vec![2, 30] },
        ]);
        let mut agg = HashAgg::new(Box::new(input));
        agg.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = agg.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
        agg.close().unwrap();
    }

    #[test]
    fn test_hash_agg_empty() {
        let input = OldMockExecutor::new(vec![]);
        let mut agg = HashAgg::new(Box::new(input));
        agg.open().unwrap();
        assert!(agg.next().unwrap().is_none());
        agg.close().unwrap();
    }
}
