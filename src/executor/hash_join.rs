use super::old_executor::{
    OldExecutor as SimpleExecutor, OldExecutorError as ExecutorError, SimpleTuple as Tuple,
};
use std::collections::HashMap;

pub struct HashJoin {
    build_side: Box<dyn SimpleExecutor>,
    probe_side: Box<dyn SimpleExecutor>,
    hash_table: HashMap<i64, Vec<Tuple>>,
    built: bool,
    probe_buffer: Vec<Tuple>,
    probe_index: usize,
}

impl HashJoin {
    pub fn new(build_side: Box<dyn SimpleExecutor>, probe_side: Box<dyn SimpleExecutor>) -> Self {
        Self {
            build_side,
            probe_side,
            hash_table: HashMap::new(),
            built: false,
            probe_buffer: Vec::new(),
            probe_index: 0,
        }
    }

    fn build_hash_table(&mut self) -> Result<(), ExecutorError> {
        while let Some(tuple) = self.build_side.next()? {
            let key = self.extract_key(&tuple);
            self.hash_table.entry(key).or_default().push(tuple);
        }
        self.built = true;
        Ok(())
    }

    fn extract_key(&self, tuple: &Tuple) -> i64 {
        tuple.data.first().copied().unwrap_or(0) as i64
    }
}

impl SimpleExecutor for HashJoin {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.build_side.open()?;
        self.probe_side.open()?;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if !self.built {
            self.build_hash_table()?;
        }

        loop {
            if self.probe_index < self.probe_buffer.len() {
                let result = self.probe_buffer[self.probe_index].clone();
                self.probe_index += 1;
                return Ok(Some(result));
            }

            let probe_tuple = match self.probe_side.next()? {
                Some(t) => t,
                None => return Ok(None),
            };
            let key = self.extract_key(&probe_tuple);

            if let Some(build_tuples) = self.hash_table.get(&key) {
                self.probe_buffer = build_tuples
                    .iter()
                    .map(|build_tuple| {
                        let mut data = build_tuple.data.clone();
                        data.extend_from_slice(&probe_tuple.data);
                        Tuple { data }
                    })
                    .collect();
                self.probe_index = 0;
            }
        }
    }

    fn close(&mut self) -> Result<(), ExecutorError> {
        self.build_side.close()?;
        self.probe_side.close()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::mock::MockExecutor;
    use crate::executor::old_executor::SimpleTuple;
    use crate::executor::test_helpers::OldMockExecutor;

    #[test]
    fn test_hash_join_basic() {
        let build =
            OldMockExecutor::new(vec![Tuple { data: vec![1, 10] }, Tuple { data: vec![2, 20] }]);
        let probe =
            OldMockExecutor::new(vec![Tuple { data: vec![1, 100] }, Tuple { data: vec![2, 200] }]);

        let mut join = HashJoin::new(Box::new(build), Box::new(probe));
        join.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = join.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
        join.close().unwrap();
    }

    #[test]
    fn test_hash_join_no_match() {
        let build = OldMockExecutor::new(vec![Tuple { data: vec![1, 10] }]);
        let probe = OldMockExecutor::new(vec![Tuple { data: vec![2, 20] }]);

        let mut join = HashJoin::new(Box::new(build), Box::new(probe));
        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }
}
