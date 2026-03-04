use super::old_executor::{OldExecutor, OldExecutorError, SimpleTuple};
use super::operators::executor::{Executor, ExecutorError, Tuple};

pub struct MockExecutor {
    tuples: Vec<SimpleTuple>,
    index: usize,
}

impl MockExecutor {
    pub fn new(tuples: Vec<SimpleTuple>) -> Self {
        Self { tuples, index: 0 }
    }
}

impl OldExecutor for MockExecutor {
    fn open(&mut self) -> Result<(), OldExecutorError> {
        Ok(())
    }

    fn next(&mut self) -> Result<Option<SimpleTuple>, OldExecutorError> {
        if self.index < self.tuples.len() {
            let tuple = self.tuples[self.index].clone();
            self.index += 1;
            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }

    fn close(&mut self) -> Result<(), OldExecutorError> {
        Ok(())
    }
}

pub struct MockTupleExecutor {
    tuples: Vec<Tuple>,
    index: usize,
}

impl MockTupleExecutor {
    pub fn new(tuples: Vec<Tuple>) -> Self {
        Self { tuples, index: 0 }
    }
}

impl Executor for MockTupleExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if self.index < self.tuples.len() {
            let tuple = self.tuples[self.index].clone();
            self.index += 1;
            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }
}
