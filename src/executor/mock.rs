use super::{Executor, ExecutorError, SimpleExecutor, SimpleTuple, Tuple};

pub struct MockExecutor {
    tuples: Vec<SimpleTuple>,
    index: usize,
}

impl MockExecutor {
    pub fn new(tuples: Vec<SimpleTuple>) -> Self {
        Self { tuples, index: 0 }
    }
}

impl SimpleExecutor for MockExecutor {
    fn open(&mut self) -> Result<(), ExecutorError> {
        Ok(())
    }

    fn next(&mut self) -> Result<Option<SimpleTuple>, ExecutorError> {
        if self.index < self.tuples.len() {
            let tuple = self.tuples[self.index].clone();
            self.index += 1;
            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }

    fn close(&mut self) -> Result<(), ExecutorError> {
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
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.index = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if self.index < self.tuples.len() {
            let tuple = self.tuples[self.index].clone();
            self.index += 1;
            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }

    fn close(&mut self) -> Result<(), ExecutorError> {
        Ok(())
    }
}
