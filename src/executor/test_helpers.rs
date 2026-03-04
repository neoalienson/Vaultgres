use crate::executor::{ExecutorError, SimpleExecutor, SimpleTuple};

pub struct MockExecutor {
    pub tuples: Vec<SimpleTuple>,
    pub position: usize,
}

impl MockExecutor {
    pub fn new(tuples: Vec<SimpleTuple>) -> Self {
        Self { tuples, position: 0 }
    }
}

impl SimpleExecutor for MockExecutor {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.position = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<SimpleTuple>, ExecutorError> {
        if self.position < self.tuples.len() {
            let tuple = self.tuples[self.position].clone();
            self.position += 1;
            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }

    fn close(&mut self) -> Result<(), ExecutorError> {
        Ok(())
    }
}

pub fn count_results<E: SimpleExecutor>(executor: &mut E) -> Result<usize, ExecutorError> {
    let mut count = 0;
    while executor.next()?.is_some() {
        count += 1;
    }
    Ok(count)
}

pub fn run_executor<E: SimpleExecutor>(
    executor: &mut E,
) -> Result<Vec<SimpleTuple>, ExecutorError> {
    let mut results = Vec::new();
    executor.open()?;
    while let Some(tuple) = executor.next()? {
        results.push(tuple);
    }
    executor.close()?;
    Ok(results)
}

pub fn test_executor_lifecycle<E: SimpleExecutor>(
    executor: &mut E,
    expected_count: usize,
) -> Result<(), ExecutorError> {
    executor.open()?;
    let count = count_results(executor)?;
    assert_eq!(count, expected_count);
    executor.close()?;
    Ok(())
}
