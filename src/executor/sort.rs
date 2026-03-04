use super::old_executor::{
    OldExecutor as SimpleExecutor, OldExecutorError as ExecutorError, SimpleTuple as Tuple,
};

pub struct Sort {
    input: Box<dyn SimpleExecutor>,
    sorted_tuples: Vec<Tuple>,
    index: usize,
    sorted: bool,
}

impl Sort {
    pub fn new(input: Box<dyn SimpleExecutor>) -> Self {
        Self { input, sorted_tuples: Vec::new(), index: 0, sorted: false }
    }

    fn sort_tuples(&mut self) -> Result<(), ExecutorError> {
        while let Some(tuple) = self.input.next()? {
            self.sorted_tuples.push(tuple);
        }

        self.sorted_tuples.sort_by_key(|t| t.data.first().copied().unwrap_or(0));

        self.sorted = true;
        Ok(())
    }
}

impl SimpleExecutor for Sort {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.input.open()
    }

    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if !self.sorted {
            self.sort_tuples()?;
        }

        if self.index < self.sorted_tuples.len() {
            let tuple = self.sorted_tuples[self.index].clone();
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
    fn test_sort_basic() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![3] },
            Tuple { data: vec![1] },
            Tuple { data: vec![2] },
        ]);
        let mut sort = Sort::new(Box::new(input));
        sort.open().unwrap();

        let t1 = sort.next().unwrap().unwrap();
        assert_eq!(t1.data[0], 1);
        let t2 = sort.next().unwrap().unwrap();
        assert_eq!(t2.data[0], 2);
        let t3 = sort.next().unwrap().unwrap();
        assert_eq!(t3.data[0], 3);

        sort.close().unwrap();
    }

    #[test]
    fn test_sort_empty() {
        let input = OldMockExecutor::new(vec![]);
        let mut sort = Sort::new(Box::new(input));
        sort.open().unwrap();
        assert!(sort.next().unwrap().is_none());
        sort.close().unwrap();
    }
}
