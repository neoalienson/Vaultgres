use crate::executor::old_executor::{
    OldExecutor as SimpleExecutor, OldExecutorError as ExecutorError, SimpleTuple,
};

pub struct Case {
    input: Box<dyn SimpleExecutor>,
    evaluator: Box<dyn Fn(&SimpleTuple) -> Vec<u8> + Send>,
    results: Vec<SimpleTuple>,
    position: usize,
    executed: bool,
}

impl Case {
    pub fn new(
        input: Box<dyn SimpleExecutor>,
        evaluator: Box<dyn Fn(&SimpleTuple) -> Vec<u8> + Send>,
    ) -> Self {
        Self { input, evaluator, results: Vec::new(), position: 0, executed: false }
    }

    fn compute(&mut self) -> Result<(), ExecutorError> {
        self.input.open()?;
        while let Some(tuple) = self.input.next()? {
            let result = (self.evaluator)(&tuple);
            let mut data = tuple.data.clone();
            data.extend_from_slice(&result);
            self.results.push(SimpleTuple { data });
        }
        self.input.close()?;
        Ok(())
    }
}

impl SimpleExecutor for Case {
    fn open(&mut self) -> Result<(), ExecutorError> {
        if !self.executed {
            self.compute()?;
            self.executed = true;
        }
        self.position = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<SimpleTuple>, ExecutorError> {
        if self.position < self.results.len() {
            let tuple = self.results[self.position].clone();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::old_executor::SimpleTuple;
    use crate::executor::test_helpers::OldMockExecutor;

    struct MockExecutor {
        tuples: Vec<SimpleTuple>,
        position: usize,
    }

    impl MockExecutor {
        fn new(tuples: Vec<SimpleTuple>) -> Self {
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

    #[test]
    fn test_case_basic() {
        let tuples = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
        ];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let evaluator = Box::new(|t: &SimpleTuple| if t.data[0] > 1 { vec![10] } else { vec![20] });
        let mut case = Case::new(mock, evaluator);

        case.open().unwrap();
        let r1 = case.next().unwrap().unwrap();
        assert_eq!(r1.data[r1.data.len() - 1], 20);

        let r2 = case.next().unwrap().unwrap();
        assert_eq!(r2.data[r2.data.len() - 1], 10);

        let r3 = case.next().unwrap().unwrap();
        assert_eq!(r3.data[r3.data.len() - 1], 10);

        case.close().unwrap();
    }

    #[test]
    fn test_case_empty() {
        let mock = Box::new(OldMockExecutor::new(vec![]));
        let evaluator = Box::new(|_: &SimpleTuple| vec![0]);
        let mut case = Case::new(mock, evaluator);

        case.open().unwrap();
        assert!(case.next().unwrap().is_none());
        case.close().unwrap();
    }

    #[test]
    fn test_case_single() {
        let tuples = vec![SimpleTuple { data: vec![5] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let evaluator = Box::new(|t: &SimpleTuple| vec![t.data[0] * 2]);
        let mut case = Case::new(mock, evaluator);

        case.open().unwrap();
        let r = case.next().unwrap().unwrap();
        assert_eq!(r.data[r.data.len() - 1], 10);
        case.close().unwrap();
    }

    #[test]
    fn test_case_multiple_conditions() {
        let tuples = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![5] },
            SimpleTuple { data: vec![10] },
        ];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let evaluator = Box::new(|t: &SimpleTuple| {
            if t.data[0] < 5 {
                vec![1]
            } else if t.data[0] < 10 {
                vec![2]
            } else {
                vec![3]
            }
        });
        let mut case = Case::new(mock, evaluator);

        case.open().unwrap();
        assert_eq!(case.next().unwrap().unwrap().data.last(), Some(&1));
        assert_eq!(case.next().unwrap().unwrap().data.last(), Some(&2));
        assert_eq!(case.next().unwrap().unwrap().data.last(), Some(&3));
        case.close().unwrap();
    }

    #[test]
    fn test_case_reopen() {
        let tuples = vec![SimpleTuple { data: vec![1] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let evaluator = Box::new(|_: &SimpleTuple| vec![42]);
        let mut case = Case::new(mock, evaluator);

        case.open().unwrap();
        case.next().unwrap();
        case.close().unwrap();

        case.open().unwrap();
        let r = case.next().unwrap().unwrap();
        assert_eq!(r.data[r.data.len() - 1], 42);
        case.close().unwrap();
    }

    #[test]
    fn test_case_large_dataset() {
        let tuples: Vec<SimpleTuple> = (0..100).map(|i| SimpleTuple { data: vec![i] }).collect();
        let mock = Box::new(OldMockExecutor::new(tuples));
        let evaluator =
            Box::new(|t: &SimpleTuple| if t.data[0].is_multiple_of(2) { vec![0] } else { vec![1] });
        let mut case = Case::new(mock, evaluator);

        case.open().unwrap();
        let mut count = 0;
        while case.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 100);
        case.close().unwrap();
    }

    #[test]
    fn test_case_else_default() {
        let tuples = vec![SimpleTuple { data: vec![100] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let evaluator = Box::new(|t: &SimpleTuple| if t.data[0] < 50 { vec![1] } else { vec![0] });
        let mut case = Case::new(mock, evaluator);

        case.open().unwrap();
        let r = case.next().unwrap().unwrap();
        assert_eq!(r.data[r.data.len() - 1], 0);
        case.close().unwrap();
    }
}
