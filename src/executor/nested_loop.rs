use super::old_executor::{OldExecutor as Executor, OldExecutorError as ExecutorError, Tuple};

pub struct NestedLoopJoin {
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    current_left: Option<Tuple>,
}

impl NestedLoopJoin {
    pub fn new(left: Box<dyn Executor>, right: Box<dyn Executor>) -> Self {
        Self { left, right, current_left: None }
    }
}

impl Executor for NestedLoopJoin {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.left.open()?;
        self.right.open()?;
        self.current_left = self.left.next()?;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        loop {
            if self.current_left.is_none() {
                return Ok(None);
            }

            match self.right.next()? {
                Some(right_tuple) => {
                    let mut result = self.current_left.as_ref().unwrap().clone();
                    result.extend(right_tuple);
                    return Ok(Some(result));
                }
                None => {
                    self.current_left = self.left.next()?;
                    if self.current_left.is_some() {
                        self.right.close()?;
                        self.right.open()?;
                    }
                }
            }
        }
    }

    fn close(&mut self) -> Result<(), ExecutorError> {
        self.left.close()?;
        self.right.close()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::executor::test_helpers::OldMockExecutor;
    use crate::executor::old_executor::SimpleTuple;
    use super::*;
    use std::collections::HashMap;

    struct MockExecutor {
        tuples: Vec<Tuple>,
        index: usize,
    }

    impl MockExecutor {
        fn new(tuples: Vec<Tuple>) -> Self {
            Self { tuples, index: 0 }
        }
    }

    impl Executor for MockExecutor {
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

    #[test]
    fn test_nested_loop_join() {
        let mut l1 = HashMap::new();
        l1.insert("left_id".to_string(), b"1".to_vec());
        let mut l2 = HashMap::new();
        l2.insert("left_id".to_string(), b"2".to_vec());

        let mut r1 = HashMap::new();
        r1.insert("right_id".to_string(), b"a".to_vec());
        let mut r2 = HashMap::new();
        r2.insert("right_id".to_string(), b"b".to_vec());

        let left = OldMockExecutor::new(vec![l1, l2]);
        let right = OldMockExecutor::new(vec![r1, r2]);

        let mut join = NestedLoopJoin::new(Box::new(left), Box::new(right));
        join.open().unwrap();

        let t1 = join.next().unwrap().unwrap();
        assert_eq!(t1.get("left_id").unwrap(), b"1");
        assert_eq!(t1.get("right_id").unwrap(), b"a");

        let t2 = join.next().unwrap().unwrap();
        assert_eq!(t2.get("left_id").unwrap(), b"1");
        assert_eq!(t2.get("right_id").unwrap(), b"b");

        let t3 = join.next().unwrap().unwrap();
        assert_eq!(t3.get("left_id").unwrap(), b"2");
        assert_eq!(t3.get("right_id").unwrap(), b"a");

        let t4 = join.next().unwrap().unwrap();
        assert_eq!(t4.get("left_id").unwrap(), b"2");
        assert_eq!(t4.get("right_id").unwrap(), b"b");

        assert!(join.next().unwrap().is_none());
    }
}
