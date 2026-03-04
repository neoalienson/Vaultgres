use super::old_executor::{
    OldExecutor as SimpleExecutor, OldExecutorError as ExecutorError, SimpleTuple as Tuple,
};
use std::collections::HashSet;

pub struct Intersect {
    left: Box<dyn SimpleExecutor>,
    right: Box<dyn SimpleExecutor>,
    right_set: HashSet<Vec<u8>>,
    right_loaded: bool,
}

impl Intersect {
    pub fn new(left: Box<dyn SimpleExecutor>, right: Box<dyn SimpleExecutor>) -> Self {
        Self { left, right, right_set: HashSet::new(), right_loaded: false }
    }

    fn load_right(&mut self) -> Result<(), ExecutorError> {
        if !self.right_loaded {
            while let Some(tuple) = self.right.next()? {
                self.right_set.insert(tuple.data);
            }
            self.right_loaded = true;
        }
        Ok(())
    }
}

impl SimpleExecutor for Intersect {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.left.open()?;
        self.right.open()?;
        self.right_set.clear();
        self.right_loaded = false;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        self.load_right()?;

        while let Some(tuple) = self.left.next()? {
            if self.right_set.contains(&tuple.data) {
                return Ok(Some(tuple));
            }
        }

        Ok(None)
    }

    fn close(&mut self) -> Result<(), ExecutorError> {
        self.left.close()?;
        self.right.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::mock::MockExecutor;
    use crate::executor::old_executor::SimpleTuple;
    use crate::executor::test_helpers::OldMockExecutor;

    #[test]
    fn test_intersect_basic() {
        let left = OldMockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![2] },
            Tuple { data: vec![3] },
        ]);
        let right = OldMockExecutor::new(vec![
            Tuple { data: vec![2] },
            Tuple { data: vec![3] },
            Tuple { data: vec![4] },
        ]);
        let mut intersect = Intersect::new(Box::new(left), Box::new(right));
        intersect.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = intersect.next().unwrap() {
            results.push(tuple);
        }
        assert_eq!(results.len(), 2);
        intersect.close().unwrap();
    }

    #[test]
    fn test_intersect_no_overlap() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![3] }, Tuple { data: vec![4] }]);
        let mut intersect = Intersect::new(Box::new(left), Box::new(right));
        intersect.open().unwrap();
        assert!(intersect.next().unwrap().is_none());
        intersect.close().unwrap();
    }

    #[test]
    fn test_intersect_all_overlap() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let mut intersect = Intersect::new(Box::new(left), Box::new(right));
        intersect.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = intersect.next().unwrap() {
            results.push(tuple);
        }
        assert_eq!(results.len(), 2);
        intersect.close().unwrap();
    }

    #[test]
    fn test_intersect_empty_left() {
        let left = OldMockExecutor::new(vec![]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let mut intersect = Intersect::new(Box::new(left), Box::new(right));
        intersect.open().unwrap();
        assert!(intersect.next().unwrap().is_none());
        intersect.close().unwrap();
    }

    #[test]
    fn test_intersect_empty_right() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let right = OldMockExecutor::new(vec![]);
        let mut intersect = Intersect::new(Box::new(left), Box::new(right));
        intersect.open().unwrap();
        assert!(intersect.next().unwrap().is_none());
        intersect.close().unwrap();
    }

    #[test]
    fn test_intersect_empty_both() {
        let left = OldMockExecutor::new(vec![]);
        let right = OldMockExecutor::new(vec![]);
        let mut intersect = Intersect::new(Box::new(left), Box::new(right));
        intersect.open().unwrap();
        assert!(intersect.next().unwrap().is_none());
        intersect.close().unwrap();
    }
}
