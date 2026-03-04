use super::old_executor::{
    OldExecutor as SimpleExecutor, OldExecutorError as ExecutorError, SimpleTuple as Tuple,
};
use std::collections::HashSet;

pub struct Union {
    left: Box<dyn SimpleExecutor>,
    right: Box<dyn SimpleExecutor>,
    all: bool,
    seen: HashSet<Vec<u8>>,
    left_done: bool,
}

impl Union {
    pub fn new(left: Box<dyn SimpleExecutor>, right: Box<dyn SimpleExecutor>, all: bool) -> Self {
        Self { left, right, all, seen: HashSet::new(), left_done: false }
    }
}

impl SimpleExecutor for Union {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.left.open()?;
        self.right.open()?;
        self.seen.clear();
        self.left_done = false;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if !self.left_done {
            while let Some(tuple) = self.left.next()? {
                if self.all || self.seen.insert(tuple.data.clone()) {
                    return Ok(Some(tuple));
                }
            }
            self.left_done = true;
        }

        while let Some(tuple) = self.right.next()? {
            if self.all || self.seen.insert(tuple.data.clone()) {
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
    fn test_union_basic() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![3] }, Tuple { data: vec![4] }]);
        let mut union = Union::new(Box::new(left), Box::new(right), false);
        union.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = union.next().unwrap() {
            results.push(tuple);
        }
        assert_eq!(results.len(), 4);
        union.close().unwrap();
    }

    #[test]
    fn test_union_removes_duplicates() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![2] }, Tuple { data: vec![3] }]);
        let mut union = Union::new(Box::new(left), Box::new(right), false);
        union.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = union.next().unwrap() {
            results.push(tuple);
        }
        assert_eq!(results.len(), 3);
        union.close().unwrap();
    }

    #[test]
    fn test_union_all_keeps_duplicates() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![2] }, Tuple { data: vec![3] }]);
        let mut union = Union::new(Box::new(left), Box::new(right), true);
        union.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = union.next().unwrap() {
            results.push(tuple);
        }
        assert_eq!(results.len(), 4);
        union.close().unwrap();
    }

    #[test]
    fn test_union_empty_left() {
        let left = OldMockExecutor::new(vec![]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let mut union = Union::new(Box::new(left), Box::new(right), false);
        union.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = union.next().unwrap() {
            results.push(tuple);
        }
        assert_eq!(results.len(), 2);
        union.close().unwrap();
    }

    #[test]
    fn test_union_empty_right() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let right = OldMockExecutor::new(vec![]);
        let mut union = Union::new(Box::new(left), Box::new(right), false);
        union.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = union.next().unwrap() {
            results.push(tuple);
        }
        assert_eq!(results.len(), 2);
        union.close().unwrap();
    }

    #[test]
    fn test_union_empty_both() {
        let left = OldMockExecutor::new(vec![]);
        let right = OldMockExecutor::new(vec![]);
        let mut union = Union::new(Box::new(left), Box::new(right), false);
        union.open().unwrap();
        assert!(union.next().unwrap().is_none());
        union.close().unwrap();
    }

    #[test]
    fn test_union_all_duplicates() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![1] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![1] }]);
        let mut union = Union::new(Box::new(left), Box::new(right), true);
        union.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = union.next().unwrap() {
            results.push(tuple);
        }
        assert_eq!(results.len(), 4);
        union.close().unwrap();
    }
}
