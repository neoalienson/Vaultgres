use super::old_executor::{
    OldExecutor as SimpleExecutor, OldExecutorError as ExecutorError, SimpleTuple as Tuple,
};
use std::collections::HashSet;

pub struct Distinct {
    input: Box<dyn SimpleExecutor>,
    seen: HashSet<Vec<u8>>,
}

impl Distinct {
    pub fn new(input: Box<dyn SimpleExecutor>) -> Self {
        Self { input, seen: HashSet::new() }
    }
}

impl SimpleExecutor for Distinct {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.seen.clear();
        self.input.open()
    }

    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        while let Some(tuple) = self.input.next()? {
            if self.seen.insert(tuple.data.clone()) {
                return Ok(Some(tuple));
            }
        }
        Ok(None)
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
    fn test_distinct_basic() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 2] },
            Tuple { data: vec![1, 2] },
            Tuple { data: vec![3, 4] },
        ]);
        let mut distinct = Distinct::new(Box::new(input));
        distinct.open().unwrap();

        let t1 = distinct.next().unwrap().unwrap();
        assert_eq!(t1.data, vec![1, 2]);
        let t2 = distinct.next().unwrap().unwrap();
        assert_eq!(t2.data, vec![3, 4]);
        assert!(distinct.next().unwrap().is_none());
        distinct.close().unwrap();
    }

    #[test]
    fn test_distinct_empty_input() {
        let input = OldMockExecutor::new(vec![]);
        let mut distinct = Distinct::new(Box::new(input));
        distinct.open().unwrap();
        assert!(distinct.next().unwrap().is_none());
        distinct.close().unwrap();
    }

    #[test]
    fn test_distinct_all_unique() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![2] },
            Tuple { data: vec![3] },
        ]);
        let mut distinct = Distinct::new(Box::new(input));
        distinct.open().unwrap();

        let mut count = 0;
        while distinct.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
        distinct.close().unwrap();
    }

    #[test]
    fn test_distinct_all_duplicates() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 2] },
            Tuple { data: vec![1, 2] },
            Tuple { data: vec![1, 2] },
        ]);
        let mut distinct = Distinct::new(Box::new(input));
        distinct.open().unwrap();

        let result = distinct.next().unwrap().unwrap();
        assert_eq!(result.data, vec![1, 2]);
        assert!(distinct.next().unwrap().is_none());
        distinct.close().unwrap();
    }

    #[test]
    fn test_distinct_single_row() {
        let input = OldMockExecutor::new(vec![Tuple { data: vec![1, 2, 3] }]);
        let mut distinct = Distinct::new(Box::new(input));
        distinct.open().unwrap();

        let result = distinct.next().unwrap().unwrap();
        assert_eq!(result.data, vec![1, 2, 3]);
        assert!(distinct.next().unwrap().is_none());
        distinct.close().unwrap();
    }

    #[test]
    fn test_distinct_multiple_columns() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 2, 3] },
            Tuple { data: vec![1, 2, 4] },
            Tuple { data: vec![1, 2, 3] },
        ]);
        let mut distinct = Distinct::new(Box::new(input));
        distinct.open().unwrap();

        let mut count = 0;
        while distinct.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 2);
        distinct.close().unwrap();
    }

    #[test]
    fn test_distinct_preserves_order() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![3] },
            Tuple { data: vec![1] },
            Tuple { data: vec![3] },
            Tuple { data: vec![2] },
        ]);
        let mut distinct = Distinct::new(Box::new(input));
        distinct.open().unwrap();

        let t1 = distinct.next().unwrap().unwrap();
        assert_eq!(t1.data[0], 3);
        let t2 = distinct.next().unwrap().unwrap();
        assert_eq!(t2.data[0], 1);
        let t3 = distinct.next().unwrap().unwrap();
        assert_eq!(t3.data[0], 2);
        assert!(distinct.next().unwrap().is_none());
        distinct.close().unwrap();
    }
}
