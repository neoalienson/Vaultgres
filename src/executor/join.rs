use super::old_executor::{
    OldExecutor as SimpleExecutor, OldExecutorError as ExecutorError, SimpleTuple as Tuple,
};

pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

pub struct Join {
    left: Box<dyn SimpleExecutor>,
    right: Box<dyn SimpleExecutor>,
    join_type: JoinType,
    condition: Box<dyn Fn(&Tuple, &Tuple) -> bool + Send>,
    left_tuple: Option<Tuple>,
    right_tuples: Vec<Tuple>,
    right_index: usize,
    right_loaded: bool,
    found_match: bool,
    right_matched: Vec<bool>,
    emitting_unmatched_right: bool,
    unmatched_right_index: usize,
}

impl Join {
    pub fn new(
        left: Box<dyn SimpleExecutor>,
        right: Box<dyn SimpleExecutor>,
        join_type: JoinType,
        condition: Box<dyn Fn(&Tuple, &Tuple) -> bool + Send>,
    ) -> Self {
        Self {
            left,
            right,
            join_type,
            condition,
            left_tuple: None,
            right_tuples: Vec::new(),
            right_index: 0,
            right_loaded: false,
            found_match: false,
            right_matched: Vec::new(),
            emitting_unmatched_right: false,
            unmatched_right_index: 0,
        }
    }

    fn load_right(&mut self) -> Result<(), ExecutorError> {
        if !self.right_loaded {
            while let Some(tuple) = self.right.next()? {
                self.right_tuples.push(tuple);
            }
            self.right_matched = vec![false; self.right_tuples.len()];
            self.right_loaded = true;
        }
        Ok(())
    }

    fn merge_tuples(&self, left: &Tuple, right: &Tuple) -> Tuple {
        let mut data = left.data.clone();
        data.extend_from_slice(&right.data);
        Tuple { data }
    }
}

impl SimpleExecutor for Join {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.left.open()?;
        self.right.open()?;
        self.right_loaded = false;
        self.right_tuples.clear();
        self.right_matched.clear();
        self.right_index = 0;
        self.left_tuple = None;
        self.found_match = false;
        self.emitting_unmatched_right = false;
        self.unmatched_right_index = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        self.load_right()?;

        // For RIGHT and FULL joins, emit unmatched right tuples after processing all left tuples
        if self.emitting_unmatched_right {
            while self.unmatched_right_index < self.right_tuples.len() {
                let idx = self.unmatched_right_index;
                self.unmatched_right_index += 1;
                if !self.right_matched[idx] {
                    return Ok(Some(self.right_tuples[idx].clone()));
                }
            }
            return Ok(None);
        }

        loop {
            if self.left_tuple.is_none() {
                self.left_tuple = self.left.next()?;
                self.right_index = 0;
                self.found_match = false;

                if self.left_tuple.is_none() {
                    // For RIGHT and FULL joins, start emitting unmatched right tuples
                    if matches!(self.join_type, JoinType::Right | JoinType::Full) {
                        self.emitting_unmatched_right = true;
                        return self.next();
                    }
                    return Ok(None);
                }
            }

            let left = self.left_tuple.as_ref().unwrap();

            while self.right_index < self.right_tuples.len() {
                let right_idx = self.right_index;
                let right = &self.right_tuples[right_idx];
                self.right_index += 1;

                if (self.condition)(left, right) {
                    self.found_match = true;
                    self.right_matched[right_idx] = true;
                    return Ok(Some(self.merge_tuples(left, right)));
                }
            }

            if matches!(self.join_type, JoinType::Left | JoinType::Full) && !self.found_match {
                let result = left.clone();
                self.left_tuple = None;
                return Ok(Some(result));
            }

            self.left_tuple = None;
        }
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
    fn test_inner_join_basic() {
        let left =
            OldMockExecutor::new(vec![Tuple { data: vec![1, 10] }, Tuple { data: vec![2, 20] }]);
        let right =
            OldMockExecutor::new(vec![Tuple { data: vec![1, 100] }, Tuple { data: vec![3, 255] }]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Inner,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();

        let result = join.next().unwrap().unwrap();
        assert_eq!(result.data, vec![1, 10, 1, 100]);
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_inner_join_no_matches() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![2] }]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Inner,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_inner_join_multiple_matches() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![1] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![1] }]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Inner,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();

        let mut count = 0;
        while join.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 4);
        join.close().unwrap();
    }

    #[test]
    fn test_left_join_basic() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1] }]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Left,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();

        let r1 = join.next().unwrap().unwrap();
        assert_eq!(r1.data, vec![1, 1]);
        let r2 = join.next().unwrap().unwrap();
        assert_eq!(r2.data, vec![2]);
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_join_empty_left() {
        let left = OldMockExecutor::new(vec![]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1] }]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Inner,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_join_empty_right() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }]);
        let right = OldMockExecutor::new(vec![]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Inner,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_right_join_basic() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Right,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();

        let r1 = join.next().unwrap().unwrap();
        assert_eq!(r1.data, vec![1, 1]);
        let r2 = join.next().unwrap().unwrap();
        assert_eq!(r2.data, vec![2]);
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_full_join_basic() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![3] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1] }, Tuple { data: vec![2] }]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Full,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = join.next().unwrap() {
            results.push(tuple);
        }
        assert_eq!(results.len(), 3);
        join.close().unwrap();
    }
}
