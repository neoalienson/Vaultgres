use super::old_executor::{
    OldExecutor as SimpleExecutor, OldExecutorError as ExecutorError, SimpleTuple as Tuple,
};

pub struct MergeJoin {
    left: Box<dyn SimpleExecutor>,
    right: Box<dyn SimpleExecutor>,
    left_buffer: Vec<Tuple>,
    right_buffer: Vec<Tuple>,
    left_idx: usize,
    right_idx: usize,
    result_buffer: Vec<Tuple>,
    result_idx: usize,
    initialized: bool,
}

impl MergeJoin {
    pub fn new(left: Box<dyn SimpleExecutor>, right: Box<dyn SimpleExecutor>) -> Self {
        Self {
            left,
            right,
            left_buffer: Vec::new(),
            right_buffer: Vec::new(),
            left_idx: 0,
            right_idx: 0,
            result_buffer: Vec::new(),
            result_idx: 0,
            initialized: false,
        }
    }

    fn load_and_sort(&mut self) -> Result<(), ExecutorError> {
        while let Some(tuple) = self.left.next()? {
            self.left_buffer.push(tuple);
        }
        while let Some(tuple) = self.right.next()? {
            self.right_buffer.push(tuple);
        }

        self.left_buffer.sort_by(|a, b| {
            let a_key = a.data.first().copied().unwrap_or(0);
            let b_key = b.data.first().copied().unwrap_or(0);
            a_key.cmp(&b_key)
        });

        self.right_buffer.sort_by(|a, b| {
            let a_key = a.data.first().copied().unwrap_or(0);
            let b_key = b.data.first().copied().unwrap_or(0);
            a_key.cmp(&b_key)
        });

        self.initialized = true;
        Ok(())
    }

    fn merge(&mut self) -> Result<(), ExecutorError> {
        while self.left_idx < self.left_buffer.len() && self.right_idx < self.right_buffer.len() {
            let left_key = self.left_buffer[self.left_idx].data.first().copied().unwrap_or(0);
            let right_key = self.right_buffer[self.right_idx].data.first().copied().unwrap_or(0);

            match left_key.cmp(&right_key) {
                std::cmp::Ordering::Less => {
                    self.left_idx += 1;
                }
                std::cmp::Ordering::Greater => {
                    self.right_idx += 1;
                }
                std::cmp::Ordering::Equal => {
                    let left_group_end =
                        self.find_group_end(&self.left_buffer, self.left_idx, left_key);
                    let right_group_end =
                        self.find_group_end(&self.right_buffer, self.right_idx, right_key);

                    for i in self.left_idx..left_group_end {
                        for j in self.right_idx..right_group_end {
                            let mut data = self.left_buffer[i].data.clone();
                            data.extend_from_slice(&self.right_buffer[j].data);
                            self.result_buffer.push(Tuple { data });
                        }
                    }

                    self.left_idx = left_group_end;
                    self.right_idx = right_group_end;
                }
            }
        }

        Ok(())
    }

    fn find_group_end(&self, buffer: &[Tuple], start: usize, key: u8) -> usize {
        let mut end = start;
        while end < buffer.len() && buffer[end].data.first().copied().unwrap_or(0) == key {
            end += 1;
        }
        end
    }
}

impl SimpleExecutor for MergeJoin {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.left.open()?;
        self.right.open()?;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if !self.initialized {
            self.load_and_sort()?;
            self.merge()?;
        }

        if self.result_idx < self.result_buffer.len() {
            let result = self.result_buffer[self.result_idx].clone();
            self.result_idx += 1;
            Ok(Some(result))
        } else {
            Ok(None)
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
    use super::*;
    use crate::executor::mock::MockExecutor;
    use crate::executor::old_executor::SimpleTuple;
    use crate::executor::test_helpers::OldMockExecutor;

    #[test]
    fn test_merge_join_basic() {
        let left =
            OldMockExecutor::new(vec![Tuple { data: vec![1, 10] }, Tuple { data: vec![2, 20] }]);
        let right =
            OldMockExecutor::new(vec![Tuple { data: vec![1, 100] }, Tuple { data: vec![2, 200] }]);

        let mut join = MergeJoin::new(Box::new(left), Box::new(right));
        join.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = join.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_no_matches() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1, 10] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![2, 20] }]);

        let mut join = MergeJoin::new(Box::new(left), Box::new(right));
        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_duplicates() {
        let left =
            OldMockExecutor::new(vec![Tuple { data: vec![1, 10] }, Tuple { data: vec![1, 11] }]);
        let right =
            OldMockExecutor::new(vec![Tuple { data: vec![1, 100] }, Tuple { data: vec![1, 101] }]);

        let mut join = MergeJoin::new(Box::new(left), Box::new(right));
        join.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = join.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 4);
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_empty_left() {
        let left = OldMockExecutor::new(vec![]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1, 100] }]);

        let mut join = MergeJoin::new(Box::new(left), Box::new(right));
        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_empty_right() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1, 10] }]);
        let right = OldMockExecutor::new(vec![]);

        let mut join = MergeJoin::new(Box::new(left), Box::new(right));
        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_unsorted_input() {
        let left =
            OldMockExecutor::new(vec![Tuple { data: vec![2, 20] }, Tuple { data: vec![1, 10] }]);
        let right =
            OldMockExecutor::new(vec![Tuple { data: vec![2, 200] }, Tuple { data: vec![1, 100] }]);

        let mut join = MergeJoin::new(Box::new(left), Box::new(right));
        join.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = join.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
        join.close().unwrap();
    }
}

#[cfg(test)]
mod edge_tests {
    use super::*;
    use crate::executor::mock::MockExecutor;
    use crate::executor::old_executor::SimpleTuple;
    use crate::executor::test_helpers::OldMockExecutor;

    #[test]
    fn test_merge_join_single_row_each() {
        let left = OldMockExecutor::new(vec![Tuple { data: vec![1, 10] }]);
        let right = OldMockExecutor::new(vec![Tuple { data: vec![1, 100] }]);

        let mut join = MergeJoin::new(Box::new(left), Box::new(right));
        join.open().unwrap();

        let result = join.next().unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().data.len(), 4);
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_large_groups() {
        let left = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 10] },
            Tuple { data: vec![1, 11] },
            Tuple { data: vec![1, 12] },
        ]);
        let right =
            OldMockExecutor::new(vec![Tuple { data: vec![1, 100] }, Tuple { data: vec![1, 101] }]);

        let mut join = MergeJoin::new(Box::new(left), Box::new(right));
        join.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = join.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 6);
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_all_left_smaller() {
        let left =
            OldMockExecutor::new(vec![Tuple { data: vec![1, 10] }, Tuple { data: vec![2, 20] }]);
        let right =
            OldMockExecutor::new(vec![Tuple { data: vec![3, 100] }, Tuple { data: vec![4, 200] }]);

        let mut join = MergeJoin::new(Box::new(left), Box::new(right));
        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_all_right_smaller() {
        let left =
            OldMockExecutor::new(vec![Tuple { data: vec![3, 10] }, Tuple { data: vec![4, 20] }]);
        let right =
            OldMockExecutor::new(vec![Tuple { data: vec![1, 100] }, Tuple { data: vec![2, 200] }]);

        let mut join = MergeJoin::new(Box::new(left), Box::new(right));
        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_interleaved() {
        let left = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 10] },
            Tuple { data: vec![3, 30] },
            Tuple { data: vec![5, 50] },
        ]);
        let right = OldMockExecutor::new(vec![
            Tuple { data: vec![2, 200] },
            Tuple { data: vec![3, 255] },
            Tuple { data: vec![4, 250] },
        ]);

        let mut join = MergeJoin::new(Box::new(left), Box::new(right));
        join.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = join.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 1);
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_many_duplicates() {
        let left = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 10] },
            Tuple { data: vec![1, 11] },
            Tuple { data: vec![1, 12] },
            Tuple { data: vec![1, 13] },
        ]);
        let right = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 100] },
            Tuple { data: vec![1, 101] },
            Tuple { data: vec![1, 102] },
        ]);

        let mut join = MergeJoin::new(Box::new(left), Box::new(right));
        join.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = join.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 12);
        join.close().unwrap();
    }
}
