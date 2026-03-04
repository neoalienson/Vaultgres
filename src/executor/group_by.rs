use super::old_executor::{
    OldExecutor as SimpleExecutor, OldExecutorError as ExecutorError, SimpleTuple as Tuple,
};
use std::collections::HashMap;

pub struct GroupBy {
    input: Box<dyn SimpleExecutor>,
    group_columns: Vec<usize>,
    agg_columns: Vec<usize>,
    groups: HashMap<Vec<u8>, Vec<u8>>,
    results: Vec<Tuple>,
    index: usize,
    grouped: bool,
}

impl GroupBy {
    pub fn new(
        input: Box<dyn SimpleExecutor>,
        group_columns: Vec<usize>,
        agg_columns: Vec<usize>,
    ) -> Self {
        Self {
            input,
            group_columns,
            agg_columns,
            groups: HashMap::new(),
            results: Vec::new(),
            index: 0,
            grouped: false,
        }
    }

    fn perform_grouping(&mut self) -> Result<(), ExecutorError> {
        while let Some(tuple) = self.input.next()? {
            let key: Vec<u8> =
                self.group_columns.iter().filter_map(|&i| tuple.data.get(i).copied()).collect();

            let agg_values: Vec<u8> =
                self.agg_columns.iter().filter_map(|&i| tuple.data.get(i).copied()).collect();

            self.groups
                .entry(key)
                .and_modify(|v| {
                    for (i, &val) in agg_values.iter().enumerate() {
                        if let Some(existing) = v.get_mut(i) {
                            *existing = existing.saturating_add(val);
                        }
                    }
                })
                .or_insert(agg_values);
        }

        for (key, agg) in &self.groups {
            let mut data = key.clone();
            data.extend_from_slice(agg);
            self.results.push(Tuple { data });
        }

        self.grouped = true;
        Ok(())
    }
}

impl SimpleExecutor for GroupBy {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.input.open()
    }

    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if !self.grouped {
            self.perform_grouping()?;
        }

        if self.index < self.results.len() {
            let tuple = self.results[self.index].clone();
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
    fn test_group_by_single_column() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 10] },
            Tuple { data: vec![1, 20] },
            Tuple { data: vec![2, 30] },
        ]);
        let mut group_by = GroupBy::new(Box::new(input), vec![0], vec![1]);
        group_by.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = group_by.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
        group_by.close().unwrap();
    }

    #[test]
    fn test_group_by_multiple_columns() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 1, 10] },
            Tuple { data: vec![1, 1, 20] },
            Tuple { data: vec![1, 2, 30] },
            Tuple { data: vec![2, 1, 40] },
        ]);
        let mut group_by = GroupBy::new(Box::new(input), vec![0, 1], vec![2]);
        group_by.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = group_by.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 3);
        group_by.close().unwrap();
    }

    #[test]
    fn test_group_by_empty_input() {
        let input = OldMockExecutor::new(vec![]);
        let mut group_by = GroupBy::new(Box::new(input), vec![0], vec![1]);
        group_by.open().unwrap();

        assert!(group_by.next().unwrap().is_none());
        group_by.close().unwrap();
    }

    #[test]
    fn test_group_by_single_group() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 10] },
            Tuple { data: vec![1, 20] },
            Tuple { data: vec![1, 30] },
        ]);
        let mut group_by = GroupBy::new(Box::new(input), vec![0], vec![1]);
        group_by.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = group_by.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].data[1], 60);
        group_by.close().unwrap();
    }

    #[test]
    fn test_group_by_all_different_groups() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 10] },
            Tuple { data: vec![2, 20] },
            Tuple { data: vec![3, 30] },
        ]);
        let mut group_by = GroupBy::new(Box::new(input), vec![0], vec![1]);
        group_by.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = group_by.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 3);
        group_by.close().unwrap();
    }

    #[test]
    fn test_group_by_reopen() {
        let input =
            OldMockExecutor::new(vec![Tuple { data: vec![1, 10] }, Tuple { data: vec![1, 20] }]);
        let mut group_by = GroupBy::new(Box::new(input), vec![0], vec![1]);
        group_by.open().unwrap();

        let first = group_by.next().unwrap();
        assert!(first.is_some());
        assert!(group_by.next().unwrap().is_none());

        group_by.close().unwrap();
    }

    #[test]
    fn test_group_by_multiple_aggregates() {
        let input = OldMockExecutor::new(vec![
            Tuple { data: vec![1, 10, 5] },
            Tuple { data: vec![1, 20, 3] },
            Tuple { data: vec![2, 30, 7] },
        ]);
        let mut group_by = GroupBy::new(Box::new(input), vec![0], vec![1, 2]);
        group_by.open().unwrap();

        let mut results = Vec::new();
        while let Some(tuple) = group_by.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
        group_by.close().unwrap();
    }
}
