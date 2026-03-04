use crate::executor::old_executor::{
    OldExecutor as SimpleExecutor, OldExecutorError as ExecutorError, SimpleTuple,
};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WindowFunction {
    RowNumber,
    Rank,
    DenseRank,
    Lag,
    Lead,
}

pub struct Window {
    input: Box<dyn SimpleExecutor>,
    function: WindowFunction,
    offset: usize,
    results: Vec<SimpleTuple>,
    position: usize,
    executed: bool,
}

impl Window {
    pub fn new(input: Box<dyn SimpleExecutor>, function: WindowFunction) -> Self {
        Self { input, function, offset: 1, results: Vec::new(), position: 0, executed: false }
    }

    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }

    fn compute_window(&mut self) -> Result<(), ExecutorError> {
        let mut tuples = Vec::new();
        self.input.open()?;
        while let Some(tuple) = self.input.next()? {
            tuples.push(tuple);
        }
        self.input.close()?;

        self.results = match self.function {
            WindowFunction::RowNumber | WindowFunction::Rank | WindowFunction::DenseRank => tuples
                .iter()
                .enumerate()
                .map(|(i, tuple)| {
                    let mut data = tuple.data.clone();
                    data.extend_from_slice(&(i as i64 + 1).to_le_bytes());
                    SimpleTuple { data }
                })
                .collect(),
            WindowFunction::Lag => tuples
                .iter()
                .enumerate()
                .map(|(i, tuple)| {
                    let mut data = tuple.data.clone();
                    if i >= self.offset {
                        data.extend_from_slice(&tuples[i - self.offset].data);
                    } else {
                        data.push(0);
                    }
                    SimpleTuple { data }
                })
                .collect(),
            WindowFunction::Lead => tuples
                .iter()
                .enumerate()
                .map(|(i, tuple)| {
                    let mut data = tuple.data.clone();
                    if i + self.offset < tuples.len() {
                        data.extend_from_slice(&tuples[i + self.offset].data);
                    } else {
                        data.push(0);
                    }
                    SimpleTuple { data }
                })
                .collect(),
        };

        Ok(())
    }
}

impl SimpleExecutor for Window {
    fn open(&mut self) -> Result<(), ExecutorError> {
        if !self.executed {
            self.compute_window()?;
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
    use crate::executor::test_helpers::OldMockExecutor;

    #[test]
    fn test_row_number() {
        let tuples = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
        ];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::RowNumber);

        window.open().unwrap();
        let r1 = window.next().unwrap().unwrap();
        assert_eq!(&r1.data[r1.data.len() - 8..], &1i64.to_le_bytes());

        let r2 = window.next().unwrap().unwrap();
        assert_eq!(&r2.data[r2.data.len() - 8..], &2i64.to_le_bytes());

        let r3 = window.next().unwrap().unwrap();
        assert_eq!(&r3.data[r3.data.len() - 8..], &3i64.to_le_bytes());

        assert!(window.next().unwrap().is_none());
        window.close().unwrap();
    }

    #[test]
    fn test_rank() {
        let tuples = vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::Rank);

        window.open().unwrap();
        let r1 = window.next().unwrap().unwrap();
        assert_eq!(&r1.data[r1.data.len() - 8..], &1i64.to_le_bytes());
        window.close().unwrap();
    }

    #[test]
    fn test_dense_rank() {
        let tuples = vec![SimpleTuple { data: vec![1] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::DenseRank);

        window.open().unwrap();
        let r1 = window.next().unwrap().unwrap();
        assert_eq!(&r1.data[r1.data.len() - 8..], &1i64.to_le_bytes());
        window.close().unwrap();
    }

    #[test]
    fn test_empty_input() {
        let mock = Box::new(OldMockExecutor::new(vec![]));
        let mut window = Window::new(mock, WindowFunction::RowNumber);

        window.open().unwrap();
        assert!(window.next().unwrap().is_none());
        window.close().unwrap();
    }

    #[test]
    fn test_single_row() {
        let tuples = vec![SimpleTuple { data: vec![42] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::RowNumber);

        window.open().unwrap();
        let r1 = window.next().unwrap().unwrap();
        assert_eq!(&r1.data[r1.data.len() - 8..], &1i64.to_le_bytes());
        assert!(window.next().unwrap().is_none());
        window.close().unwrap();
    }

    #[test]
    fn test_reopen() {
        let tuples = vec![SimpleTuple { data: vec![1] }];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::RowNumber);

        window.open().unwrap();
        window.next().unwrap();
        window.close().unwrap();

        window.open().unwrap();
        let r1 = window.next().unwrap().unwrap();
        assert_eq!(&r1.data[r1.data.len() - 8..], &1i64.to_le_bytes());
        window.close().unwrap();
    }

    #[test]
    fn test_large_dataset() {
        let tuples: Vec<SimpleTuple> = (0..100).map(|i| SimpleTuple { data: vec![i] }).collect();
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::RowNumber);

        window.open().unwrap();
        for i in 1..=100 {
            let r = window.next().unwrap().unwrap();
            assert_eq!(&r.data[r.data.len() - 8..], &(i as i64).to_le_bytes());
        }
        assert!(window.next().unwrap().is_none());
        window.close().unwrap();
    }

    #[test]
    fn test_lag_basic() {
        let tuples = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
        ];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::Lag);

        window.open().unwrap();
        let r1 = window.next().unwrap().unwrap();
        assert_eq!(r1.data[r1.data.len() - 1], 0);

        let r2 = window.next().unwrap().unwrap();
        assert_eq!(r2.data[r2.data.len() - 1], 1);

        let r3 = window.next().unwrap().unwrap();
        assert_eq!(r3.data[r3.data.len() - 1], 2);

        window.close().unwrap();
    }

    #[test]
    fn test_lead_basic() {
        let tuples = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
        ];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::Lead);

        window.open().unwrap();
        let r1 = window.next().unwrap().unwrap();
        assert_eq!(r1.data[r1.data.len() - 1], 2);

        let r2 = window.next().unwrap().unwrap();
        assert_eq!(r2.data[r2.data.len() - 1], 3);

        let r3 = window.next().unwrap().unwrap();
        assert_eq!(r3.data[r3.data.len() - 1], 0);

        window.close().unwrap();
    }

    #[test]
    fn test_lag_with_offset() {
        let tuples = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
            SimpleTuple { data: vec![4] },
        ];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::Lag).with_offset(2);

        window.open().unwrap();
        let r1 = window.next().unwrap().unwrap();
        assert_eq!(r1.data[r1.data.len() - 1], 0);

        let r2 = window.next().unwrap().unwrap();
        assert_eq!(r2.data[r2.data.len() - 1], 0);

        let r3 = window.next().unwrap().unwrap();
        assert_eq!(r3.data[r3.data.len() - 1], 1);

        let r4 = window.next().unwrap().unwrap();
        assert_eq!(r4.data[r4.data.len() - 1], 2);

        window.close().unwrap();
    }

    #[test]
    fn test_lead_with_offset() {
        let tuples = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
            SimpleTuple { data: vec![4] },
        ];
        let mock = Box::new(OldMockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::Lead).with_offset(2);

        window.open().unwrap();
        let r1 = window.next().unwrap().unwrap();
        assert_eq!(r1.data[r1.data.len() - 1], 3);

        let r2 = window.next().unwrap().unwrap();
        assert_eq!(r2.data[r2.data.len() - 1], 4);

        let r3 = window.next().unwrap().unwrap();
        assert_eq!(r3.data[r3.data.len() - 1], 0);

        let r4 = window.next().unwrap().unwrap();
        assert_eq!(r4.data[r4.data.len() - 1], 0);

        window.close().unwrap();
    }

    #[test]
    fn test_lag_empty() {
        let mock = Box::new(OldMockExecutor::new(vec![]));
        let mut window = Window::new(mock, WindowFunction::Lag);

        window.open().unwrap();
        assert!(window.next().unwrap().is_none());
        window.close().unwrap();
    }

    #[test]
    fn test_lead_empty() {
        let mock = Box::new(OldMockExecutor::new(vec![]));
        let mut window = Window::new(mock, WindowFunction::Lead);

        window.open().unwrap();
        assert!(window.next().unwrap().is_none());
        window.close().unwrap();
    }
}
