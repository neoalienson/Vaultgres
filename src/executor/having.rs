use super::old_executor::{
    OldExecutor as SimpleExecutor, OldExecutorError as ExecutorError, SimpleTuple as Tuple,
};

pub struct Having {
    input: Box<dyn SimpleExecutor>,
    condition: Box<dyn Fn(&Tuple) -> bool + Send>,
}

impl Having {
    pub fn new(
        input: Box<dyn SimpleExecutor>,
        condition: Box<dyn Fn(&Tuple) -> bool + Send>,
    ) -> Self {
        Self { input, condition }
    }
}

impl SimpleExecutor for Having {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.input.open()
    }

    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        while let Some(tuple) = self.input.next()? {
            if (self.condition)(&tuple) {
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
    use crate::executor::old_executor::SimpleTuple;
    use crate::executor::test_helpers::OldMockExecutor;

    #[test]
    fn test_having_basic() {
        let input = OldMockExecutor::new(vec![
            SimpleTuple { data: vec![1, 30] },
            SimpleTuple { data: vec![2, 10] },
            SimpleTuple { data: vec![3, 50] },
        ]);
        let mut having =
            Having::new(Box::new(input), Box::new(|t| t.data.get(1).copied().unwrap_or(0) > 20));
        having.open().unwrap();

        let t1 = having.next().unwrap().unwrap();
        assert_eq!(t1.data[1], 30);
        let t2 = having.next().unwrap().unwrap();
        assert_eq!(t2.data[1], 50);
        assert!(having.next().unwrap().is_none());
        having.close().unwrap();
    }

    #[test]
    fn test_having_empty_input() {
        let input = OldMockExecutor::new(vec![]);
        let mut having = Having::new(Box::new(input), Box::new(|_| true));
        having.open().unwrap();
        assert!(having.next().unwrap().is_none());
        having.close().unwrap();
    }

    #[test]
    fn test_having_no_matches() {
        let input = OldMockExecutor::new(vec![
            SimpleTuple { data: vec![1, 5] },
            SimpleTuple { data: vec![2, 10] },
        ]);
        let mut having =
            Having::new(Box::new(input), Box::new(|t| t.data.get(1).copied().unwrap_or(0) > 100));
        having.open().unwrap();
        assert!(having.next().unwrap().is_none());
        having.close().unwrap();
    }

    #[test]
    fn test_having_all_match() {
        let input = OldMockExecutor::new(vec![
            SimpleTuple { data: vec![1, 50] },
            SimpleTuple { data: vec![2, 60] },
            SimpleTuple { data: vec![3, 70] },
        ]);
        let mut having =
            Having::new(Box::new(input), Box::new(|t| t.data.get(1).copied().unwrap_or(0) > 10));
        having.open().unwrap();

        let mut count = 0;
        while having.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
        having.close().unwrap();
    }

    #[test]
    fn test_having_single_row() {
        let input = OldMockExecutor::new(vec![SimpleTuple { data: vec![1, 100] }]);
        let mut having =
            Having::new(Box::new(input), Box::new(|t| t.data.get(1).copied().unwrap_or(0) >= 100));
        having.open().unwrap();

        let result = having.next().unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().data[1], 100);
        assert!(having.next().unwrap().is_none());
        having.close().unwrap();
    }

    #[test]
    fn test_having_equality() {
        let input = OldMockExecutor::new(vec![
            SimpleTuple { data: vec![1, 42] },
            SimpleTuple { data: vec![2, 42] },
            SimpleTuple { data: vec![3, 43] },
        ]);
        let mut having =
            Having::new(Box::new(input), Box::new(|t| t.data.get(1).copied().unwrap_or(0) == 42));
        having.open().unwrap();

        let mut count = 0;
        while having.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 2);
        having.close().unwrap();
    }

    #[test]
    fn test_having_complex_condition() {
        let input = OldMockExecutor::new(vec![
            SimpleTuple { data: vec![1, 10, 5] },
            SimpleTuple { data: vec![2, 20, 15] },
            SimpleTuple { data: vec![3, 30, 25] },
        ]);
        let mut having = Having::new(
            Box::new(input),
            Box::new(|t| {
                let sum = t.data.get(1).copied().unwrap_or(0) as u16
                    + t.data.get(2).copied().unwrap_or(0) as u16;
                sum > 30
            }),
        );
        having.open().unwrap();

        let mut count = 0;
        while having.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 2);
        having.close().unwrap();
    }
}
