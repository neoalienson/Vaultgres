use super::old_executor::{OldExecutor as Executor, OldExecutorError as ExecutorError, Tuple};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

pub struct Aggregate {
    input: Box<dyn Executor>,
    agg_func: AggregateFunction,
    column: Option<String>,
    result: Option<i64>,
    count: u64,
    sum: i64,
    min: Option<i64>,
    max: Option<i64>,
    computed: bool,
    returned: bool,
}

impl Aggregate {
    pub fn new(
        input: Box<dyn Executor>,
        agg_func: AggregateFunction,
        column: Option<String>,
    ) -> Self {
        Self {
            input,
            agg_func,
            column,
            result: None,
            count: 0,
            sum: 0,
            min: None,
            max: None,
            computed: false,
            returned: false,
        }
    }

    fn compute(&mut self) -> Result<(), ExecutorError> {
        while let Some(tuple) = self.input.next()? {
            match &self.agg_func {
                AggregateFunction::Count => {
                    self.count += 1;
                }
                _ => {
                    if let Some(col) = &self.column {
                        if let Some(value_bytes) = tuple.get(col) {
                            let value = self.bytes_to_i64(value_bytes);

                            match &self.agg_func {
                                AggregateFunction::Sum | AggregateFunction::Avg => {
                                    self.sum += value;
                                    self.count += 1;
                                }
                                AggregateFunction::Min => {
                                    self.min = Some(match self.min {
                                        Some(current) => current.min(value),
                                        None => value,
                                    });
                                }
                                AggregateFunction::Max => {
                                    self.max = Some(match self.max {
                                        Some(current) => current.max(value),
                                        None => value,
                                    });
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }

        self.result = match &self.agg_func {
            AggregateFunction::Count => Some(self.count as i64),
            AggregateFunction::Sum => Some(self.sum),
            AggregateFunction::Avg => {
                if self.count > 0 {
                    Some(self.sum / self.count as i64)
                } else {
                    Some(0)
                }
            }
            AggregateFunction::Min => self.min,
            AggregateFunction::Max => self.max,
        };

        self.computed = true;
        Ok(())
    }

    fn bytes_to_i64(&self, bytes: &[u8]) -> i64 {
        if bytes.is_empty() {
            return 0;
        }
        if bytes.len() == 1 {
            return bytes[0] as i64;
        }
        // Simple conversion for testing
        bytes.iter().fold(0i64, |acc, &b| acc * 256 + b as i64)
    }

    fn i64_to_bytes(&self, value: i64) -> Vec<u8> {
        value.to_le_bytes().to_vec()
    }
}

impl Executor for Aggregate {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.input.open()?;
        self.computed = false;
        self.returned = false;
        self.count = 0;
        self.sum = 0;
        self.min = None;
        self.max = None;
        self.result = None;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if !self.computed {
            self.compute()?;
        }

        if self.returned {
            return Ok(None);
        }

        self.returned = true;

        if let Some(result) = self.result {
            let mut tuple = HashMap::new();
            let col_name = match &self.agg_func {
                AggregateFunction::Count => "count".to_string(),
                AggregateFunction::Sum => "sum".to_string(),
                AggregateFunction::Avg => "avg".to_string(),
                AggregateFunction::Min => "min".to_string(),
                AggregateFunction::Max => "max".to_string(),
            };
            tuple.insert(col_name, self.i64_to_bytes(result));
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
    use std::collections::HashMap;

    struct TestExecutor {
        tuples: Vec<HashMap<String, Vec<u8>>>,
        index: usize,
    }

    impl TestExecutor {
        fn new(tuples: Vec<HashMap<String, Vec<u8>>>) -> Self {
            Self { tuples, index: 0 }
        }
    }

    impl Executor for TestExecutor {
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

    fn make_tuple(value: u8) -> HashMap<String, Vec<u8>> {
        let mut map = HashMap::new();
        map.insert("value".to_string(), vec![value]);
        map
    }

    #[test]
    fn test_count() {
        let tuples = vec![make_tuple(1), make_tuple(2), make_tuple(3)];
        let input = TestExecutor::new(tuples);
        let mut agg = Aggregate::new(Box::new(input), AggregateFunction::Count, None);

        agg.open().unwrap();
        let result = agg.next().unwrap().unwrap();
        assert_eq!(result.get("count").unwrap()[0], 3);
        assert!(agg.next().unwrap().is_none());
        agg.close().unwrap();
    }

    #[test]
    fn test_sum() {
        let tuples = vec![make_tuple(10), make_tuple(20), make_tuple(30)];
        let input = TestExecutor::new(tuples);
        let mut agg =
            Aggregate::new(Box::new(input), AggregateFunction::Sum, Some("value".to_string()));

        agg.open().unwrap();
        let result = agg.next().unwrap().unwrap();
        assert_eq!(result.get("sum").unwrap()[0], 60);
        assert!(agg.next().unwrap().is_none());
        agg.close().unwrap();
    }

    #[test]
    fn test_avg() {
        let tuples = vec![make_tuple(10), make_tuple(20), make_tuple(30)];
        let input = TestExecutor::new(tuples);
        let mut agg =
            Aggregate::new(Box::new(input), AggregateFunction::Avg, Some("value".to_string()));

        agg.open().unwrap();
        let result = agg.next().unwrap().unwrap();
        assert_eq!(result.get("avg").unwrap()[0], 20);
        assert!(agg.next().unwrap().is_none());
        agg.close().unwrap();
    }

    #[test]
    fn test_min() {
        let tuples = vec![make_tuple(30), make_tuple(10), make_tuple(20)];
        let input = TestExecutor::new(tuples);
        let mut agg =
            Aggregate::new(Box::new(input), AggregateFunction::Min, Some("value".to_string()));

        agg.open().unwrap();
        let result = agg.next().unwrap().unwrap();
        assert_eq!(result.get("min").unwrap()[0], 10);
        assert!(agg.next().unwrap().is_none());
        agg.close().unwrap();
    }

    #[test]
    fn test_max() {
        let tuples = vec![make_tuple(30), make_tuple(10), make_tuple(20)];
        let input = TestExecutor::new(tuples);
        let mut agg =
            Aggregate::new(Box::new(input), AggregateFunction::Max, Some("value".to_string()));

        agg.open().unwrap();
        let result = agg.next().unwrap().unwrap();
        assert_eq!(result.get("max").unwrap()[0], 30);
        assert!(agg.next().unwrap().is_none());
        agg.close().unwrap();
    }

    #[test]
    fn test_count_empty() {
        let tuples = vec![];
        let input = TestExecutor::new(tuples);
        let mut agg = Aggregate::new(Box::new(input), AggregateFunction::Count, None);

        agg.open().unwrap();
        let result = agg.next().unwrap().unwrap();
        assert_eq!(result.get("count").unwrap()[0], 0);
        agg.close().unwrap();
    }

    #[test]
    fn test_min_empty() {
        let tuples = vec![];
        let input = TestExecutor::new(tuples);
        let mut agg =
            Aggregate::new(Box::new(input), AggregateFunction::Min, Some("value".to_string()));

        agg.open().unwrap();
        assert!(agg.next().unwrap().is_none());
        agg.close().unwrap();
    }
}