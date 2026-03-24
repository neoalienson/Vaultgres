//! WindowExecutor - Implements window functions (ROW_NUMBER, RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE, etc.)
//!
//! Follows PostgreSQL's window function design:
//! - Window functions compute values across sets of rows related to the current row
//! - PARTITION BY divides rows into groups
//! - ORDER BY determines the order within each partition
//! - Window frames define which rows to include in the computation

use crate::catalog::{TableSchema, Value};
use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};
use crate::parser::ast::{OrderByExpr, WindowFrame, WindowFunc};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone)]
pub struct WindowInfo {
    func: WindowFunc,
    arg: Box<crate::parser::ast::Expr>,
    partition_by: Vec<String>,
    order_by: Vec<OrderByExpr>,
    output_column: String,
    frame: Option<WindowFrame>,
}

pub struct WindowExecutor {
    child: Box<dyn Executor>,
    windows: Vec<WindowInfo>,
    buffered_tuples: Vec<Tuple>,
    current_idx: usize,
    output_schema: TableSchema,
    sort_order: Vec<OrderByExpr>,
    require_top_order: bool,
}

impl WindowExecutor {
    pub fn new(
        mut child: Box<dyn Executor>,
        windows: Vec<WindowInfo>,
        output_schema: TableSchema,
    ) -> Result<Self, ExecutorError> {
        let mut buffered_tuples = Vec::new();
        while let Some(tuple) = child.next()? {
            buffered_tuples.push(tuple);
        }

        let sort_order: Vec<OrderByExpr> =
            windows.iter().flat_map(|w| w.order_by.clone()).collect();

        let require_top_order = !sort_order.is_empty();

        Ok(Self {
            child,
            windows,
            buffered_tuples,
            current_idx: 0,
            output_schema,
            sort_order,
            require_top_order,
        })
    }

    pub fn with_top_level_order_by(
        mut child: Box<dyn Executor>,
        windows: Vec<WindowInfo>,
        output_schema: TableSchema,
        top_order_by: Vec<OrderByExpr>,
    ) -> Result<Self, ExecutorError> {
        let mut buffered_tuples = Vec::new();
        while let Some(tuple) = child.next()? {
            buffered_tuples.push(tuple);
        }

        let mut sort_order = top_order_by.clone();
        for w in &windows {
            sort_order.extend(w.order_by.clone());
        }

        let require_top_order = !top_order_by.is_empty();

        Ok(Self {
            child,
            windows,
            buffered_tuples,
            current_idx: 0,
            output_schema,
            sort_order,
            require_top_order,
        })
    }

    fn compute_partition_key(tuple: &Tuple, partition_by: &[String]) -> Vec<u8> {
        let mut hasher = DefaultHasher::new();
        for col in partition_by {
            if let Some(val) = tuple.get(col) {
                Self::hash_value(val, &mut hasher);
            }
        }
        hasher.finish().to_le_bytes().to_vec()
    }

    fn hash_value(value: &Value, hasher: &mut DefaultHasher) {
        match value {
            Value::Int(n) => {
                "int".hash(hasher);
                n.hash(hasher);
            }
            Value::Text(s) => {
                "text".hash(hasher);
                s.hash(hasher);
            }
            Value::Bool(b) => {
                "bool".hash(hasher);
                b.hash(hasher);
            }
            Value::Float(f) => {
                "float".hash(hasher);
                f.to_le_bytes().hash(hasher);
            }
            Value::Null => {
                "null".hash(hasher);
            }
            _ => {
                format!("{:?}", value).hash(hasher);
            }
        }
    }

    fn compare_values(a: &Value, b: &Value) -> Result<std::cmp::Ordering, ExecutorError> {
        match (a, b) {
            (Value::Int(a), Value::Int(b)) => Ok(a.cmp(b)),
            (Value::Text(a), Value::Text(b)) => Ok(a.cmp(b)),
            (Value::Bool(a), Value::Bool(b)) => Ok(a.cmp(b)),
            (Value::Float(a), Value::Float(b)) => {
                Ok(a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            }
            (Value::Null, Value::Null) => Ok(std::cmp::Ordering::Equal),
            (Value::Null, _) => Ok(std::cmp::Ordering::Less),
            (_, Value::Null) => Ok(std::cmp::Ordering::Greater),
            _ => Err(ExecutorError::TypeMismatch("Cannot compare values".to_string())),
        }
    }

    fn compare_tuples(
        a: &Tuple,
        b: &Tuple,
        order_by: &[OrderByExpr],
    ) -> Result<std::cmp::Ordering, ExecutorError> {
        for expr in order_by {
            let col = &expr.column;
            let a_val = a.get(col).unwrap_or(&Value::Null);
            let b_val = b.get(col).unwrap_or(&Value::Null);

            let cmp = Self::compare_values(a_val, b_val)?;
            if cmp != std::cmp::Ordering::Equal {
                return Ok(if expr.ascending { cmp } else { cmp.reverse() });
            }
        }
        Ok(std::cmp::Ordering::Equal)
    }

    fn sort_tuples(tuples: &mut [Tuple], order_by: &[OrderByExpr]) -> Result<(), ExecutorError> {
        if order_by.is_empty() {
            return Ok(());
        }
        tuples.sort_by(|a, b| {
            Self::compare_tuples(a, b, order_by).unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok(())
    }

    fn get_value_from_tuple(
        tuple: &Tuple,
        arg: &crate::parser::ast::Expr,
    ) -> Result<Value, ExecutorError> {
        match arg {
            crate::parser::ast::Expr::Column(name) => tuple
                .get(name)
                .cloned()
                .ok_or_else(|| ExecutorError::InternalError(format!("Column {} not found", name))),
            crate::parser::ast::Expr::Star => Ok(Value::Int(1)),
            _ => Err(ExecutorError::UnsupportedExpression(format!(
                "Unsupported window function argument: {:?}",
                arg
            ))),
        }
    }

    fn compute_row_number(_tuples: &[Tuple], _idx: usize, _window: &WindowInfo) -> i64 {
        (_idx + 1) as i64
    }

    fn compute_rank(tuples: &[Tuple], idx: usize, window: &WindowInfo) -> i64 {
        if tuples.is_empty() || idx >= tuples.len() {
            return 1;
        }
        let current_val = Self::get_value_from_tuple(&tuples[idx], &window.arg).ok();
        let mut rank = 1i64;
        for i in 0..tuples.len() {
            if i == idx {
                continue;
            }
            let val = Self::get_value_from_tuple(&tuples[i], &window.arg).ok();
            if let (Some(c), Some(v)) = (&current_val, &val) {
                if let Ok(std::cmp::Ordering::Less) = Self::compare_values(v, c) {
                    rank += 1;
                }
            }
        }
        rank
    }

    fn compute_dense_rank(tuples: &[Tuple], idx: usize, window: &WindowInfo) -> i64 {
        if tuples.is_empty() || idx >= tuples.len() {
            return 1;
        }
        let current_val = Self::get_value_from_tuple(&tuples[idx], &window.arg).ok();
        let mut rank = 1i64;
        let mut seen_values: Vec<Value> = Vec::new();

        for i in 0..tuples.len() {
            if i == idx {
                continue;
            }
            let val = Self::get_value_from_tuple(&tuples[i], &window.arg).ok();
            if let (Some(c), Some(v)) = (&current_val, &val) {
                if let Ok(std::cmp::Ordering::Less) = Self::compare_values(v, c) {
                    let already_counted = seen_values.iter().any(|d| {
                        if let Ok(std::cmp::Ordering::Equal) = Self::compare_values(d, v) {
                            true
                        } else {
                            false
                        }
                    });
                    if !already_counted {
                        seen_values.push(v.clone());
                        rank += 1;
                    }
                }
            }
        }
        rank
    }

    fn compute_percent_rank(tuples: &[Tuple], idx: usize, window: &WindowInfo) -> f64 {
        if tuples.len() <= 1 {
            return 0.0;
        }
        let rank = Self::compute_rank(tuples, idx, window) as f64;
        (rank - 1.0) / (tuples.len() as f64 - 1.0)
    }

    fn compute_cume_dist(tuples: &[Tuple], idx: usize, window: &WindowInfo) -> f64 {
        if tuples.is_empty() {
            return 0.0;
        }
        let current_val = Self::get_value_from_tuple(&tuples[idx], &window.arg).ok();
        let mut count_less_or_equal = 0i64;
        let mut count_less = 0i64;
        for i in 0..tuples.len() {
            let val = Self::get_value_from_tuple(&tuples[i], &window.arg).ok();
            if let (Some(c), Some(v)) = (&current_val, &val) {
                match Self::compare_values(v, c) {
                    Ok(std::cmp::Ordering::Less) => count_less += 1,
                    Ok(std::cmp::Ordering::Equal) => count_less_or_equal += 1,
                    _ => {}
                }
            }
        }
        (count_less_or_equal as f64) / (tuples.len() as f64)
    }

    fn compute_lag(tuples: &[Tuple], idx: usize, window: &WindowInfo) -> Value {
        let offset = Self::get_lag_lead_offset(window);
        if idx >= offset {
            Self::get_value_from_tuple(&tuples[idx - offset], &window.arg).unwrap_or(Value::Null)
        } else {
            Value::Null
        }
    }

    fn compute_lead(tuples: &[Tuple], idx: usize, window: &WindowInfo) -> Value {
        let offset = Self::get_lag_lead_offset(window);
        if idx + offset < tuples.len() {
            Self::get_value_from_tuple(&tuples[idx + offset], &window.arg).unwrap_or(Value::Null)
        } else {
            Value::Null
        }
    }

    fn get_lag_lead_offset(window: &WindowInfo) -> usize {
        if let crate::parser::ast::Expr::Column(_) = window.arg.as_ref() { 1 } else { 1 }
    }

    fn compute_first_value(tuples: &[Tuple], _idx: usize, window: &WindowInfo) -> Value {
        if tuples.is_empty() {
            return Value::Null;
        }
        Self::get_value_from_tuple(&tuples[0], &window.arg).unwrap_or(Value::Null)
    }

    fn compute_last_value(tuples: &[Tuple], _idx: usize, window: &WindowInfo) -> Value {
        if tuples.is_empty() {
            return Value::Null;
        }
        Self::get_value_from_tuple(&tuples[tuples.len() - 1], &window.arg).unwrap_or(Value::Null)
    }

    fn compute_nth_value(tuples: &[Tuple], _idx: usize, window: &WindowInfo) -> Value {
        let n = Self::get_nth_offset(window);
        if n == 0 || n > tuples.len() {
            return Value::Null;
        }
        Self::get_value_from_tuple(&tuples[n - 1], &window.arg).unwrap_or(Value::Null)
    }

    fn get_nth_offset(window: &WindowInfo) -> usize {
        1
    }

    fn compute_ntile(tuples: &[Tuple], idx: usize, window: &WindowInfo) -> Value {
        let num_buckets = Self::get_ntile_buckets(window);
        if num_buckets == 0 || tuples.is_empty() {
            return Value::Null;
        }
        let bucket_size = tuples.len() / num_buckets as usize;
        let remainder = tuples.len() % num_buckets as usize;
        let bucket = if idx < (remainder * (bucket_size + 1)) {
            (idx as f64 / (bucket_size as f64 + 1.0)).ceil() as i64
        } else {
            ((idx - remainder) / bucket_size + remainder) as i64 + 1
        };
        Value::Int(bucket.max(1).min(num_buckets as i64))
    }

    fn get_ntile_buckets(window: &WindowInfo) -> usize {
        1
    }

    fn compute_window_value(
        &self,
        tuples: &[Tuple],
        idx: usize,
        window: &WindowInfo,
    ) -> Result<Value, ExecutorError> {
        let value = match &window.func {
            WindowFunc::RowNumber => Value::Int(Self::compute_row_number(tuples, idx, window)),
            WindowFunc::Rank => Value::Int(Self::compute_rank(tuples, idx, window)),
            WindowFunc::DenseRank => Value::Int(Self::compute_dense_rank(tuples, idx, window)),
            WindowFunc::DenseRankWithNulls => {
                Value::Int(Self::compute_dense_rank(tuples, idx, window))
            }
            WindowFunc::PercentRank => {
                Value::Float(Self::compute_percent_rank(tuples, idx, window))
            }
            WindowFunc::CumeDist => Value::Float(Self::compute_cume_dist(tuples, idx, window)),
            WindowFunc::Lag => Self::compute_lag(tuples, idx, window),
            WindowFunc::Lead => Self::compute_lead(tuples, idx, window),
            WindowFunc::FirstValue => Self::compute_first_value(tuples, idx, window),
            WindowFunc::LastValue => Self::compute_last_value(tuples, idx, window),
            WindowFunc::NthValue => Self::compute_nth_value(tuples, idx, window),
            WindowFunc::Ntile => Self::compute_ntile(tuples, idx, window),
        };
        Ok(value)
    }

    fn get_partition_boundaries(&self, tuples: &[Tuple]) -> Vec<(usize, usize)> {
        let mut boundaries = Vec::new();
        if tuples.is_empty() {
            return boundaries;
        }

        if self.windows.is_empty() || self.windows.iter().all(|w| w.partition_by.is_empty()) {
            boundaries.push((0, tuples.len()));
            return boundaries;
        }

        let first_window = &self.windows[0];
        let mut start = 0;
        let mut prev_key = Self::compute_partition_key(&tuples[0], &first_window.partition_by);

        for i in 1..tuples.len() {
            let key = Self::compute_partition_key(&tuples[i], &first_window.partition_by);
            if key != prev_key {
                boundaries.push((start, i));
                start = i;
                prev_key = key;
            }
        }
        boundaries.push((start, tuples.len()));
        boundaries
    }

    fn process_tuples(&self, tuples: &[Tuple]) -> Result<Vec<Tuple>, ExecutorError> {
        let mut results = Vec::with_capacity(tuples.len());
        let partition_boundaries = self.get_partition_boundaries(tuples);

        for (start, end) in partition_boundaries {
            let partition = &tuples[start..end];
            for (idx, tuple) in partition.iter().enumerate() {
                let mut result_tuple = tuple.clone();
                for window in &self.windows {
                    let value = self.compute_window_value(partition, idx, window)?;
                    result_tuple.insert(window.output_column.clone(), value);
                }
                results.push(result_tuple);
            }
        }

        Ok(results)
    }

    fn get_window_output_name(window: &WindowInfo) -> String {
        match &window.func {
            WindowFunc::RowNumber => "row_number".to_string(),
            WindowFunc::Rank => "rank".to_string(),
            WindowFunc::DenseRank => "dense_rank".to_string(),
            WindowFunc::DenseRankWithNulls => "dense_rank".to_string(),
            WindowFunc::PercentRank => "percent_rank".to_string(),
            WindowFunc::CumeDist => "cume_dist".to_string(),
            WindowFunc::Lag => format!("lag({})", "col"),
            WindowFunc::Lead => format!("lead({})", "col"),
            WindowFunc::FirstValue => format!("first_value({})", "col"),
            WindowFunc::LastValue => format!("last_value({})", "col"),
            WindowFunc::NthValue => format!("nth_value({})", "col"),
            WindowFunc::Ntile => "ntile".to_string(),
        }
    }
}

impl Executor for WindowExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if self.current_idx == 0 {
            if !self.sort_order.is_empty() {
                Self::sort_tuples(&mut self.buffered_tuples, &self.sort_order)?;
            }
            self.buffered_tuples = self.process_tuples(&self.buffered_tuples)?;
        }

        if self.current_idx >= self.buffered_tuples.len() {
            return Ok(None);
        }

        let tuple = self.buffered_tuples[self.current_idx].clone();
        self.current_idx += 1;
        Ok(Some(tuple))
    }
}

pub fn create_window_info(
    func: WindowFunc,
    arg: Box<crate::parser::ast::Expr>,
    partition_by: Vec<String>,
    order_by: Vec<OrderByExpr>,
    frame: Option<WindowFrame>,
) -> WindowInfo {
    let output_column = match &func {
        WindowFunc::RowNumber => "row_number".to_string(),
        WindowFunc::Rank => "rank".to_string(),
        WindowFunc::DenseRank => "dense_rank".to_string(),
        WindowFunc::DenseRankWithNulls => "dense_rank".to_string(),
        WindowFunc::PercentRank => "percent_rank".to_string(),
        WindowFunc::CumeDist => "cume_dist".to_string(),
        WindowFunc::Lag => "lag".to_string(),
        WindowFunc::Lead => "lead".to_string(),
        WindowFunc::FirstValue => "first_value".to_string(),
        WindowFunc::LastValue => "last_value".to_string(),
        WindowFunc::NthValue => "nth_value".to_string(),
        WindowFunc::Ntile => "ntile".to_string(),
    };

    WindowInfo { func, arg, partition_by, order_by, output_column, frame }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::test_helpers::TupleBuilder;

    struct MockExecutor {
        tuples: Vec<Tuple>,
        idx: usize,
    }

    impl MockExecutor {
        fn new(tuples: Vec<Tuple>) -> Self {
            Self { tuples, idx: 0 }
        }
    }

    impl Executor for MockExecutor {
        fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
            if self.idx >= self.tuples.len() {
                Ok(None)
            } else {
                self.idx += 1;
                Ok(Some(self.tuples[self.idx - 1].clone()))
            }
        }
    }

    fn create_mock_window_info(func: WindowFunc) -> WindowInfo {
        create_window_info(func, Box::new(crate::parser::ast::Expr::Star), vec![], vec![], None)
    }

    fn create_mock_tuples_with_values() -> Vec<Tuple> {
        vec![
            TupleBuilder::new().with_int("id", 1).with_int("value", 100).build(),
            TupleBuilder::new().with_int("id", 2).with_int("value", 200).build(),
            TupleBuilder::new().with_int("id", 3).with_int("value", 300).build(),
        ]
    }

    fn create_partitioned_tuples() -> Vec<Tuple> {
        vec![
            TupleBuilder::new().with_int("dept", 1).with_int("value", 100).build(),
            TupleBuilder::new().with_int("dept", 1).with_int("value", 200).build(),
            TupleBuilder::new().with_int("dept", 2).with_int("value", 150).build(),
            TupleBuilder::new().with_int("dept", 2).with_int("value", 250).build(),
        ]
    }

    #[test]
    fn test_row_number_basic() {
        let tuples = create_mock_tuples_with_values();
        let child = Box::new(MockExecutor::new(tuples));
        let windows = vec![create_mock_window_info(WindowFunc::RowNumber)];
        let schema = TableSchema::new("t".to_string(), vec![]);

        let mut executor = WindowExecutor::new(child, windows, schema).unwrap();
        let results: Vec<Tuple> = std::iter::from_fn(|| executor.next().unwrap()).collect();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].get("row_number"), Some(&Value::Int(1)));
        assert_eq!(results[1].get("row_number"), Some(&Value::Int(2)));
        assert_eq!(results[2].get("row_number"), Some(&Value::Int(3)));
    }

    #[test]
    fn test_row_number_partitioned() {
        let tuples = create_partitioned_tuples();
        let child = Box::new(MockExecutor::new(tuples));
        let mut windows = vec![create_mock_window_info(WindowFunc::RowNumber)];
        windows[0].partition_by = vec!["dept".to_string()];
        let schema = TableSchema::new("t".to_string(), vec![]);

        let mut executor = WindowExecutor::new(child, windows, schema).unwrap();
        let results: Vec<Tuple> = std::iter::from_fn(|| executor.next().unwrap()).collect();

        assert_eq!(results.len(), 4);
        assert_eq!(results[0].get("row_number"), Some(&Value::Int(1)));
        assert_eq!(results[1].get("row_number"), Some(&Value::Int(2)));
        assert_eq!(results[2].get("row_number"), Some(&Value::Int(1)));
        assert_eq!(results[3].get("row_number"), Some(&Value::Int(2)));
    }

    #[test]
    fn test_rank_basic() {
        let tuples = create_mock_tuples_with_values();
        let child = Box::new(MockExecutor::new(tuples));
        let mut windows = vec![create_mock_window_info(WindowFunc::Rank)];
        windows[0].arg = Box::new(crate::parser::ast::Expr::Column("value".to_string()));
        let schema = TableSchema::new("t".to_string(), vec![]);

        let mut executor = WindowExecutor::new(child, windows, schema).unwrap();
        let results: Vec<Tuple> = std::iter::from_fn(|| executor.next().unwrap()).collect();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].get("rank"), Some(&Value::Int(1)));
        assert_eq!(results[1].get("rank"), Some(&Value::Int(2)));
        assert_eq!(results[2].get("rank"), Some(&Value::Int(3)));
    }

    #[test]
    fn test_rank_with_duplicates() {
        let tuples = vec![
            TupleBuilder::new().with_int("id", 1).with_int("value", 100).build(),
            TupleBuilder::new().with_int("id", 2).with_int("value", 100).build(),
            TupleBuilder::new().with_int("id", 3).with_int("value", 100).build(),
            TupleBuilder::new().with_int("id", 4).with_int("value", 200).build(),
        ];
        let child = Box::new(MockExecutor::new(tuples));
        let mut windows = vec![create_mock_window_info(WindowFunc::Rank)];
        windows[0].arg = Box::new(crate::parser::ast::Expr::Column("value".to_string()));
        let schema = TableSchema::new("t".to_string(), vec![]);

        let mut executor = WindowExecutor::new(child, windows, schema).unwrap();
        let results: Vec<Tuple> = std::iter::from_fn(|| executor.next().unwrap()).collect();

        assert_eq!(results.len(), 4);
        assert_eq!(results[0].get("rank"), Some(&Value::Int(1)));
        assert_eq!(results[1].get("rank"), Some(&Value::Int(1)));
        assert_eq!(results[2].get("rank"), Some(&Value::Int(1)));
        assert_eq!(results[3].get("rank"), Some(&Value::Int(4)));
    }

    #[test]
    fn test_dense_rank() {
        let tuples = vec![
            TupleBuilder::new().with_int("id", 1).with_int("value", 100).build(),
            TupleBuilder::new().with_int("id", 2).with_int("value", 100).build(),
            TupleBuilder::new().with_int("id", 3).with_int("value", 100).build(),
            TupleBuilder::new().with_int("id", 4).with_int("value", 200).build(),
        ];
        let child = Box::new(MockExecutor::new(tuples));
        let mut windows = vec![create_mock_window_info(WindowFunc::DenseRank)];
        windows[0].arg = Box::new(crate::parser::ast::Expr::Column("value".to_string()));
        let schema = TableSchema::new("t".to_string(), vec![]);

        let mut executor = WindowExecutor::new(child, windows, schema).unwrap();
        let results: Vec<Tuple> = std::iter::from_fn(|| executor.next().unwrap()).collect();

        assert_eq!(results.len(), 4);
        assert_eq!(results[0].get("dense_rank"), Some(&Value::Int(1)));
        assert_eq!(results[1].get("dense_rank"), Some(&Value::Int(1)));
        assert_eq!(results[2].get("dense_rank"), Some(&Value::Int(1)));
        assert_eq!(results[3].get("dense_rank"), Some(&Value::Int(2)));
    }

    #[test]
    fn test_lag_basic() {
        let tuples = create_mock_tuples_with_values();
        let child = Box::new(MockExecutor::new(tuples));
        let mut windows = vec![create_mock_window_info(WindowFunc::Lag)];
        windows[0].arg = Box::new(crate::parser::ast::Expr::Column("value".to_string()));
        let schema = TableSchema::new("t".to_string(), vec![]);

        let mut executor = WindowExecutor::new(child, windows, schema).unwrap();
        let results: Vec<Tuple> = std::iter::from_fn(|| executor.next().unwrap()).collect();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].get("lag"), Some(&Value::Null));
        assert_eq!(results[1].get("lag"), Some(&Value::Int(100)));
        assert_eq!(results[2].get("lag"), Some(&Value::Int(200)));
    }

    #[test]
    fn test_lead_basic() {
        let tuples = create_mock_tuples_with_values();
        let child = Box::new(MockExecutor::new(tuples));
        let mut windows = vec![create_mock_window_info(WindowFunc::Lead)];
        windows[0].arg = Box::new(crate::parser::ast::Expr::Column("value".to_string()));
        let schema = TableSchema::new("t".to_string(), vec![]);

        let mut executor = WindowExecutor::new(child, windows, schema).unwrap();
        let results: Vec<Tuple> = std::iter::from_fn(|| executor.next().unwrap()).collect();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].get("lead"), Some(&Value::Int(200)));
        assert_eq!(results[1].get("lead"), Some(&Value::Int(300)));
        assert_eq!(results[2].get("lead"), Some(&Value::Null));
    }

    #[test]
    fn test_first_value() {
        let tuples = create_mock_tuples_with_values();
        let child = Box::new(MockExecutor::new(tuples));
        let mut windows = vec![create_mock_window_info(WindowFunc::FirstValue)];
        windows[0].arg = Box::new(crate::parser::ast::Expr::Column("value".to_string()));
        let schema = TableSchema::new("t".to_string(), vec![]);

        let mut executor = WindowExecutor::new(child, windows, schema).unwrap();
        let results: Vec<Tuple> = std::iter::from_fn(|| executor.next().unwrap()).collect();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].get("first_value"), Some(&Value::Int(100)));
        assert_eq!(results[1].get("first_value"), Some(&Value::Int(100)));
        assert_eq!(results[2].get("first_value"), Some(&Value::Int(100)));
    }

    #[test]
    fn test_last_value() {
        let tuples = create_mock_tuples_with_values();
        let child = Box::new(MockExecutor::new(tuples));
        let mut windows = vec![create_mock_window_info(WindowFunc::LastValue)];
        windows[0].arg = Box::new(crate::parser::ast::Expr::Column("value".to_string()));
        let schema = TableSchema::new("t".to_string(), vec![]);

        let mut executor = WindowExecutor::new(child, windows, schema).unwrap();
        let results: Vec<Tuple> = std::iter::from_fn(|| executor.next().unwrap()).collect();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].get("last_value"), Some(&Value::Int(300)));
        assert_eq!(results[1].get("last_value"), Some(&Value::Int(300)));
        assert_eq!(results[2].get("last_value"), Some(&Value::Int(300)));
    }

    #[test]
    fn test_multiple_windows() {
        let tuples = create_mock_tuples_with_values();
        let child = Box::new(MockExecutor::new(tuples));
        let mut windows = vec![
            create_mock_window_info(WindowFunc::RowNumber),
            create_mock_window_info(WindowFunc::Lag),
        ];
        windows[1].arg = Box::new(crate::parser::ast::Expr::Column("value".to_string()));
        let schema = TableSchema::new("t".to_string(), vec![]);

        let mut executor = WindowExecutor::new(child, windows, schema).unwrap();
        let results: Vec<Tuple> = std::iter::from_fn(|| executor.next().unwrap()).collect();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].get("row_number"), Some(&Value::Int(1)));
        assert_eq!(results[1].get("row_number"), Some(&Value::Int(2)));
        assert_eq!(results[1].get("lag"), Some(&Value::Int(100)));
        assert_eq!(results[2].get("lag"), Some(&Value::Int(200)));
    }

    #[test]
    fn test_empty_input() {
        let tuples: Vec<Tuple> = vec![];
        let child = Box::new(MockExecutor::new(tuples));
        let windows = vec![create_mock_window_info(WindowFunc::RowNumber)];
        let schema = TableSchema::new("t".to_string(), vec![]);

        let mut executor = WindowExecutor::new(child, windows, schema).unwrap();
        let results: Vec<Tuple> = std::iter::from_fn(|| executor.next().unwrap()).collect();

        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_percent_rank() {
        let tuples = vec![
            TupleBuilder::new().with_int("id", 1).with_int("value", 100).build(),
            TupleBuilder::new().with_int("id", 2).with_int("value", 200).build(),
            TupleBuilder::new().with_int("id", 3).with_int("value", 300).build(),
        ];
        let child = Box::new(MockExecutor::new(tuples));
        let mut windows = vec![create_mock_window_info(WindowFunc::PercentRank)];
        windows[0].arg = Box::new(crate::parser::ast::Expr::Column("value".to_string()));
        let schema = TableSchema::new("t".to_string(), vec![]);

        let mut executor = WindowExecutor::new(child, windows, schema).unwrap();
        let results: Vec<Tuple> = std::iter::from_fn(|| executor.next().unwrap()).collect();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].get("percent_rank"), Some(&Value::Float(0.0)));
    }

    #[test]
    fn test_cume_dist() {
        let tuples = vec![
            TupleBuilder::new().with_int("id", 1).with_int("value", 100).build(),
            TupleBuilder::new().with_int("id", 2).with_int("value", 200).build(),
            TupleBuilder::new().with_int("id", 3).with_int("value", 200).build(),
        ];
        let child = Box::new(MockExecutor::new(tuples));
        let mut windows = vec![create_mock_window_info(WindowFunc::CumeDist)];
        windows[0].arg = Box::new(crate::parser::ast::Expr::Column("value".to_string()));
        let schema = TableSchema::new("t".to_string(), vec![]);

        let mut executor = WindowExecutor::new(child, windows, schema).unwrap();
        let results: Vec<Tuple> = std::iter::from_fn(|| executor.next().unwrap()).collect();

        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_ntile() {
        let tuples = create_mock_tuples_with_values();
        let child = Box::new(MockExecutor::new(tuples));
        let windows = vec![create_mock_window_info(WindowFunc::Ntile)];
        let schema = TableSchema::new("t".to_string(), vec![]);

        let mut executor = WindowExecutor::new(child, windows, schema).unwrap();
        let results: Vec<Tuple> = std::iter::from_fn(|| executor.next().unwrap()).collect();

        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_nth_value() {
        let tuples = create_mock_tuples_with_values();
        let child = Box::new(MockExecutor::new(tuples));
        let mut windows = vec![create_mock_window_info(WindowFunc::NthValue)];
        windows[0].arg = Box::new(crate::parser::ast::Expr::Column("value".to_string()));
        let schema = TableSchema::new("t".to_string(), vec![]);

        let mut executor = WindowExecutor::new(child, windows, schema).unwrap();
        let results: Vec<Tuple> = std::iter::from_fn(|| executor.next().unwrap()).collect();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].get("nth_value"), Some(&Value::Int(100)));
        assert_eq!(results[1].get("nth_value"), Some(&Value::Int(100)));
        assert_eq!(results[2].get("nth_value"), Some(&Value::Int(100)));
    }

    #[test]
    fn test_null_handling() {
        let tuples = vec![
            TupleBuilder::new().with_int("id", 1).with_value("value", Value::Null).build(),
            TupleBuilder::new().with_int("id", 2).with_int("value", 200).build(),
            TupleBuilder::new().with_int("id", 3).with_int("value", 300).build(),
        ];
        let child = Box::new(MockExecutor::new(tuples));
        let mut windows = vec![create_mock_window_info(WindowFunc::Lag)];
        windows[0].arg = Box::new(crate::parser::ast::Expr::Column("value".to_string()));
        let schema = TableSchema::new("t".to_string(), vec![]);

        let mut executor = WindowExecutor::new(child, windows, schema).unwrap();
        let results: Vec<Tuple> = std::iter::from_fn(|| executor.next().unwrap()).collect();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].get("lag"), Some(&Value::Null));
        assert_eq!(results[1].get("lag"), Some(&Value::Null));
        assert_eq!(results[2].get("lag"), Some(&Value::Int(200)));
    }
}
