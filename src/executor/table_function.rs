//! TableFunctionExecutor - Executes table-valued functions (TVFs)
//!
//! Table-valued functions return sets of rows and can be used in the FROM clause:
//! - SELECT * FROM generate_series(1, 10)
//! - SELECT * FROM unnest(ARRAY[1,2,3])
//! - User-defined functions with RETURNS SETOF or RETURNS TABLE

use crate::catalog::Value;
use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};
use crate::parser::ast::FunctionReturnType;

/// TableFunctionExecutor - Executes table-valued functions
///
/// A table-valued function (TVF) returns multiple rows and can be used
/// anywhere a table reference is valid (FROM clause, JOINs, etc.)
pub struct TableFunctionExecutor {
    /// The function name
    name: String,
    /// Arguments to the function
    args: Vec<Value>,
    /// The return type of the function
    return_type: FunctionReturnType,
    /// Schema of the returned rows (column names and types)
    output_schema: Vec<(String, String)>,
    /// Internal state for stateful functions
    state: TableFunctionState,
    /// Whether the function has been initialized
    initialized: bool,
}

/// Internal state for table function execution
enum TableFunctionState {
    /// Not initialized yet
    Uninitialized,
    /// For generate_series - current value, end value, and step
    GenerateSeries { current: i64, end: i64, step: i64 },
    /// For generate_series with start/stop/step
    GenerateSeriesStepped { current: i64, end: i64, step: i64, first: bool },
    /// For unnest - array to expand and current index
    Unnest { values: Vec<Value>, index: usize },
    /// For table functions that return a fixed set of rows
    FixedRows { rows: Vec<Tuple>, index: usize },
    /// For SQL functions with RETURNING clause - executor for the query
    SqlFunction { rows: Vec<Tuple>, index: usize },
    /// Function has completed
    Exhausted,
}

impl TableFunctionExecutor {
    /// Create a new TableFunctionExecutor for a named function with pre-evaluated args
    pub fn new(
        name: String,
        args: Vec<Value>,
        return_type: FunctionReturnType,
        output_schema: Vec<(String, String)>,
    ) -> Result<Self, ExecutorError> {
        let state = if matches!(return_type, FunctionReturnType::Setof(_)) {
            match name.as_str() {
                "generate_series" => Self::init_generate_series(&args)?,
                "unnest" => Self::init_unnest(&args)?,
                _ => TableFunctionState::Uninitialized,
            }
        } else {
            TableFunctionState::Uninitialized
        };

        Ok(Self { name, args, return_type, output_schema, state, initialized: true })
    }

    /// Initialize generate_series state
    fn init_generate_series(args: &[Value]) -> Result<TableFunctionState, ExecutorError> {
        match args.len() {
            2 => {
                let start = match &args[0] {
                    Value::Int(n) => *n,
                    _ => {
                        return Err(ExecutorError::TypeMismatch(
                            "generate_series requires integer start".to_string(),
                        ));
                    }
                };
                let end = match &args[1] {
                    Value::Int(n) => *n,
                    _ => {
                        return Err(ExecutorError::TypeMismatch(
                            "generate_series requires integer end".to_string(),
                        ));
                    }
                };
                Ok(TableFunctionState::GenerateSeries { current: start, end, step: 1 })
            }
            3 => {
                let start = match &args[0] {
                    Value::Int(n) => *n,
                    _ => {
                        return Err(ExecutorError::TypeMismatch(
                            "generate_series requires integer start".to_string(),
                        ));
                    }
                };
                let end = match &args[1] {
                    Value::Int(n) => *n,
                    _ => {
                        return Err(ExecutorError::TypeMismatch(
                            "generate_series requires integer end".to_string(),
                        ));
                    }
                };
                let step = match &args[2] {
                    Value::Int(n) => *n,
                    _ => {
                        return Err(ExecutorError::TypeMismatch(
                            "generate_series requires integer step".to_string(),
                        ));
                    }
                };
                if step == 0 {
                    return Err(ExecutorError::InvalidInput(
                        "generate_series step cannot be zero".to_string(),
                    ));
                }
                Ok(TableFunctionState::GenerateSeriesStepped {
                    current: start,
                    end,
                    step,
                    first: true,
                })
            }
            _ => Err(ExecutorError::InvalidInput(
                "generate_series requires 2 or 3 arguments".to_string(),
            )),
        }
    }

    /// Initialize unnest state
    fn init_unnest(args: &[Value]) -> Result<TableFunctionState, ExecutorError> {
        if args.is_empty() {
            return Err(ExecutorError::InvalidInput(
                "unnest requires an array argument".to_string(),
            ));
        }
        match &args[0] {
            Value::Array(arr) => Ok(TableFunctionState::Unnest { values: arr.clone(), index: 0 }),
            _ => Err(ExecutorError::TypeMismatch("unnest requires an array argument".to_string())),
        }
    }

    /// Create a TableFunctionExecutor for generate_series (simple form)
    pub fn generate_series(start: i64, end: i64) -> Result<Self, ExecutorError> {
        Self::new(
            "generate_series".to_string(),
            vec![Value::Int(start), Value::Int(end)],
            FunctionReturnType::Setof("INTEGER".to_string()),
            vec![("generate_series".to_string(), "INTEGER".to_string())],
        )
    }

    /// Create a TableFunctionExecutor for generate_series with step
    pub fn generate_series_stepped(start: i64, end: i64, step: i64) -> Result<Self, ExecutorError> {
        Self::new(
            "generate_series".to_string(),
            vec![Value::Int(start), Value::Int(end), Value::Int(step)],
            FunctionReturnType::Setof("INTEGER".to_string()),
            vec![("generate_series".to_string(), "INTEGER".to_string())],
        )
    }

    /// Create a TableFunctionExecutor for unnest
    pub fn unnest(values: Vec<Value>) -> Result<Self, ExecutorError> {
        Self::new(
            "unnest".to_string(),
            vec![Value::Array(values)],
            FunctionReturnType::Setof("ANY".to_string()),
            vec![("unnest".to_string(), "ANY".to_string())],
        )
    }

    /// Get the output schema (column names and types)
    pub fn output_schema(&self) -> &[(String, String)] {
        &self.output_schema
    }

    /// Check if the function has completed
    pub fn is_exhausted(&self) -> bool {
        matches!(self.state, TableFunctionState::Exhausted)
    }

    fn make_tuple(value: Value) -> Tuple {
        let mut tuple = Tuple::new();
        tuple.insert("".to_string(), value);
        tuple
    }
}

impl Executor for TableFunctionExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if !self.initialized {
            self.state = TableFunctionState::Exhausted;
            return Ok(None);
        }

        // Take the current state out so we can process it
        let current_state = std::mem::replace(&mut self.state, TableFunctionState::Exhausted);

        // Process the current state and determine next state + result
        let (next_state, result) = match current_state {
            TableFunctionState::Uninitialized => (TableFunctionState::Exhausted, Ok(None)),

            TableFunctionState::GenerateSeries { current, end, step } => {
                if current > end {
                    (TableFunctionState::Exhausted, Ok(None))
                } else {
                    let value = Value::Int(current);
                    let new_current = current + 1;
                    if new_current > end {
                        (TableFunctionState::Exhausted, Ok(Some(Self::make_tuple(value))))
                    } else {
                        (
                            TableFunctionState::GenerateSeries { current: new_current, end, step },
                            Ok(Some(Self::make_tuple(value))),
                        )
                    }
                }
            }

            TableFunctionState::GenerateSeriesStepped { current, end, step, first } => {
                if first {
                    if (step > 0 && current > end) || (step < 0 && current < end) {
                        (TableFunctionState::Exhausted, Ok(None))
                    } else {
                        let value = Value::Int(current);
                        let next_val = current + step;
                        if (step > 0 && next_val > end) || (step < 0 && next_val < end) {
                            (TableFunctionState::Exhausted, Ok(Some(Self::make_tuple(value))))
                        } else {
                            (
                                TableFunctionState::GenerateSeriesStepped {
                                    current,
                                    end,
                                    step,
                                    first: false,
                                },
                                Ok(Some(Self::make_tuple(value))),
                            )
                        }
                    }
                } else {
                    let next_val = current + step;
                    if (step > 0 && next_val > end) || (step < 0 && next_val < end) {
                        (TableFunctionState::Exhausted, Ok(None))
                    } else {
                        let value = Value::Int(next_val);
                        (
                            TableFunctionState::GenerateSeriesStepped {
                                current: next_val,
                                end,
                                step,
                                first: false,
                            },
                            Ok(Some(Self::make_tuple(value))),
                        )
                    }
                }
            }

            TableFunctionState::Unnest { values, index } => {
                if index >= values.len() {
                    (TableFunctionState::Exhausted, Ok(None))
                } else {
                    let value = values[index].clone();
                    (
                        TableFunctionState::Unnest { values, index: index + 1 },
                        Ok(Some(Self::make_tuple(value))),
                    )
                }
            }

            TableFunctionState::FixedRows { rows, index } => {
                if index >= rows.len() {
                    (TableFunctionState::Exhausted, Ok(None))
                } else {
                    let row = rows[index].clone();
                    (TableFunctionState::FixedRows { rows, index: index + 1 }, Ok(Some(row)))
                }
            }

            TableFunctionState::SqlFunction { rows, index } => {
                if index >= rows.len() {
                    (TableFunctionState::Exhausted, Ok(None))
                } else {
                    let row = rows[index].clone();
                    (TableFunctionState::SqlFunction { rows, index: index + 1 }, Ok(Some(row)))
                }
            }

            TableFunctionState::Exhausted => (TableFunctionState::Exhausted, Ok(None)),
        };

        // Store the next state
        self.state = next_state;

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_series_basic() {
        let mut exec = TableFunctionExecutor::generate_series(1, 5).unwrap();

        let mut values = Vec::new();
        while let Some(tuple) = exec.next().unwrap() {
            values.push(tuple.get("").unwrap().clone());
        }

        assert_eq!(
            values,
            vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(4), Value::Int(5),]
        );
    }

    #[test]
    fn test_generate_series_empty() {
        let mut exec = TableFunctionExecutor::generate_series(5, 1).unwrap();

        let result = exec.next().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_generate_series_single() {
        let mut exec = TableFunctionExecutor::generate_series(42, 42).unwrap();

        let result = exec.next().unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().get("").unwrap(), &Value::Int(42));

        assert!(exec.next().unwrap().is_none());
    }

    #[test]
    fn test_generate_series_negative() {
        let mut exec = TableFunctionExecutor::generate_series(-2, 2).unwrap();

        let mut values = Vec::new();
        while let Some(tuple) = exec.next().unwrap() {
            values.push(tuple.get("").unwrap().clone());
        }

        assert_eq!(
            values,
            vec![Value::Int(-2), Value::Int(-1), Value::Int(0), Value::Int(1), Value::Int(2),]
        );
    }

    #[test]
    fn test_generate_series_stepped() {
        let mut exec = TableFunctionExecutor::generate_series_stepped(0, 10, 2).unwrap();

        let mut values = Vec::new();
        while let Some(tuple) = exec.next().unwrap() {
            values.push(tuple.get("").unwrap().clone());
        }

        assert_eq!(
            values,
            vec![
                Value::Int(0),
                Value::Int(2),
                Value::Int(4),
                Value::Int(6),
                Value::Int(8),
                Value::Int(10),
            ]
        );
    }

    #[test]
    fn test_generate_series_negative_step() {
        let mut exec = TableFunctionExecutor::generate_series_stepped(10, 0, -2).unwrap();

        let mut values = Vec::new();
        while let Some(tuple) = exec.next().unwrap() {
            values.push(tuple.get("").unwrap().clone());
        }

        assert_eq!(
            values,
            vec![
                Value::Int(10),
                Value::Int(8),
                Value::Int(6),
                Value::Int(4),
                Value::Int(2),
                Value::Int(0),
            ]
        );
    }

    #[test]
    fn test_generate_series_zero_step_error() {
        let result = TableFunctionExecutor::generate_series_stepped(1, 10, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_unnest_basic() {
        let arr = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let mut exec = TableFunctionExecutor::unnest(arr).unwrap();

        let mut values = Vec::new();
        while let Some(tuple) = exec.next().unwrap() {
            values.push(tuple.get("").unwrap().clone());
        }

        assert_eq!(values, vec![Value::Int(1), Value::Int(2), Value::Int(3),]);
    }

    #[test]
    fn test_unnest_empty() {
        let arr = vec![];
        let mut exec = TableFunctionExecutor::unnest(arr).unwrap();

        let result = exec.next().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_unnest_strings() {
        let arr = vec![Value::Text("apple".to_string()), Value::Text("banana".to_string())];
        let mut exec = TableFunctionExecutor::unnest(arr).unwrap();

        let mut values = Vec::new();
        while let Some(tuple) = exec.next().unwrap() {
            values.push(tuple.get("").unwrap().clone());
        }

        assert_eq!(
            values,
            vec![Value::Text("apple".to_string()), Value::Text("banana".to_string()),]
        );
    }

    #[test]
    fn test_unnest_mixed_types() {
        let arr = vec![Value::Int(1), Value::Text("two".to_string()), Value::Bool(true)];
        let mut exec = TableFunctionExecutor::unnest(arr).unwrap();

        let mut values = Vec::new();
        while let Some(tuple) = exec.next().unwrap() {
            values.push(tuple.get("").unwrap().clone());
        }

        assert_eq!(values, vec![Value::Int(1), Value::Text("two".to_string()), Value::Bool(true),]);
    }

    #[test]
    fn test_unnest_with_nulls() {
        let arr = vec![Value::Int(1), Value::Null, Value::Int(3)];
        let mut exec = TableFunctionExecutor::unnest(arr).unwrap();

        let mut values = Vec::new();
        while let Some(tuple) = exec.next().unwrap() {
            values.push(tuple.get("").unwrap().clone());
        }

        assert_eq!(values, vec![Value::Int(1), Value::Null, Value::Int(3),]);
    }

    #[test]
    fn test_generate_series_large_range() {
        let mut exec = TableFunctionExecutor::generate_series(1, 1000).unwrap();

        let mut count = 0;
        while let Some(_) = exec.next().unwrap() {
            count += 1;
        }

        assert_eq!(count, 1000);
    }

    #[test]
    fn test_generate_series_with_zero_step_in_args() {
        let result = TableFunctionExecutor::new(
            "generate_series".to_string(),
            vec![Value::Int(1), Value::Int(10), Value::Int(0)],
            FunctionReturnType::Setof("INTEGER".to_string()),
            vec![("generate_series".to_string(), "INTEGER".to_string())],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_generate_series_wrong_arg_type() {
        let result = TableFunctionExecutor::new(
            "generate_series".to_string(),
            vec![Value::Text("one".to_string()), Value::Int(10)],
            FunctionReturnType::Setof("INTEGER".to_string()),
            vec![("generate_series".to_string(), "INTEGER".to_string())],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_output_schema() {
        let exec = TableFunctionExecutor::generate_series(1, 5).unwrap();
        let schema = exec.output_schema();

        assert_eq!(schema.len(), 1);
        assert_eq!(schema[0].0, "generate_series");
        assert_eq!(schema[0].1, "INTEGER");
    }

    #[test]
    fn test_is_exhausted() {
        let mut exec = TableFunctionExecutor::generate_series(1, 1).unwrap();
        assert!(!exec.is_exhausted());

        exec.next().unwrap();
        assert!(exec.is_exhausted());
    }
}
