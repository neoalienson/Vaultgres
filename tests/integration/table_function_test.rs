use vaultgres::catalog::Value;
use vaultgres::executor::{Executor, table_function::TableFunctionExecutor};
use vaultgres::parser::ast::FunctionReturnType;

#[test]
fn test_table_function_executor_generate_series_basic() {
    let mut exec = TableFunctionExecutor::generate_series(1, 3).unwrap();

    let result1 = exec.next().unwrap();
    assert!(result1.is_some());

    let result2 = exec.next().unwrap();
    assert!(result2.is_some());

    let result3 = exec.next().unwrap();
    assert!(result3.is_some());

    let result4 = exec.next().unwrap();
    assert!(result4.is_none());
}

#[test]
fn test_table_function_executor_generate_series_empty_range() {
    let mut exec = TableFunctionExecutor::generate_series(5, 1).unwrap();

    let result = exec.next().unwrap();
    assert!(result.is_none());
}

#[test]
fn test_table_function_executor_generate_series_single_value() {
    let mut exec = TableFunctionExecutor::generate_series(42, 42).unwrap();

    let result = exec.next().unwrap();
    assert!(result.is_some());

    let result2 = exec.next().unwrap();
    assert!(result2.is_none());
}

#[test]
fn test_table_function_executor_generate_series_large_range() {
    let mut exec = TableFunctionExecutor::generate_series(1, 10000).unwrap();

    let mut count = 0;
    loop {
        match exec.next() {
            Ok(Some(_)) => count += 1,
            Ok(None) => break,
            Err(e) => panic!("Error: {:?}", e),
        }
    }
    assert_eq!(count, 10000);
}

#[test]
fn test_table_function_executor_generate_series_stepped() {
    let mut exec = TableFunctionExecutor::generate_series_stepped(0, 10, 2).unwrap();

    let mut values = Vec::new();
    loop {
        match exec.next() {
            Ok(Some(tuple)) => {
                if let Value::Int(n) = tuple.get("").unwrap() {
                    values.push(*n);
                }
            }
            Ok(None) => break,
            Err(e) => panic!("Error: {:?}", e),
        }
    }

    assert_eq!(values, vec![0, 2, 4, 6, 8, 10]);
}

#[test]
fn test_table_function_executor_generate_series_negative_step() {
    let mut exec = TableFunctionExecutor::generate_series_stepped(10, 0, -2).unwrap();

    let mut values = Vec::new();
    loop {
        match exec.next() {
            Ok(Some(tuple)) => {
                if let Value::Int(n) = tuple.get("").unwrap() {
                    values.push(*n);
                }
            }
            Ok(None) => break,
            Err(e) => panic!("Error: {:?}", e),
        }
    }

    assert_eq!(values, vec![10, 8, 6, 4, 2, 0]);
}

#[test]
fn test_table_function_executor_generate_series_stepped_single_element() {
    let mut exec = TableFunctionExecutor::generate_series_stepped(5, 5, 1).unwrap();

    let result = exec.next().unwrap();
    assert!(result.is_some());

    let result2 = exec.next().unwrap();
    assert!(result2.is_none());
}

#[test]
fn test_table_function_executor_unnest_integers() {
    let arr = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
    let mut exec = TableFunctionExecutor::unnest(arr).unwrap();

    let result1 = exec.next().unwrap();
    assert!(result1.is_some());

    let result2 = exec.next().unwrap();
    assert!(result2.is_some());

    let result3 = exec.next().unwrap();
    assert!(result3.is_some());

    let result4 = exec.next().unwrap();
    assert!(result4.is_none());
}

#[test]
fn test_table_function_executor_unnest_strings() {
    let arr = vec![
        Value::Text("apple".to_string()),
        Value::Text("banana".to_string()),
        Value::Text("cherry".to_string()),
    ];
    let mut exec = TableFunctionExecutor::unnest(arr).unwrap();

    let result1 = exec.next().unwrap();
    assert!(result1.is_some());
    if let Value::Text(s) = result1.unwrap().get("").unwrap() {
        assert_eq!(s, "apple");
    } else {
        panic!("Expected Text");
    }
}

#[test]
fn test_table_function_executor_unnest_empty() {
    let arr: Vec<Value> = vec![];
    let mut exec = TableFunctionExecutor::unnest(arr).unwrap();

    let result = exec.next().unwrap();
    assert!(result.is_none());
}

#[test]
fn test_table_function_executor_unnest_with_nulls() {
    let arr = vec![Value::Int(1), Value::Null, Value::Int(3)];
    let mut exec = TableFunctionExecutor::unnest(arr).unwrap();

    let result1 = exec.next().unwrap();
    assert!(result1.is_some());

    let result2 = exec.next().unwrap();
    assert!(result2.is_some());
    assert_eq!(result2.unwrap().get("").unwrap(), &Value::Null);

    let result3 = exec.next().unwrap();
    assert!(result3.is_some());
}

#[test]
fn test_table_function_executor_unnest_mixed_types() {
    let arr = vec![
        Value::Int(1),
        Value::Text("hello".to_string()),
        Value::Bool(true),
        Value::Float(3.14),
    ];
    let mut exec = TableFunctionExecutor::unnest(arr).unwrap();

    let mut count = 0;
    loop {
        match exec.next() {
            Ok(Some(_)) => count += 1,
            Ok(None) => break,
            Err(e) => panic!("Error: {:?}", e),
        }
    }
    assert_eq!(count, 4);
}

#[test]
fn test_table_function_executor_output_schema() {
    let exec = TableFunctionExecutor::generate_series(1, 5).unwrap();
    let schema = exec.output_schema();

    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].0, "generate_series");
    assert_eq!(schema[0].1, "INTEGER");
}

#[test]
fn test_table_function_executor_is_exhausted() {
    let mut exec = TableFunctionExecutor::generate_series(1, 1).unwrap();
    assert!(!exec.is_exhausted());

    exec.next().unwrap();
    assert!(exec.is_exhausted());
}

#[test]
fn test_table_function_executor_generate_series_negative_range() {
    let mut exec = TableFunctionExecutor::generate_series(-3, 3).unwrap();

    let mut values = Vec::new();
    loop {
        match exec.next() {
            Ok(Some(tuple)) => {
                if let Value::Int(n) = tuple.get("").unwrap() {
                    values.push(*n);
                }
            }
            Ok(None) => break,
            Err(e) => panic!("Error: {:?}", e),
        }
    }

    assert_eq!(values, vec![-3, -2, -1, 0, 1, 2, 3]);
}

#[test]
fn test_table_function_executor_with_function_return_type() {
    let result = TableFunctionExecutor::new(
        "generate_series".to_string(),
        vec![Value::Int(1), Value::Int(5)],
        FunctionReturnType::Setof("INTEGER".to_string()),
        vec![("generate_series".to_string(), "INTEGER".to_string())],
    );
    assert!(result.is_ok());
}

#[test]
fn test_table_function_executor_invalid_arg_count() {
    let result = TableFunctionExecutor::new(
        "generate_series".to_string(),
        vec![Value::Int(1)],
        FunctionReturnType::Setof("INTEGER".to_string()),
        vec![("generate_series".to_string(), "INTEGER".to_string())],
    );
    assert!(result.is_err());
}

#[test]
fn test_table_function_executor_wrong_argument_type() {
    let result = TableFunctionExecutor::new(
        "generate_series".to_string(),
        vec![Value::Text("one".to_string()), Value::Int(10)],
        FunctionReturnType::Setof("INTEGER".to_string()),
        vec![("generate_series".to_string(), "INTEGER".to_string())],
    );
    assert!(result.is_err());
}

#[test]
fn test_table_function_executor_unnest_two_dimensional() {
    let inner1 = vec![Value::Int(1), Value::Int(2)];
    let inner2 = vec![Value::Int(3), Value::Int(4)];
    let arr = vec![Value::Array(inner1), Value::Array(inner2)];
    let mut exec = TableFunctionExecutor::unnest(arr).unwrap();

    let result1 = exec.next().unwrap();
    assert!(result1.is_some());
    if let Value::Array(inner) = result1.unwrap().get("").unwrap() {
        assert_eq!(inner.len(), 2);
    } else {
        panic!("Expected Array");
    }
}

#[test]
fn test_table_function_multiple_sequential_calls() {
    let mut exec1 = TableFunctionExecutor::generate_series(1, 3).unwrap();

    let first = exec1.next().unwrap();
    assert!(first.is_some());

    let mut exec2 = TableFunctionExecutor::generate_series(10, 12).unwrap();
    let result = exec2.next().unwrap();
    assert!(result.is_some());
}
