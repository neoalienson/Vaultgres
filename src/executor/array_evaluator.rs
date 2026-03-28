use crate::catalog::Value;
use crate::executor::operators::executor::ExecutorError;

pub struct ArrayEvaluator;

impl ArrayEvaluator {
    pub fn eval_array_contains(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
        match (left, right) {
            (Value::Array(left_arr), Value::Array(right_arr)) => {
                for elem in right_arr {
                    let mut found = false;
                    for item in left_arr {
                        if item == elem {
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        return Ok(Value::Bool(false));
                    }
                }
                Ok(Value::Bool(true))
            }
            (Value::Array(arr), elem) => {
                for item in arr {
                    if item == elem {
                        return Ok(Value::Bool(true));
                    }
                }
                Ok(Value::Bool(false))
            }
            _ => Err(ExecutorError::TypeMismatch(
                "Array contains (@>) requires array on left side".to_string(),
            )),
        }
    }

    pub fn eval_array_contained_by(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
        match (left, right) {
            (Value::Array(left_arr), Value::Array(right_arr)) => {
                for elem in left_arr {
                    let mut found = false;
                    for item in right_arr {
                        if item == elem {
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        return Ok(Value::Bool(false));
                    }
                }
                Ok(Value::Bool(true))
            }
            _ => Err(ExecutorError::TypeMismatch(
                "Array contained by (<@) requires arrays on both sides".to_string(),
            )),
        }
    }

    pub fn eval_array_overlaps(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
        let (left_arr, right_arr) = match (left, right) {
            (Value::Array(l), Value::Array(r)) => (l, r),
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "Array overlaps (&&) requires arrays on both sides".to_string(),
                ));
            }
        };

        for left_item in left_arr {
            for right_item in right_arr {
                if left_item == right_item {
                    return Ok(Value::Bool(true));
                }
            }
        }
        Ok(Value::Bool(false))
    }

    pub fn eval_array_concat(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
        match (left, right) {
            (Value::Array(l), Value::Array(r)) => {
                let mut result = l.clone();
                result.extend(r.clone());
                Ok(Value::Array(result))
            }
            (Value::Array(arr), elem) => {
                let mut result = arr.clone();
                result.push(elem.clone());
                Ok(Value::Array(result))
            }
            (elem, Value::Array(arr)) => {
                let mut result = vec![elem.clone()];
                result.extend(arr.clone());
                Ok(Value::Array(result))
            }
            _ => Err(ExecutorError::TypeMismatch(
                "Array concat (||) requires at least one array operand".to_string(),
            )),
        }
    }

    pub fn eval_array_access(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
        let arr = match left {
            Value::Array(arr) => arr,
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "Array element access requires array on left side".to_string(),
                ));
            }
        };

        let idx = match right {
            Value::Int(idx) => *idx,
            _ => {
                return Err(ExecutorError::TypeMismatch(
                    "Array index must be an integer".to_string(),
                ));
            }
        };

        if idx <= 0 {
            return Err(ExecutorError::InvalidArrayIndex("Array index must be >= 1".to_string()));
        }

        let idx = idx as usize;
        if idx > arr.len() {
            return Ok(Value::Null);
        }

        Ok(arr[idx - 1].clone())
    }
}
