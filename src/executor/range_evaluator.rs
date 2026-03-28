use crate::catalog::Value;
use crate::executor::operators::executor::ExecutorError;

pub struct RangeEvaluator;

impl RangeEvaluator {
    pub fn eval_range_contains(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
        match (left, right) {
            (Value::Range(range), Value::Int(elem)) => {
                if let Some(lower) = range.lower_bound() {
                    if let Value::Int(lv) = lower {
                        if !range.lower_inclusive() && *lv >= *elem {
                            return Ok(Value::Bool(false));
                        }
                        if range.lower_inclusive() && *lv > *elem {
                            return Ok(Value::Bool(false));
                        }
                    }
                }
                if let Some(upper) = range.upper_bound() {
                    if let Value::Int(uv) = upper {
                        if !range.upper_inclusive() && *uv <= *elem {
                            return Ok(Value::Bool(false));
                        }
                        if range.upper_inclusive() && *uv < *elem {
                            return Ok(Value::Bool(false));
                        }
                    }
                }
                Ok(Value::Bool(true))
            }
            _ => Err(ExecutorError::TypeMismatch(
                "Range contains (@>) requires range on left side and element on right side"
                    .to_string(),
            )),
        }
    }

    pub fn eval_range_contained_by(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
        match (left, right) {
            (Value::Int(elem), Value::Range(range)) => {
                if let Some(lower) = range.lower_bound() {
                    if let Value::Int(lv) = lower {
                        if !range.lower_inclusive() && *lv >= *elem {
                            return Ok(Value::Bool(false));
                        }
                        if range.lower_inclusive() && *lv > *elem {
                            return Ok(Value::Bool(false));
                        }
                    }
                }
                if let Some(upper) = range.upper_bound() {
                    if let Value::Int(uv) = upper {
                        if !range.upper_inclusive() && *uv <= *elem {
                            return Ok(Value::Bool(false));
                        }
                        if range.upper_inclusive() && *uv < *elem {
                            return Ok(Value::Bool(false));
                        }
                    }
                }
                Ok(Value::Bool(true))
            }
            _ => Err(ExecutorError::TypeMismatch(
                "Range contained by (<@) requires element on left side and range on right side"
                    .to_string(),
            )),
        }
    }

    pub fn eval_range_overlaps(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
        match (left, right) {
            (Value::Range(r1), Value::Range(r2)) => {
                let r1_lower = r1.lower_bound();
                let r1_upper = r1.upper_bound();
                let r2_lower = r2.lower_bound();
                let r2_upper = r2.upper_bound();

                if let (Some(l1), Some(u1), Some(l2), Some(u2)) =
                    (r1_lower, r1_upper, r2_lower, r2_upper)
                {
                    if let (Value::Int(l1v), Value::Int(u1v), Value::Int(l2v), Value::Int(u2v)) =
                        (l1, u1, l2, u2)
                    {
                        let r1_left_of_r2 =
                            if r1.upper_inclusive() { *u1v <= *l2v } else { *u1v < *l2v };
                        let r2_left_of_r1 =
                            if r2.upper_inclusive() { *u2v <= *l1v } else { *u2v < *l1v };
                        return Ok(Value::Bool(!(r1_left_of_r2 || r2_left_of_r1)));
                    }
                }
                Err(ExecutorError::TypeMismatch(
                    "Range overlaps (&&) requires compatible range types".to_string(),
                ))
            }
            _ => Err(ExecutorError::TypeMismatch(
                "Range overlaps (&&) requires range operands".to_string(),
            )),
        }
    }

    pub fn eval_range_left_of(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
        match (left, right) {
            (Value::Range(r1), Value::Range(r2)) => {
                if let (Some(u1), Some(l2)) = (r1.upper_bound(), r2.lower_bound()) {
                    if let (Value::Int(u1v), Value::Int(l2v)) = (u1, l2) {
                        let result = if r1.upper_inclusive() { *u1v <= *l2v } else { *u1v < *l2v };
                        return Ok(Value::Bool(result));
                    }
                }
                Err(ExecutorError::TypeMismatch(
                    "Range left of (<<) requires integer range operands".to_string(),
                ))
            }
            _ => Err(ExecutorError::TypeMismatch(
                "Range left of (<<) requires range operands".to_string(),
            )),
        }
    }

    pub fn eval_range_right_of(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
        match (left, right) {
            (Value::Range(r1), Value::Range(r2)) => {
                if let (Some(l1), Some(u2)) = (r1.lower_bound(), r2.upper_bound()) {
                    if let (Value::Int(l1v), Value::Int(u2v)) = (l1, u2) {
                        let result = if r2.upper_inclusive() { *l1v > *u2v } else { *l1v >= *u2v };
                        return Ok(Value::Bool(result));
                    }
                }
                Err(ExecutorError::TypeMismatch(
                    "Range right of (>>) requires integer range operands".to_string(),
                ))
            }
            _ => Err(ExecutorError::TypeMismatch(
                "Range right of (>>) requires range operands".to_string(),
            )),
        }
    }

    pub fn eval_range_adjacent(left: &Value, right: &Value) -> Result<Value, ExecutorError> {
        match (left, right) {
            (Value::Range(r1), Value::Range(r2)) => {
                if let (Some(u1), Some(l2)) = (r1.upper_bound(), r2.lower_bound()) {
                    if let (Value::Int(u1v), Value::Int(l2v)) = (u1, l2) {
                        let diff = if r1.upper_inclusive() != r2.lower_inclusive() {
                            (*l2v as i64 - *u1v as i64).abs()
                        } else {
                            (*l2v as i64 - *u1v as i64).abs() - 1
                        };
                        return Ok(Value::Bool(diff == 1));
                    }
                }
                Err(ExecutorError::TypeMismatch(
                    "Range adjacent (-|-) requires integer range operands".to_string(),
                ))
            }
            _ => Err(ExecutorError::TypeMismatch(
                "Range adjacent (-|-) requires range operands".to_string(),
            )),
        }
    }
}
