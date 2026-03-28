use crate::catalog::{Aggregate, Value};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone)]
pub enum AggregateState {
    Count(i64),
    Sum(i64),
    Avg { sum: i64, count: i64 },
    Min(Value),
    Max(Value),
    Custom(CustomAggregateState),
}

#[derive(Debug, Clone)]
pub struct CustomAggregateState {
    pub info: Aggregate,
    pub state: Value,
}

pub fn hash_value(value: &Value, hasher: &mut DefaultHasher) {
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
        Value::Null => {
            "null".hash(hasher);
        }
        _ => {
            format!("{:?}", value).hash(hasher);
        }
    }
}
