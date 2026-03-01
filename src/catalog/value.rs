/// Value types stored in tuples
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Bool(bool),
    Text(String),
    Array(Vec<Value>),
    Json(String),
    Null,
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
            (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
            (Value::Json(a), Value::Json(b)) => a.partial_cmp(b),
            (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
            _ => None,
        }
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => {
                if a < b {
                    std::cmp::Ordering::Less
                } else if a > b {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                }
            }
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Json(a), Value::Json(b)) => a.cmp(b),
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            _ => std::cmp::Ordering::Equal,
        }
    }
}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::Int(i) => i.hash(state),
            Value::Float(f) => f.to_bits().hash(state),
            Value::Bool(b) => b.hash(state),
            Value::Text(s) => s.hash(state),
            Value::Array(a) => a.hash(state),
            Value::Json(j) => j.hash(state),
            Value::Null => 0.hash(state),
        }
    }
}
