/// Value types stored in tuples
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Bool(bool),
    Text(String),
    Array(Vec<Value>),
    Json(String),
    Date(i32),
    Time(i64),
    Timestamp(i64),
    Decimal(i128, u8),
    Bytea(Vec<u8>),
    Null,
}

impl Value {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Value::Int(i) => i.to_le_bytes().to_vec(),
            Value::Float(f) => f.to_le_bytes().to_vec(),
            Value::Bool(b) => vec![*b as u8],
            Value::Text(s) => s.as_bytes().to_vec(),
            Value::Array(_) => b"ARRAY".to_vec(), // Placeholder
            Value::Json(_) => b"JSON".to_vec(),   // Placeholder
            Value::Date(d) => d.to_le_bytes().to_vec(),
            Value::Time(t) => t.to_le_bytes().to_vec(),
            Value::Timestamp(ts) => ts.to_le_bytes().to_vec(),
            Value::Decimal(_, _) => b"DECIMAL".to_vec(), // Placeholder
            Value::Bytea(b) => b.clone(),
            Value::Null => vec![],
        }
    }
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
            (Value::Date(a), Value::Date(b)) => a.partial_cmp(b),
            (Value::Time(a), Value::Time(b)) => a.partial_cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.partial_cmp(b),
            (Value::Decimal(a, s1), Value::Decimal(b, s2)) if s1 == s2 => a.partial_cmp(b),
            (Value::Bytea(a), Value::Bytea(b)) => a.partial_cmp(b),
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
            (Value::Date(a), Value::Date(b)) => a.cmp(b),
            (Value::Time(a), Value::Time(b)) => a.cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
            (Value::Decimal(a, _), Value::Decimal(b, _)) => a.cmp(b),
            (Value::Bytea(a), Value::Bytea(b)) => a.cmp(b),
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
            Value::Date(d) => d.hash(state),
            Value::Time(t) => t.hash(state),
            Value::Timestamp(ts) => ts.hash(state),
            Value::Decimal(v, s) => {
                v.hash(state);
                s.hash(state);
            }
            Value::Bytea(b) => b.hash(state),
            Value::Null => 0.hash(state),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Value::Int(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Text(s) => write!(f, "{}", s),
            Value::Array(_) => write!(f, "ARRAY"), // TODO: Proper array display
            Value::Json(j) => write!(f, "{}", j),
            Value::Date(d) => write!(f, "{}", d), // TODO: Proper date display
            Value::Time(t) => write!(f, "{}", t), // TODO: Proper time display
            Value::Timestamp(ts) => write!(f, "{}", ts), // TODO: Proper timestamp display
            Value::Decimal(v, s) => write!(f, "{}.{}", v, s), // TODO: Proper decimal display
            Value::Bytea(_) => write!(f, "BYTEA"), // TODO: Proper bytea display
            Value::Null => write!(f, "NULL"),
        }
    }
}
