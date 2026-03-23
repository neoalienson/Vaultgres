use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnumValue {
    pub type_name: String,
    pub index: i32,
}

/// Value types stored in tuples
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    Enum(EnumValue),
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
            Value::Enum(e) => {
                let mut bytes = e.type_name.as_bytes().to_vec();
                bytes.push(0);
                bytes.extend_from_slice(&e.index.to_le_bytes());
                bytes
            }
            Value::Null => vec![],
        }
    }

    pub fn as_float(&self) -> Result<f64, String> {
        match self {
            Value::Float(f) => Ok(*f),
            _ => Err(format!("Cannot convert {:?} to f64", self)),
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
            (Value::Enum(a), Value::Enum(b)) => {
                if a.type_name != b.type_name {
                    return None;
                }
                a.index.partial_cmp(&b.index)
            }
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
            (Value::Enum(a), Value::Enum(b)) => match a.type_name.cmp(&b.type_name) {
                std::cmp::Ordering::Equal => a.index.cmp(&b.index),
                other => other,
            },
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
            Value::Enum(e) => {
                "Enum".hash(state);
                e.type_name.hash(state);
                e.index.hash(state);
            }
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
            Value::Enum(e) => write!(f, "{}[{}]", e.type_name, e.index),
            Value::Null => write!(f, "NULL"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    // Helper to calculate hash
    fn calculate_hash<T: Hash>(t: &T) -> u64 {
        let mut s = DefaultHasher::new();
        t.hash(&mut s);
        s.finish()
    }

    #[test]
    fn test_to_bytes() {
        assert_eq!(Value::Int(123).to_bytes(), 123i64.to_le_bytes().to_vec());
        assert_eq!(Value::Float(12.3).to_bytes(), 12.3f64.to_le_bytes().to_vec());
        assert_eq!(Value::Bool(true).to_bytes(), vec![1]);
        assert_eq!(Value::Text("hello".to_string()).to_bytes(), "hello".as_bytes().to_vec());
        assert_eq!(Value::Array(vec![]).to_bytes(), b"ARRAY".to_vec());
        assert_eq!(Value::Json("{}".to_string()).to_bytes(), b"JSON".to_vec());
        assert_eq!(Value::Date(100).to_bytes(), 100i32.to_le_bytes().to_vec());
        assert_eq!(Value::Time(200).to_bytes(), 200i64.to_le_bytes().to_vec());
        assert_eq!(Value::Timestamp(300).to_bytes(), 300i64.to_le_bytes().to_vec());
        assert_eq!(Value::Decimal(123, 2).to_bytes(), b"DECIMAL".to_vec());
        assert_eq!(Value::Bytea(vec![1, 2, 3]).to_bytes(), vec![1, 2, 3]);
        assert_eq!(Value::Null.to_bytes(), Vec::<u8>::new());
    }

    #[test]
    fn test_partial_eq() {
        assert_eq!(Value::Int(1), Value::Int(1));
        assert_ne!(Value::Int(1), Value::Int(2));
        assert_eq!(Value::Text("a".to_string()), Value::Text("a".to_string()));
        assert_ne!(Value::Text("a".to_string()), Value::Text("b".to_string()));
        assert_eq!(Value::Null, Value::Null);
        assert_ne!(Value::Int(1), Value::Null); // Different types are not equal
        assert_ne!(Value::Int(1), Value::Text("1".to_string()));
    }

    #[test]
    fn test_partial_ord() {
        assert!(Value::Int(1) < Value::Int(2));
        assert!(Value::Int(2) > Value::Int(1));
        assert!(Value::Int(1) <= Value::Int(1));

        assert!(Value::Text("a".to_string()) < Value::Text("b".to_string()));
        assert!(Value::Text("b".to_string()) > Value::Text("a".to_string()));

        assert_eq!(Value::Int(1).partial_cmp(&Value::Null), None);
        assert_eq!(Value::Null.partial_cmp(&Value::Int(1)), None);
        assert_eq!(Value::Null.partial_cmp(&Value::Null), Some(std::cmp::Ordering::Equal));

        // Decimal with different scale should be None
        assert_eq!(Value::Decimal(10, 1).partial_cmp(&Value::Decimal(100, 2)), None);
        // Decimal with same scale should compare
        assert_eq!(
            Value::Decimal(10, 1).partial_cmp(&Value::Decimal(20, 1)),
            Some(std::cmp::Ordering::Less)
        );
    }

    #[test]
    fn test_ord() {
        assert_eq!(Value::Int(1).cmp(&Value::Int(2)), std::cmp::Ordering::Less);
        assert_eq!(Value::Int(2).cmp(&Value::Int(1)), std::cmp::Ordering::Greater);
        assert_eq!(Value::Int(1).cmp(&Value::Int(1)), std::cmp::Ordering::Equal);

        assert_eq!(
            Value::Text("a".to_string()).cmp(&Value::Text("b".to_string())),
            std::cmp::Ordering::Less
        );
        assert_eq!(Value::Null.cmp(&Value::Null), std::cmp::Ordering::Equal);

        // Ord for mismatched types defaults to Equal, this might be unexpected but is defined by the impl
        assert_eq!(Value::Int(1).cmp(&Value::Text("1".to_string())), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_hash() {
        let h1 = calculate_hash(&Value::Int(1));
        let h2 = calculate_hash(&Value::Int(1));
        let h3 = calculate_hash(&Value::Int(2));
        assert_eq!(h1, h2);
        assert_ne!(h1, h3);

        let h_text1 = calculate_hash(&Value::Text("hello".to_string()));
        let h_text2 = calculate_hash(&Value::Text("hello".to_string()));
        let h_text3 = calculate_hash(&Value::Text("world".to_string()));
        assert_eq!(h_text1, h_text2);
        assert_ne!(h_text1, h_text3);

        let h_null1 = calculate_hash(&Value::Null);
        let h_null2 = calculate_hash(&Value::Null);
        assert_eq!(h_null1, h_null2);

        // Different types should generally hash differently
        assert_ne!(calculate_hash(&Value::Int(1)), calculate_hash(&Value::Float(1.0)));
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", Value::Int(123)), "123");
        assert_eq!(format!("{}", Value::Float(12.3)), "12.3");
        assert_eq!(format!("{}", Value::Bool(true)), "true");
        assert_eq!(format!("{}", Value::Text("hello".to_string())), "hello");
        assert_eq!(format!("{}", Value::Array(vec![])), "ARRAY");
        assert_eq!(
            format!("{}", Value::Json("{\"key\":\"value\"}".to_string())),
            "{\"key\":\"value\"}"
        );
        assert_eq!(format!("{}", Value::Date(100)), "100");
        assert_eq!(format!("{}", Value::Time(200)), "200");
        assert_eq!(format!("{}", Value::Timestamp(300)), "300");
        assert_eq!(format!("{}", Value::Decimal(123, 2)), "123.2");
        assert_eq!(format!("{}", Value::Bytea(vec![1, 2, 3])), "BYTEA");
        assert_eq!(format!("{}", Value::Null), "NULL");
    }
}
