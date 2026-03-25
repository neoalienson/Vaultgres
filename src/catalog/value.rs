use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnumValue {
    pub type_name: String,
    pub index: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash)]
pub struct CompositeValue {
    pub type_name: String,
    pub fields: Vec<(String, Value)>,
}

impl std::fmt::Display for CompositeValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "({})",
            self.fields.iter().map(|(_, v)| format!("{}", v)).collect::<Vec<_>>().join(", ")
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RangeBound {
    pub value: Box<Value>,
    pub inclusive: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Range {
    pub lower: Option<RangeBound>,
    pub upper: Option<RangeBound>,
}

impl std::fmt::Display for Range {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let lower_bracket = if self.lower_inclusive() { '[' } else { '(' };
        let upper_bracket = if self.upper_inclusive() { ']' } else { ')' };
        write!(f, "{}", lower_bracket)?;
        if let Some(l) = self.lower_bound() {
            write!(f, "{}", l)?;
        }
        write!(f, ",")?;
        if let Some(u) = self.upper_bound() {
            write!(f, "{}", u)?;
        }
        write!(f, "{}", upper_bracket)
    }
}

impl Range {
    pub fn new(
        lower: Option<Value>,
        lower_inclusive: bool,
        upper: Option<Value>,
        upper_inclusive: bool,
    ) -> Self {
        Self {
            lower: lower.map(|v| RangeBound { value: Box::new(v), inclusive: lower_inclusive }),
            upper: upper.map(|v| RangeBound { value: Box::new(v), inclusive: upper_inclusive }),
        }
    }

    pub fn empty() -> Self {
        Self { lower: None, upper: None }
    }

    pub fn is_empty(&self) -> bool {
        match (&self.lower, &self.upper) {
            (Some(l), Some(u)) => {
                let lower_val = l.value.as_ref();
                let upper_val = u.value.as_ref();
                match (lower_val, upper_val) {
                    (Value::Int(lv), Value::Int(uv)) => {
                        if l.inclusive && u.inclusive {
                            *lv > *uv
                        } else if l.inclusive && !u.inclusive {
                            *lv >= *uv
                        } else if !l.inclusive && u.inclusive {
                            *lv > *uv
                        } else {
                            *lv >= *uv
                        }
                    }
                    (Value::Date(lv), Value::Date(uv)) => {
                        if l.inclusive && u.inclusive {
                            *lv > *uv
                        } else {
                            *lv >= *uv
                        }
                    }
                    (Value::Timestamp(lv), Value::Timestamp(uv)) => {
                        if l.inclusive && u.inclusive {
                            *lv > *uv
                        } else {
                            *lv >= *uv
                        }
                    }
                    _ => true,
                }
            }
            _ => false,
        }
    }

    pub fn lower_bound(&self) -> Option<&Value> {
        self.lower.as_ref().map(|b| b.value.as_ref())
    }

    pub fn upper_bound(&self) -> Option<&Value> {
        self.upper.as_ref().map(|b| b.value.as_ref())
    }

    pub fn lower_inclusive(&self) -> bool {
        self.lower.as_ref().map(|b| b.inclusive).unwrap_or(false)
    }

    pub fn upper_inclusive(&self) -> bool {
        self.upper.as_ref().map(|b| b.inclusive).unwrap_or(false)
    }
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
    Composite(CompositeValue),
    Range(Range),
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
            Value::Composite(c) => {
                let mut bytes = c.type_name.as_bytes().to_vec();
                bytes.push(b':');
                for (i, (_, field_val)) in c.fields.iter().enumerate() {
                    if i > 0 {
                        bytes.push(b',');
                    }
                    bytes.extend_from_slice(field_val.to_bytes().as_slice());
                }
                bytes
            }
            Value::Range(r) => {
                let mut bytes = vec![];
                bytes.push(if r.lower_inclusive() { b'[' } else { b'(' });
                if let Some(l) = r.lower.as_ref() {
                    bytes.extend_from_slice(l.value.to_bytes().as_slice());
                }
                bytes.push(b',');
                if let Some(u) = r.upper.as_ref() {
                    bytes.extend_from_slice(u.value.to_bytes().as_slice());
                }
                bytes.push(if r.upper_inclusive() { b']' } else { b')' });
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

    pub fn type_name(&self) -> String {
        match self {
            Value::Int(_) => "INT".to_string(),
            Value::Float(_) => "FLOAT".to_string(),
            Value::Bool(_) => "BOOL".to_string(),
            Value::Text(_) => "TEXT".to_string(),
            Value::Array(_) => "ARRAY".to_string(),
            Value::Json(_) => "JSON".to_string(),
            Value::Date(_) => "DATE".to_string(),
            Value::Time(_) => "TIME".to_string(),
            Value::Timestamp(_) => "TIMESTAMP".to_string(),
            Value::Decimal(_, _) => "DECIMAL".to_string(),
            Value::Bytea(_) => "BYTEA".to_string(),
            Value::Enum(e) => e.type_name.clone(),
            Value::Composite(c) => c.type_name.clone(),
            Value::Range(_) => "RANGE".to_string(),
            Value::Null => "NULL".to_string(),
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
            (Value::Composite(a), Value::Composite(b)) => {
                if a.type_name != b.type_name {
                    return None;
                }
                for ((_, av), (_, bv)) in a.fields.iter().zip(b.fields.iter()) {
                    match av.partial_cmp(bv) {
                        Some(std::cmp::Ordering::Equal) => continue,
                        other => return other,
                    }
                }
                Some(std::cmp::Ordering::Equal)
            }
            (Value::Range(_), Value::Range(_)) => None,
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
            (Value::Composite(a), Value::Composite(b)) => match a.type_name.cmp(&b.type_name) {
                std::cmp::Ordering::Equal => {
                    for ((_, av), (_, bv)) in a.fields.iter().zip(b.fields.iter()) {
                        match av.cmp(bv) {
                            std::cmp::Ordering::Equal => continue,
                            other => return other,
                        }
                    }
                    std::cmp::Ordering::Equal
                }
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
            Value::Composite(c) => {
                "Composite".hash(state);
                c.type_name.hash(state);
                for (name, val) in &c.fields {
                    name.hash(state);
                    val.hash(state);
                }
            }
            Value::Range(r) => {
                "Range".hash(state);
                if let Some(l) = &r.lower {
                    l.value.hash(state);
                    l.inclusive.hash(state);
                }
                if let Some(u) = &r.upper {
                    u.value.hash(state);
                    u.inclusive.hash(state);
                }
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
            Value::Array(_) => write!(f, "ARRAY"),
            Value::Json(j) => write!(f, "{}", j),
            Value::Date(d) => write!(f, "{}", d),
            Value::Time(t) => write!(f, "{}", t),
            Value::Timestamp(ts) => write!(f, "{}", ts),
            Value::Decimal(v, s) => write!(f, "{}.{}", v, s),
            Value::Bytea(_) => write!(f, "BYTEA"),
            Value::Enum(e) => write!(f, "{}[{}]", e.type_name, e.index),
            Value::Composite(c) => {
                write!(
                    f,
                    "({})",
                    c.fields.iter().map(|(_, v)| format!("{}", v)).collect::<Vec<_>>().join(", ")
                )
            }
            Value::Range(r) => {
                let lower_bracket = if r.lower_inclusive() { '[' } else { '(' };
                let upper_bracket = if r.upper_inclusive() { ']' } else { ')' };
                write!(f, "{}", lower_bracket)?;
                if let Some(l) = r.lower_bound() {
                    write!(f, "{}", l)?;
                }
                write!(f, ",")?;
                if let Some(u) = r.upper_bound() {
                    write!(f, "{}", u)?;
                }
                write!(f, "{}", upper_bracket)
            }
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

    #[test]
    fn test_composite_value_creation() {
        let composite = Value::Composite(CompositeValue {
            type_name: "address".to_string(),
            fields: vec![
                ("street".to_string(), Value::Text("123 Main St".to_string())),
                ("city".to_string(), Value::Text("NYC".to_string())),
            ],
        });
        assert!(matches!(composite, Value::Composite(_)));
    }

    #[test]
    fn test_composite_value_to_bytes() {
        let composite = Value::Composite(CompositeValue {
            type_name: "address".to_string(),
            fields: vec![
                ("street".to_string(), Value::Int(123)),
                ("city".to_string(), Value::Int(456)),
            ],
        });
        let bytes = composite.to_bytes();
        assert!(bytes.starts_with(b"address:"));
    }

    #[test]
    fn test_composite_value_partial_eq() {
        let composite1 = Value::Composite(CompositeValue {
            type_name: "address".to_string(),
            fields: vec![
                ("street".to_string(), Value::Int(123)),
                ("city".to_string(), Value::Int(456)),
            ],
        });
        let composite2 = Value::Composite(CompositeValue {
            type_name: "address".to_string(),
            fields: vec![
                ("street".to_string(), Value::Int(123)),
                ("city".to_string(), Value::Int(456)),
            ],
        });
        let composite3 = Value::Composite(CompositeValue {
            type_name: "address".to_string(),
            fields: vec![
                ("street".to_string(), Value::Int(789)),
                ("city".to_string(), Value::Int(456)),
            ],
        });
        assert_eq!(composite1, composite2);
        assert_ne!(composite1, composite3);
    }

    #[test]
    fn test_composite_value_partial_ord() {
        let composite1 = Value::Composite(CompositeValue {
            type_name: "address".to_string(),
            fields: vec![("a".to_string(), Value::Int(1))],
        });
        let composite2 = Value::Composite(CompositeValue {
            type_name: "address".to_string(),
            fields: vec![("a".to_string(), Value::Int(2))],
        });
        assert_eq!(composite1.partial_cmp(&composite2), Some(std::cmp::Ordering::Less));
        assert_eq!(composite2.partial_cmp(&composite1), Some(std::cmp::Ordering::Greater));
        assert_eq!(composite1.partial_cmp(&composite1), Some(std::cmp::Ordering::Equal));
    }

    #[test]
    fn test_composite_value_hash() {
        let composite1 = Value::Composite(CompositeValue {
            type_name: "address".to_string(),
            fields: vec![("street".to_string(), Value::Text("123 Main St".to_string()))],
        });
        let composite2 = Value::Composite(CompositeValue {
            type_name: "address".to_string(),
            fields: vec![("street".to_string(), Value::Text("123 Main St".to_string()))],
        });
        let composite3 = Value::Composite(CompositeValue {
            type_name: "address".to_string(),
            fields: vec![("street".to_string(), Value::Text("456 Oak Ave".to_string()))],
        });
        assert_eq!(calculate_hash(&composite1), calculate_hash(&composite2));
        assert_ne!(calculate_hash(&composite1), calculate_hash(&composite3));
    }

    #[test]
    fn test_composite_value_display() {
        let composite = Value::Composite(CompositeValue {
            type_name: "address".to_string(),
            fields: vec![
                ("street".to_string(), Value::Text("123 Main St".to_string())),
                ("city".to_string(), Value::Text("NYC".to_string())),
            ],
        });
        assert_eq!(format!("{}", composite), "(123 Main St, NYC)");
    }

    #[test]
    fn test_composite_value_with_null_field() {
        let composite = Value::Composite(CompositeValue {
            type_name: "address".to_string(),
            fields: vec![
                ("street".to_string(), Value::Text("123 Main St".to_string())),
                ("city".to_string(), Value::Null),
            ],
        });
        assert!(matches!(composite, Value::Composite(_)));
    }

    #[test]
    fn test_composite_value_nested() {
        let inner = Value::Composite(CompositeValue {
            type_name: "inner_type".to_string(),
            fields: vec![("x".to_string(), Value::Int(10))],
        });
        let outer = Value::Composite(CompositeValue {
            type_name: "outer_type".to_string(),
            fields: vec![
                ("inner".to_string(), inner),
                ("name".to_string(), Value::Text("test".to_string())),
            ],
        });
        assert!(matches!(outer, Value::Composite(_)));
    }
}
