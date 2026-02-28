/// Value types stored in tuples
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Value {
    Int(i64),
    Text(String),
}
