use super::value::Value;
use crate::transaction::TupleHeader;
use std::collections::HashMap;

/// Tuple with MVCC header and data
#[derive(Debug, Clone)]
pub struct Tuple {
    pub header: TupleHeader,
    pub data: Vec<Value>,
    pub column_map: HashMap<String, usize>,
}

impl Tuple {
    pub fn new() -> Self {
        Self { header: TupleHeader::new(0), data: Vec::new(), column_map: HashMap::new() }
    }

    pub fn add_value(&mut self, name: String, value: Value) {
        let index = self.data.len();
        self.data.push(value);
        self.column_map.insert(name, index);
    }

    pub fn get_value(&self, name: &str) -> Option<Value> {
        self.column_map.get(name).and_then(|&idx| self.data.get(idx).cloned())
    }
}

impl Default for Tuple {
    fn default() -> Self {
        Self::new()
    }
}
