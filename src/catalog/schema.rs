use crate::parser::ast::{ColumnDef, ForeignKeyDef};

/// Table schema definition
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Option<Vec<String>>,
    pub foreign_keys: Vec<ForeignKeyDef>,
}

impl TableSchema {
    pub fn new(name: String, columns: Vec<ColumnDef>) -> Self {
        Self { name, columns, primary_key: None, foreign_keys: Vec::new() }
    }

    pub fn with_constraints(
        name: String,
        columns: Vec<ColumnDef>,
        primary_key: Option<Vec<String>>,
        foreign_keys: Vec<ForeignKeyDef>,
    ) -> Self {
        Self { name, columns, primary_key, foreign_keys }
    }
}
