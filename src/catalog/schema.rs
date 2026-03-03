use crate::parser::ast::{CheckConstraint, ColumnDef, ForeignKeyDef, UniqueConstraint};

/// Table schema definition
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Option<Vec<String>>,
    pub foreign_keys: Vec<ForeignKeyDef>,
    pub check_constraints: Vec<CheckConstraint>,
    pub unique_constraints: Vec<UniqueConstraint>,
}

impl TableSchema {
    pub fn new(name: String, columns: Vec<ColumnDef>) -> Self {
        Self {
            name,
            columns,
            primary_key: None,
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
            unique_constraints: Vec::new(),
        }
    }

    pub fn with_constraints(
        name: String,
        columns: Vec<ColumnDef>,
        primary_key: Option<Vec<String>>,
        foreign_keys: Vec<ForeignKeyDef>,
    ) -> Self {
        Self {
            name,
            columns,
            primary_key,
            foreign_keys,
            check_constraints: Vec::new(),
            unique_constraints: Vec::new(),
        }
    }

    pub fn with_all_constraints(
        name: String,
        columns: Vec<ColumnDef>,
        primary_key: Option<Vec<String>>,
        foreign_keys: Vec<ForeignKeyDef>,
        check_constraints: Vec<CheckConstraint>,
        unique_constraints: Vec<UniqueConstraint>,
    ) -> Self {
        Self { name, columns, primary_key, foreign_keys, check_constraints, unique_constraints }
    }
}
