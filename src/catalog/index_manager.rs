use super::crud_helper::CrudHelper;
use crate::parser::ast::CreateIndexStmt;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct IndexManager {
    pub(crate) indexes: Arc<RwLock<HashMap<String, CreateIndexStmt>>>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self { indexes: Arc::new(RwLock::new(HashMap::new())) }
    }

    pub fn with_indexes(indexes: Arc<RwLock<HashMap<String, CreateIndexStmt>>>) -> Self {
        Self { indexes }
    }

    pub fn create_index(&self, index: CreateIndexStmt) -> Result<(), String> {
        CrudHelper::create(&self.indexes, index.name.clone(), index, "Index")
    }

    pub fn drop_index(&self, name: &str, if_exists: bool) -> Result<(), String> {
        CrudHelper::drop(&self.indexes, name, if_exists, "Index")
    }

    pub fn get_index(&self, name: &str) -> Option<CreateIndexStmt> {
        CrudHelper::get(&self.indexes, name)
    }
}
