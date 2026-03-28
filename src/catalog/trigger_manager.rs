use super::crud_helper::CrudHelper;
use crate::parser::ast::CreateTriggerStmt;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct TriggerManager {
    pub(crate) triggers: Arc<RwLock<HashMap<String, CreateTriggerStmt>>>,
}

impl TriggerManager {
    pub fn new() -> Self {
        Self { triggers: Arc::new(RwLock::new(HashMap::new())) }
    }

    pub fn with_triggers(triggers: Arc<RwLock<HashMap<String, CreateTriggerStmt>>>) -> Self {
        Self { triggers }
    }

    pub fn create_trigger(&self, trigger: CreateTriggerStmt) -> Result<(), String> {
        CrudHelper::create(&self.triggers, trigger.name.clone(), trigger, "Trigger")
    }

    pub fn drop_trigger(&self, name: &str, if_exists: bool) -> Result<(), String> {
        CrudHelper::drop(&self.triggers, name, if_exists, "Trigger")
    }

    pub fn get_trigger(&self, name: &str) -> Option<CreateTriggerStmt> {
        CrudHelper::get(&self.triggers, name)
    }
}
