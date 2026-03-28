use super::Aggregate;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct AggregateManager {
    pub(crate) aggregates: Arc<RwLock<HashMap<String, Aggregate>>>,
}

impl AggregateManager {
    pub fn new() -> Self {
        Self { aggregates: Arc::new(RwLock::new(HashMap::new())) }
    }

    pub fn with_aggregates(aggregates: Arc<RwLock<HashMap<String, Aggregate>>>) -> Self {
        Self { aggregates }
    }

    pub fn create_aggregate(&self, agg: Aggregate) -> Result<(), String> {
        let mut aggregates = self.aggregates.write().unwrap();
        aggregates.insert(agg.name.clone(), agg);
        Ok(())
    }

    pub fn drop_aggregate(&self, name: &str, if_exists: bool) -> Result<(), String> {
        let mut aggregates = self.aggregates.write().unwrap();
        if aggregates.remove(name).is_some() {
            Ok(())
        } else {
            if if_exists { Ok(()) } else { Err(format!("Aggregate '{}' does not exist", name)) }
        }
    }

    pub fn get_aggregate(&self, name: &str) -> Option<Aggregate> {
        let aggregates = self.aggregates.read().unwrap();
        aggregates.get(name).cloned()
    }
}
