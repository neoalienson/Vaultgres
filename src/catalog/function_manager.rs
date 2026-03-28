use super::Function;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct FunctionManager {
    pub(crate) functions: Arc<RwLock<HashMap<String, Vec<Function>>>>,
}

impl FunctionManager {
    pub fn new() -> Self {
        Self { functions: Arc::new(RwLock::new(HashMap::new())) }
    }

    pub fn with_functions(functions: Arc<RwLock<HashMap<String, Vec<Function>>>>) -> Self {
        Self { functions }
    }

    pub fn create_function(&self, func: Function) -> Result<(), String> {
        let mut functions = self.functions.write().unwrap();
        functions.entry(func.name.clone()).or_default().push(func);
        Ok(())
    }

    pub fn drop_function(&self, name: &str, if_exists: bool) -> Result<(), String> {
        let mut functions = self.functions.write().unwrap();
        if let Some(funcs) = functions.get_mut(name) {
            funcs.retain(|_| true);
            functions.remove(name);
            Ok(())
        } else {
            if if_exists { Ok(()) } else { Err(format!("Function '{}' does not exist", name)) }
        }
    }

    pub fn get_function(&self, name: &str, arg_types: &[String]) -> Option<Function> {
        let functions = self.functions.read().unwrap();
        functions
            .get(name)?
            .iter()
            .find(|f| {
                f.parameters.len() == arg_types.len()
                    && f.parameters.iter().zip(arg_types).all(|(p, t)| &p.data_type == t)
            })
            .cloned()
    }

    pub fn get_all_functions(&self) -> Vec<Function> {
        let functions = self.functions.read().unwrap();
        functions.values().flatten().cloned().collect()
    }
}
