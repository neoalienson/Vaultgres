use crate::parser::ast::{ColumnDef, DataType};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

pub struct Catalog {
    tables: Arc<RwLock<HashMap<String, TableSchema>>>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    pub fn create_table(&self, name: String, columns: Vec<ColumnDef>) -> Result<(), String> {
        let mut tables = self.tables.write().unwrap();
        
        if tables.contains_key(&name) {
            return Err(format!("Table '{}' already exists", name));
        }
        
        tables.insert(name.clone(), TableSchema { name, columns });
        Ok(())
    }
    
    pub fn drop_table(&self, name: &str, if_exists: bool) -> Result<(), String> {
        let mut tables = self.tables.write().unwrap();
        
        if tables.remove(name).is_none() && !if_exists {
            return Err(format!("Table '{}' does not exist", name));
        }
        
        Ok(())
    }
    
    pub fn get_table(&self, name: &str) -> Option<TableSchema> {
        let tables = self.tables.read().unwrap();
        tables.get(name).cloned()
    }
    
    pub fn list_tables(&self) -> Vec<String> {
        let tables = self.tables.read().unwrap();
        tables.keys().cloned().collect()
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_create_table() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
            ColumnDef { name: "name".to_string(), data_type: DataType::Text },
        ];
        
        assert!(catalog.create_table("users".to_string(), columns).is_ok());
        assert!(catalog.get_table("users").is_some());
    }
    
    #[test]
    fn test_create_duplicate_table() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("users".to_string(), columns.clone()).unwrap();
        assert!(catalog.create_table("users".to_string(), columns).is_err());
    }
    
    #[test]
    fn test_drop_table() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("users".to_string(), columns).unwrap();
        assert!(catalog.drop_table("users", false).is_ok());
        assert!(catalog.get_table("users").is_none());
    }
    
    #[test]
    fn test_drop_nonexistent_table() {
        let catalog = Catalog::new();
        assert!(catalog.drop_table("users", false).is_err());
        assert!(catalog.drop_table("users", true).is_ok());
    }
}
