use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct CrudHelper;

impl CrudHelper {
    pub fn create<T: Clone>(
        store: &Arc<RwLock<HashMap<String, T>>>,
        name: String,
        item: T,
        type_name: &str,
    ) -> Result<(), String> {
        let mut map = store.write().unwrap();
        if map.contains_key(&name) {
            return Err(format!("{} '{}' already exists", type_name, name));
        }
        map.insert(name, item);
        Ok(())
    }

    pub fn drop<T>(
        store: &Arc<RwLock<HashMap<String, T>>>,
        name: &str,
        if_exists: bool,
        type_name: &str,
    ) -> Result<(), String> {
        let mut map = store.write().unwrap();
        if map.remove(name).is_none() && !if_exists {
            return Err(format!("{} '{}' does not exist", type_name, name));
        }
        Ok(())
    }

    pub fn get<T: Clone>(store: &Arc<RwLock<HashMap<String, T>>>, name: &str) -> Option<T> {
        store.read().unwrap().get(name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create a new store for tests
    fn setup_store() -> Arc<RwLock<HashMap<String, String>>> {
        Arc::new(RwLock::new(HashMap::new()))
    }

    #[test]
    fn test_create_success() {
        let store = setup_store();
        let name = "table1".to_string();
        let item = "data1".to_string();
        let type_name = "Table";

        assert!(CrudHelper::create(&store, name.clone(), item.clone(), type_name).is_ok());
        assert_eq!(CrudHelper::get(&store, &name), Some(item));
    }

    #[test]
    fn test_create_already_exists() {
        let store = setup_store();
        let name = "table1".to_string();
        let item = "data1".to_string();
        let type_name = "Table";

        CrudHelper::create(&store, name.clone(), item.clone(), type_name).unwrap();
        let result = CrudHelper::create(&store, name.clone(), "data2".to_string(), type_name);

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Table 'table1' already exists");
        assert_eq!(CrudHelper::get(&store, &name), Some(item)); // Should still be original item
    }

    #[test]
    fn test_drop_success() {
        let store = setup_store();
        let name = "table1".to_string();
        let item = "data1".to_string();
        let type_name = "Table";

        CrudHelper::create(&store, name.clone(), item, type_name).unwrap();
        assert!(CrudHelper::drop(&store, &name, false, type_name).is_ok());
        assert_eq!(CrudHelper::get(&store, &name), None);
    }

    #[test]
    fn test_drop_non_existent_no_if_exists() {
        let store = setup_store();
        let name = "table1".to_string();
        let type_name = "Table";

        let result = CrudHelper::drop(&store, &name, false, type_name);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Table 'table1' does not exist");
    }

    #[test]
    fn test_drop_non_existent_with_if_exists() {
        let store = setup_store();
        let name = "table1".to_string();
        let type_name = "Table";

        assert!(CrudHelper::drop(&store, &name, true, type_name).is_ok());
    }

    #[test]
    fn test_get_existing() {
        let store = setup_store();
        let name = "table1".to_string();
        let item = "data1".to_string();
        let type_name = "Table";

        CrudHelper::create(&store, name.clone(), item.clone(), type_name).unwrap();
        assert_eq!(CrudHelper::get(&store, &name), Some(item));
    }

    #[test]
    fn test_get_non_existent() {
        let store = setup_store();
        let name = "table1".to_string();

        assert_eq!(CrudHelper::get(&store, &name), None);
    }

    #[test]
    fn test_multiple_create_drop_get() {
        let store = setup_store();
        let type_name = "Item";

        // Create item1
        assert!(
            CrudHelper::create(&store, "item1".to_string(), "value1".to_string(), type_name)
                .is_ok()
        );
        assert_eq!(CrudHelper::get(&store, "item1"), Some("value1".to_string()));

        // Create item2
        assert!(
            CrudHelper::create(&store, "item2".to_string(), "value2".to_string(), type_name)
                .is_ok()
        );
        assert_eq!(CrudHelper::get(&store, "item2"), Some("value2".to_string()));

        // Drop item1
        assert!(CrudHelper::drop(&store, "item1", false, type_name).is_ok());
        assert_eq!(CrudHelper::get(&store, "item1"), None);
        assert_eq!(CrudHelper::get(&store, "item2"), Some("value2".to_string())); // item2 still exists

        // Try to create item1 again
        assert!(
            CrudHelper::create(&store, "item1".to_string(), "new_value1".to_string(), type_name)
                .is_ok()
        );
        assert_eq!(CrudHelper::get(&store, "item1"), Some("new_value1".to_string()));
    }
}
