use super::operators::executor::{Executor, ExecutorError, Tuple};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct CursorManager {
    cursors: Arc<Mutex<HashMap<String, Cursor>>>,
}

struct Cursor {
    tuples: Vec<Tuple>,
    position: usize,
}

impl CursorManager {
    pub fn new() -> Self {
        Self { cursors: Arc::new(Mutex::new(HashMap::new())) }
    }

    pub fn declare(
        &self,
        name: String,
        mut executor: Box<dyn Executor>,
    ) -> Result<(), ExecutorError> {
        let mut tuples = Vec::new();
        while let Some(tuple) = executor.next()? {
            tuples.push(tuple);
        }
        let cursor = Cursor { tuples, position: 0 };
        self.cursors.lock().unwrap().insert(name, cursor);
        Ok(())
    }

    pub fn fetch_next(&self, name: &str) -> Result<Option<Tuple>, ExecutorError> {
        let mut cursors = self.cursors.lock().unwrap();
        let cursor = cursors
            .get_mut(name)
            .ok_or_else(|| ExecutorError::InvalidInput(format!("Cursor {} not found", name)))?;

        if cursor.position < cursor.tuples.len() {
            let tuple = cursor.tuples[cursor.position].clone();
            cursor.position += 1;
            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }

    pub fn fetch_prior(&self, name: &str) -> Result<Option<Tuple>, ExecutorError> {
        let mut cursors = self.cursors.lock().unwrap();
        let cursor = cursors
            .get_mut(name)
            .ok_or_else(|| ExecutorError::InvalidInput(format!("Cursor {} not found", name)))?;

        if cursor.position > 0 {
            cursor.position -= 1;
            Ok(Some(cursor.tuples[cursor.position].clone()))
        } else {
            Ok(None)
        }
    }

    pub fn fetch_first(&self, name: &str) -> Result<Option<Tuple>, ExecutorError> {
        let mut cursors = self.cursors.lock().unwrap();
        let cursor = cursors
            .get_mut(name)
            .ok_or_else(|| ExecutorError::InvalidInput(format!("Cursor {} not found", name)))?;

        if !cursor.tuples.is_empty() {
            cursor.position = 1;
            Ok(Some(cursor.tuples[0].clone()))
        } else {
            Ok(None)
        }
    }

    pub fn fetch_last(&self, name: &str) -> Result<Option<Tuple>, ExecutorError> {
        let mut cursors = self.cursors.lock().unwrap();
        let cursor = cursors
            .get_mut(name)
            .ok_or_else(|| ExecutorError::InvalidInput(format!("Cursor {} not found", name)))?;

        if !cursor.tuples.is_empty() {
            cursor.position = cursor.tuples.len();
            Ok(Some(cursor.tuples[cursor.tuples.len() - 1].clone()))
        } else {
            Ok(None)
        }
    }

    pub fn fetch_absolute(&self, name: &str, pos: i64) -> Result<Option<Tuple>, ExecutorError> {
        let mut cursors = self.cursors.lock().unwrap();
        let cursor = cursors
            .get_mut(name)
            .ok_or_else(|| ExecutorError::InvalidInput(format!("Cursor {} not found", name)))?;

        let idx = if pos >= 0 { pos as usize } else { (cursor.tuples.len() as i64 + pos) as usize };

        if idx < cursor.tuples.len() {
            cursor.position = idx + 1;
            Ok(Some(cursor.tuples[idx].clone()))
        } else {
            Ok(None)
        }
    }

    pub fn fetch_relative(&self, name: &str, offset: i64) -> Result<Option<Tuple>, ExecutorError> {
        let mut cursors = self.cursors.lock().unwrap();
        let cursor = cursors
            .get_mut(name)
            .ok_or_else(|| ExecutorError::InvalidInput(format!("Cursor {} not found", name)))?;

        let new_pos = (cursor.position as i64 + offset) as usize;
        if new_pos < cursor.tuples.len() {
            cursor.position = new_pos + 1;
            Ok(Some(cursor.tuples[new_pos].clone()))
        } else {
            Ok(None)
        }
    }

    pub fn close(&self, name: &str) -> Result<(), ExecutorError> {
        self.cursors.lock().unwrap().remove(name);
        Ok(())
    }
}

impl Default for CursorManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Value;
    use crate::executor::MockTupleExecutor;

    fn make_tuple(val: i64) -> Tuple {
        let mut map = HashMap::new();
        map.insert("col".to_string(), Value::Int(val));
        map
    }

    #[test]
    fn test_declare_cursor() {
        let manager = CursorManager::new();
        let tuples = vec![make_tuple(1), make_tuple(2), make_tuple(3)];
        let executor = Box::new(MockTupleExecutor::new(tuples));
        assert!(manager.declare("test_cursor".to_string(), executor).is_ok());
    }

    #[test]
    fn test_fetch_next() {
        let manager = CursorManager::new();
        let tuples = vec![make_tuple(1), make_tuple(2), make_tuple(3)];
        let executor = Box::new(MockTupleExecutor::new(tuples.clone()));
        manager.declare("test_cursor".to_string(), executor).unwrap();

        let result = manager.fetch_next("test_cursor").unwrap();
        assert_eq!(result, Some(tuples[0].clone()));

        let result = manager.fetch_next("test_cursor").unwrap();
        assert_eq!(result, Some(tuples[1].clone()));
    }

    #[test]
    fn test_fetch_prior() {
        let manager = CursorManager::new();
        let tuples = vec![make_tuple(1), make_tuple(2), make_tuple(3)];
        let executor = Box::new(MockTupleExecutor::new(tuples.clone()));
        manager.declare("test_cursor".to_string(), executor).unwrap();

        manager.fetch_next("test_cursor").unwrap(); // position = 1
        manager.fetch_next("test_cursor").unwrap(); // position = 2

        let result = manager.fetch_prior("test_cursor").unwrap(); // position = 1, returns tuples[1]
        assert_eq!(result, Some(tuples[1].clone()));
    }

    #[test]
    fn test_fetch_first() {
        let manager = CursorManager::new();
        let tuples = vec![make_tuple(1), make_tuple(2), make_tuple(3)];
        let executor = Box::new(MockTupleExecutor::new(tuples.clone()));
        manager.declare("test_cursor".to_string(), executor).unwrap();

        let result = manager.fetch_first("test_cursor").unwrap();
        assert_eq!(result, Some(tuples[0].clone()));
    }

    #[test]
    fn test_fetch_last() {
        let manager = CursorManager::new();
        let tuples = vec![make_tuple(1), make_tuple(2), make_tuple(3)];
        let executor = Box::new(MockTupleExecutor::new(tuples.clone()));
        manager.declare("test_cursor".to_string(), executor).unwrap();

        let result = manager.fetch_last("test_cursor").unwrap();
        assert_eq!(result, Some(tuples[2].clone()));
    }

    #[test]
    fn test_fetch_absolute() {
        let manager = CursorManager::new();
        let tuples = vec![make_tuple(1), make_tuple(2), make_tuple(3)];
        let executor = Box::new(MockTupleExecutor::new(tuples.clone()));
        manager.declare("test_cursor".to_string(), executor).unwrap();

        let result = manager.fetch_absolute("test_cursor", 1).unwrap();
        assert_eq!(result, Some(tuples[1].clone()));

        let result = manager.fetch_absolute("test_cursor", -1).unwrap();
        assert_eq!(result, Some(tuples[2].clone()));
    }

    #[test]
    fn test_fetch_relative() {
        let manager = CursorManager::new();
        let tuples = vec![make_tuple(1), make_tuple(2), make_tuple(3)];
        let executor = Box::new(MockTupleExecutor::new(tuples.clone()));
        manager.declare("test_cursor".to_string(), executor).unwrap();

        manager.fetch_next("test_cursor").unwrap();
        let result = manager.fetch_relative("test_cursor", 1).unwrap();
        assert_eq!(result, Some(tuples[2].clone()));
    }

    #[test]
    fn test_close_cursor() {
        let manager = CursorManager::new();
        let tuples = vec![make_tuple(1)];
        let executor = Box::new(MockTupleExecutor::new(tuples));
        manager.declare("test_cursor".to_string(), executor).unwrap();

        assert!(manager.close("test_cursor").is_ok());
        assert!(manager.fetch_next("test_cursor").is_err());
    }

    #[test]
    fn test_fetch_nonexistent_cursor() {
        let manager = CursorManager::new();
        assert!(manager.fetch_next("nonexistent").is_err());
    }

    #[test]
    fn test_fetch_beyond_end() {
        let manager = CursorManager::new();
        let tuples = vec![make_tuple(1)];
        let executor = Box::new(MockTupleExecutor::new(tuples));
        manager.declare("test_cursor".to_string(), executor).unwrap();

        manager.fetch_next("test_cursor").unwrap();
        let result = manager.fetch_next("test_cursor").unwrap();
        assert_eq!(result, None);
    }
}
