use crate::parser::ast::Statement;
use dashmap::DashMap;
use std::sync::Arc;

pub struct PreparedStatementManager {
    statements: Arc<DashMap<String, Statement>>,
}

impl PreparedStatementManager {
    pub fn new() -> Self {
        Self { statements: Arc::new(DashMap::new()) }
    }

    pub fn prepare(&self, name: String, statement: Statement) {
        self.statements.insert(name, statement);
    }

    pub fn get(&self, name: &str) -> Option<Statement> {
        self.statements.get(name).map(|s| s.clone())
    }

    pub fn deallocate(&self, name: &str) -> bool {
        self.statements.remove(name).is_some()
    }

    pub fn clear(&self) {
        self.statements.clear();
    }
}

impl Default for PreparedStatementManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{Expr, SelectStmt};

    fn create_test_stmt(table: &str) -> Statement {
        Statement::Select(SelectStmt {
            distinct: false,
            columns: vec![Expr::Star],
            from: table.to_string(),
            table_alias: None,
            joins: vec![],
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        })
    }

    #[test]
    fn test_prepare_and_get() {
        let manager = PreparedStatementManager::new();
        let stmt = create_test_stmt("users");
        manager.prepare("test".to_string(), stmt.clone());
        let retrieved = manager.get("test");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), stmt);
    }

    #[test]
    fn test_get_nonexistent() {
        let manager = PreparedStatementManager::new();
        assert!(manager.get("nonexistent").is_none());
    }

    #[test]
    fn test_deallocate() {
        let manager = PreparedStatementManager::new();
        let stmt = create_test_stmt("users");
        manager.prepare("test".to_string(), stmt);
        assert!(manager.deallocate("test"));
        assert!(manager.get("test").is_none());
    }

    #[test]
    fn test_deallocate_nonexistent() {
        let manager = PreparedStatementManager::new();
        assert!(!manager.deallocate("nonexistent"));
    }

    #[test]
    fn test_clear() {
        let manager = PreparedStatementManager::new();
        let stmt = create_test_stmt("users");
        manager.prepare("test1".to_string(), stmt.clone());
        manager.prepare("test2".to_string(), stmt);
        manager.clear();
        assert!(manager.get("test1").is_none());
        assert!(manager.get("test2").is_none());
    }

    #[test]
    fn test_multiple_statements() {
        let manager = PreparedStatementManager::new();
        let stmt1 = create_test_stmt("users");
        let stmt2 = create_test_stmt("orders");
        manager.prepare("stmt1".to_string(), stmt1.clone());
        manager.prepare("stmt2".to_string(), stmt2.clone());
        assert_eq!(manager.get("stmt1").unwrap(), stmt1);
        assert_eq!(manager.get("stmt2").unwrap(), stmt2);
    }
}
