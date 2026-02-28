use crate::parser::ast::{ColumnDef, DataType, Expr, OrderByExpr};
use crate::transaction::{TransactionManager, TupleHeader};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone)]
pub struct Tuple {
    pub header: TupleHeader,
    pub data: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Value {
    Int(i64),
    Text(String),
}

pub struct Catalog {
    tables: Arc<RwLock<HashMap<String, TableSchema>>>,
    data: Arc<RwLock<HashMap<String, Vec<Tuple>>>>,
    txn_mgr: Arc<TransactionManager>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            data: Arc::new(RwLock::new(HashMap::new())),
            txn_mgr: Arc::new(TransactionManager::new()),
        }
    }
    
    pub fn create_table(&self, name: String, columns: Vec<ColumnDef>) -> Result<(), String> {
        let mut tables = self.tables.write().unwrap();
        
        if tables.contains_key(&name) {
            return Err(format!("Table '{}' already exists", name));
        }
        
        tables.insert(name.clone(), TableSchema { name: name.clone(), columns });
        
        let mut data = self.data.write().unwrap();
        data.insert(name, Vec::new());
        Ok(())
    }
    
    pub fn drop_table(&self, name: &str, if_exists: bool) -> Result<(), String> {
        let mut tables = self.tables.write().unwrap();
        
        if tables.remove(name).is_none() && !if_exists {
            return Err(format!("Table '{}' does not exist", name));
        }
        
        let mut data = self.data.write().unwrap();
        data.remove(name);
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
    
    pub fn insert(&self, table: &str, values: Vec<Expr>) -> Result<(), String> {
        let schema = self.get_table(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;
        
        if values.len() != schema.columns.len() {
            return Err(format!("Expected {} values, got {}", schema.columns.len(), values.len()));
        }
        
        let txn = self.txn_mgr.begin();
        let header = TupleHeader::new(txn.xid);
        
        let mut tuple_data = Vec::new();
        for (i, expr) in values.iter().enumerate() {
            let value = match expr {
                Expr::Number(n) => Value::Int(*n),
                Expr::String(s) => Value::Text(s.clone()),
                _ => return Err("Invalid value expression".to_string()),
            };
            
            match (&schema.columns[i].data_type, &value) {
                (DataType::Int, Value::Int(_)) => {},
                (DataType::Text, Value::Text(_)) => {},
                (DataType::Varchar(_), Value::Text(_)) => {},
                _ => return Err(format!("Type mismatch for column '{}'", schema.columns[i].name)),
            }
            
            tuple_data.push(value);
        }
        
        let tuple = Tuple { header, data: tuple_data };
        
        let mut data = self.data.write().unwrap();
        data.get_mut(table).unwrap().push(tuple);
        
        self.txn_mgr.commit(txn.xid).map_err(|e| e.to_string())?;
        Ok(())
    }
    
    pub fn row_count(&self, table: &str) -> usize {
        let data = self.data.read().unwrap();
        data.get(table).map(|rows| rows.len()).unwrap_or(0)
    }
    
    pub fn select(&self, table: &str, columns: Vec<String>, where_clause: Option<Expr>, order_by: Option<Vec<OrderByExpr>>) -> Result<Vec<Vec<Value>>, String> {
        let schema = self.get_table(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;
        
        let data = self.data.read().unwrap();
        let tuples = data.get(table).ok_or_else(|| format!("Table '{}' has no data", table))?;
        
        let snapshot = self.txn_mgr.get_snapshot();
        let mut results = Vec::new();
        
        for tuple in tuples {
            if tuple.header.is_visible(&snapshot, &self.txn_mgr) {
                // Evaluate WHERE clause
                if let Some(ref predicate) = where_clause {
                    if !self.evaluate_predicate(predicate, &tuple.data, &schema)? {
                        continue;
                    }
                }
                
                if columns.is_empty() || columns[0] == "*" {
                    results.push(tuple.data.clone());
                } else {
                    let mut row = Vec::new();
                    for col_name in &columns {
                        if let Some(idx) = schema.columns.iter().position(|c| &c.name == col_name) {
                            row.push(tuple.data[idx].clone());
                        } else {
                            return Err(format!("Column '{}' not found", col_name));
                        }
                    }
                    results.push(row);
                }
            }
        }
        
        // Apply ORDER BY if specified
        if let Some(order_by_exprs) = order_by {
            for order_expr in order_by_exprs.iter().rev() {
                let col_idx = schema.columns.iter().position(|c| c.name == order_expr.column)
                    .ok_or_else(|| format!("Column '{}' not found", order_expr.column))?;
                
                results.sort_by(|a, b| {
                    let cmp = a[col_idx].cmp(&b[col_idx]);
                    if order_expr.ascending { cmp } else { cmp.reverse() }
                });
            }
        }
        
        Ok(results)
    }
    
    fn evaluate_predicate(&self, expr: &Expr, tuple: &[Value], schema: &TableSchema) -> Result<bool, String> {
        use crate::parser::ast::BinaryOperator;
        
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_expr(left, tuple, schema)?;
                let right_val = self.evaluate_expr(right, tuple, schema)?;
                
                match op {
                    BinaryOperator::Equals => Ok(left_val == right_val),
                    BinaryOperator::NotEquals => Ok(left_val != right_val),
                    BinaryOperator::LessThan => match (&left_val, &right_val) {
                        (Value::Int(l), Value::Int(r)) => Ok(l < r),
                        (Value::Text(l), Value::Text(r)) => Ok(l < r),
                        _ => Err("Type mismatch in comparison".to_string()),
                    },
                    BinaryOperator::LessThanOrEqual => match (&left_val, &right_val) {
                        (Value::Int(l), Value::Int(r)) => Ok(l <= r),
                        (Value::Text(l), Value::Text(r)) => Ok(l <= r),
                        _ => Err("Type mismatch in comparison".to_string()),
                    },
                    BinaryOperator::GreaterThan => match (&left_val, &right_val) {
                        (Value::Int(l), Value::Int(r)) => Ok(l > r),
                        (Value::Text(l), Value::Text(r)) => Ok(l > r),
                        _ => Err("Type mismatch in comparison".to_string()),
                    },
                    BinaryOperator::GreaterThanOrEqual => match (&left_val, &right_val) {
                        (Value::Int(l), Value::Int(r)) => Ok(l >= r),
                        (Value::Text(l), Value::Text(r)) => Ok(l >= r),
                        _ => Err("Type mismatch in comparison".to_string()),
                    },
                }
            }
            _ => Err("Unsupported predicate expression".to_string()),
        }
    }
    
    fn evaluate_expr(&self, expr: &Expr, tuple: &[Value], schema: &TableSchema) -> Result<Value, String> {
        match expr {
            Expr::Column(name) => {
                let idx = schema.columns.iter().position(|c| &c.name == name)
                    .ok_or_else(|| format!("Column '{}' not found", name))?;
                Ok(tuple[idx].clone())
            }
            Expr::Number(n) => Ok(Value::Int(*n)),
            Expr::String(s) => Ok(Value::Text(s.clone())),
            _ => Err("Unsupported expression".to_string()),
        }
    }
    
    pub fn update(&self, table: &str, assignments: Vec<(String, Expr)>, where_clause: Option<Expr>) -> Result<usize, String> {
        let schema = self.get_table(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;
        
        let txn = self.txn_mgr.begin();
        let snapshot = txn.snapshot.clone();
        
        let mut data = self.data.write().unwrap();
        let tuples = data.get_mut(table).ok_or_else(|| format!("Table '{}' has no data", table))?;
        
        let mut updated = 0;
        for tuple in tuples.iter_mut() {
            if tuple.header.is_visible(&snapshot, &self.txn_mgr) {
                // Evaluate WHERE clause
                if let Some(ref predicate) = where_clause {
                    if !self.evaluate_predicate(predicate, &tuple.data, &schema)? {
                        continue;
                    }
                }
                
                for (col_name, expr) in &assignments {
                    let idx = schema.columns.iter().position(|c| &c.name == col_name)
                        .ok_or_else(|| format!("Column '{}' not found", col_name))?;
                    
                    let value = match expr {
                        Expr::Number(n) => Value::Int(*n),
                        Expr::String(s) => Value::Text(s.clone()),
                        _ => return Err("Invalid value expression".to_string()),
                    };
                    
                    match (&schema.columns[idx].data_type, &value) {
                        (DataType::Int, Value::Int(_)) => {},
                        (DataType::Text, Value::Text(_)) => {},
                        (DataType::Varchar(_), Value::Text(_)) => {},
                        _ => return Err(format!("Type mismatch for column '{}'", col_name)),
                    }
                    
                    tuple.data[idx] = value;
                }
                updated += 1;
            }
        }
        
        self.txn_mgr.commit(txn.xid).map_err(|e| e.to_string())?;
        Ok(updated)
    }
    
    pub fn delete(&self, table: &str, where_clause: Option<Expr>) -> Result<usize, String> {
        let schema = self.get_table(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;
        
        let txn = self.txn_mgr.begin();
        let snapshot = txn.snapshot.clone();
        
        let mut data = self.data.write().unwrap();
        let tuples = data.get_mut(table).ok_or_else(|| format!("Table '{}' has no data", table))?;
        
        let mut deleted = 0;
        for tuple in tuples.iter_mut() {
            if tuple.header.is_visible(&snapshot, &self.txn_mgr) {
                // Evaluate WHERE clause
                if let Some(ref predicate) = where_clause {
                    if !self.evaluate_predicate(predicate, &tuple.data, &schema)? {
                        continue;
                    }
                }
                
                tuple.header.delete(txn.xid);
                deleted += 1;
            }
        }
        
        self.txn_mgr.commit(txn.xid).map_err(|e| e.to_string())?;
        Ok(deleted)
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
    
    #[test]
    fn test_insert() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
            ColumnDef { name: "name".to_string(), data_type: DataType::Text },
        ];
        
        catalog.create_table("users".to_string(), columns).unwrap();
        
        let values = vec![Expr::Number(1), Expr::String("Alice".to_string())];
        assert!(catalog.insert("users", values).is_ok());
        assert_eq!(catalog.row_count("users"), 1);
    }
    
    #[test]
    fn test_insert_wrong_column_count() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("users".to_string(), columns).unwrap();
        
        let values = vec![Expr::Number(1), Expr::String("Alice".to_string())];
        assert!(catalog.insert("users", values).is_err());
    }
    
    #[test]
    fn test_insert_type_mismatch() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("users".to_string(), columns).unwrap();
        
        let values = vec![Expr::String("not a number".to_string())];
        assert!(catalog.insert("users", values).is_err());
    }
    
    #[test]
    fn test_insert_multiple_rows() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
            ColumnDef { name: "name".to_string(), data_type: DataType::Text },
        ];
        
        catalog.create_table("users".to_string(), columns).unwrap();
        
        catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
        catalog.insert("users", vec![Expr::Number(2), Expr::String("Bob".to_string())]).unwrap();
        catalog.insert("users", vec![Expr::Number(3), Expr::String("Charlie".to_string())]).unwrap();
        
        assert_eq!(catalog.row_count("users"), 3);
    }
    
    #[test]
    fn test_select_all() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
            ColumnDef { name: "name".to_string(), data_type: DataType::Text },
        ];
        
        catalog.create_table("users".to_string(), columns).unwrap();
        catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
        catalog.insert("users", vec![Expr::Number(2), Expr::String("Bob".to_string())]).unwrap();
        
        let rows = catalog.select("users", vec!["*".to_string()], None, None).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].len(), 2);
    }
    
    #[test]
    fn test_select_specific_columns() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
            ColumnDef { name: "name".to_string(), data_type: DataType::Text },
        ];
        
        catalog.create_table("users".to_string(), columns).unwrap();
        catalog.insert("users", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
        
        let rows = catalog.select("users", vec!["id".to_string()], None, None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 1);
    }
    
    #[test]
    fn test_select_nonexistent_table() {
        let catalog = Catalog::new();
        let result = catalog.select("nonexistent", vec!["*".to_string()], None, None);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_select_empty_table() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("empty".to_string(), columns).unwrap();
        let rows = catalog.select("empty", vec!["*".to_string()], None, None).unwrap();
        assert_eq!(rows.len(), 0);
    }
    
    #[test]
    fn test_select_with_where() {
        use crate::parser::ast::BinaryOperator;
        
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
            ColumnDef { name: "value".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(1), Expr::Number(100)]).unwrap();
        catalog.insert("data", vec![Expr::Number(2), Expr::Number(200)]).unwrap();
        catalog.insert("data", vec![Expr::Number(3), Expr::Number(300)]).unwrap();
        
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(2)),
        });
        
        let rows = catalog.select("data", vec!["*".to_string()], where_clause, None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int(2));
        assert_eq!(rows[0][1], Value::Int(200));
    }
    
    #[test]
    fn test_select_with_not_equals() {
        use crate::parser::ast::BinaryOperator;
        
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(1)]).unwrap();
        catalog.insert("data", vec![Expr::Number(2)]).unwrap();
        catalog.insert("data", vec![Expr::Number(3)]).unwrap();
        
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::NotEquals,
            right: Box::new(Expr::Number(2)),
        });
        
        let rows = catalog.select("data", vec!["*".to_string()], where_clause, None).unwrap();
        assert_eq!(rows.len(), 2);
    }
    
    #[test]
    fn test_select_with_less_than() {
        use crate::parser::ast::BinaryOperator;
        
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "value".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(10)]).unwrap();
        catalog.insert("data", vec![Expr::Number(20)]).unwrap();
        catalog.insert("data", vec![Expr::Number(30)]).unwrap();
        
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::LessThan,
            right: Box::new(Expr::Number(25)),
        });
        
        let rows = catalog.select("data", vec!["*".to_string()], where_clause, None).unwrap();
        assert_eq!(rows.len(), 2);
    }
    
    #[test]
    fn test_select_with_greater_than() {
        use crate::parser::ast::BinaryOperator;
        
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "value".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(10)]).unwrap();
        catalog.insert("data", vec![Expr::Number(20)]).unwrap();
        catalog.insert("data", vec![Expr::Number(30)]).unwrap();
        
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expr::Number(15)),
        });
        
        let rows = catalog.select("data", vec!["*".to_string()], where_clause, None).unwrap();
        assert_eq!(rows.len(), 2);
    }
    
    #[test]
    fn test_select_with_less_than_or_equal() {
        use crate::parser::ast::BinaryOperator;
        
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "value".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(10)]).unwrap();
        catalog.insert("data", vec![Expr::Number(20)]).unwrap();
        catalog.insert("data", vec![Expr::Number(30)]).unwrap();
        
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::LessThanOrEqual,
            right: Box::new(Expr::Number(20)),
        });
        
        let rows = catalog.select("data", vec!["*".to_string()], where_clause, None).unwrap();
        assert_eq!(rows.len(), 2);
    }
    
    #[test]
    fn test_select_with_greater_than_or_equal() {
        use crate::parser::ast::BinaryOperator;
        
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "value".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(10)]).unwrap();
        catalog.insert("data", vec![Expr::Number(20)]).unwrap();
        catalog.insert("data", vec![Expr::Number(30)]).unwrap();
        
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("value".to_string())),
            op: BinaryOperator::GreaterThanOrEqual,
            right: Box::new(Expr::Number(20)),
        });
        
        let rows = catalog.select("data", vec!["*".to_string()], where_clause, None).unwrap();
        assert_eq!(rows.len(), 2);
    }
    
    #[test]
    fn test_update() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
            ColumnDef { name: "value".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(1), Expr::Number(100)]).unwrap();
        catalog.insert("data", vec![Expr::Number(2), Expr::Number(200)]).unwrap();
        
        let updated = catalog.update("data", vec![("value".to_string(), Expr::Number(999))], None).unwrap();
        assert_eq!(updated, 2);
    }
    
    #[test]
    fn test_update_nonexistent_table() {
        let catalog = Catalog::new();
        let result = catalog.update("nonexistent", vec![("col".to_string(), Expr::Number(1))], None);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_update_with_where() {
        use crate::parser::ast::BinaryOperator;
        
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
            ColumnDef { name: "value".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(1), Expr::Number(100)]).unwrap();
        catalog.insert("data", vec![Expr::Number(2), Expr::Number(200)]).unwrap();
        
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(1)),
        });
        
        let updated = catalog.update("data", vec![("value".to_string(), Expr::Number(999))], where_clause).unwrap();
        assert_eq!(updated, 1);
    }
    
    #[test]
    fn test_delete() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(1)]).unwrap();
        catalog.insert("data", vec![Expr::Number(2)]).unwrap();
        catalog.insert("data", vec![Expr::Number(3)]).unwrap();
        
        let deleted = catalog.delete("data", None).unwrap();
        assert_eq!(deleted, 3);
    }
    
    #[test]
    fn test_delete_empty_table() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("empty".to_string(), columns).unwrap();
        let deleted = catalog.delete("empty", None).unwrap();
        assert_eq!(deleted, 0);
    }
    
    #[test]
    fn test_delete_with_where() {
        use crate::parser::ast::BinaryOperator;
        
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(1)]).unwrap();
        catalog.insert("data", vec![Expr::Number(2)]).unwrap();
        catalog.insert("data", vec![Expr::Number(3)]).unwrap();
        
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(2)),
        });
        
        let deleted = catalog.delete("data", where_clause).unwrap();
        assert_eq!(deleted, 1);
    }
    
    #[test]
    fn test_select_with_order_by_asc() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
            ColumnDef { name: "value".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(3), Expr::Number(300)]).unwrap();
        catalog.insert("data", vec![Expr::Number(1), Expr::Number(100)]).unwrap();
        catalog.insert("data", vec![Expr::Number(2), Expr::Number(200)]).unwrap();
        
        let order_by = Some(vec![OrderByExpr { column: "id".to_string(), ascending: true }]);
        let rows = catalog.select("data", vec!["*".to_string()], None, order_by).unwrap();
        
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::Int(1));
        assert_eq!(rows[1][0], Value::Int(2));
        assert_eq!(rows[2][0], Value::Int(3));
    }
    
    #[test]
    fn test_select_with_order_by_desc() {
        let catalog = Catalog::new();
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
        ];
        
        catalog.create_table("data".to_string(), columns).unwrap();
        catalog.insert("data", vec![Expr::Number(1)]).unwrap();
        catalog.insert("data", vec![Expr::Number(3)]).unwrap();
        catalog.insert("data", vec![Expr::Number(2)]).unwrap();
        
        let order_by = Some(vec![OrderByExpr { column: "id".to_string(), ascending: false }]);
        let rows = catalog.select("data", vec!["*".to_string()], None, order_by).unwrap();
        
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::Int(3));
        assert_eq!(rows[1][0], Value::Int(2));
        assert_eq!(rows[2][0], Value::Int(1));
    }
}
