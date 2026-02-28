use crate::parser::ast::{ColumnDef, DataType, Expr, OrderByExpr, AggregateFunc};
use crate::transaction::{TransactionManager, TupleHeader};
use super::{Value, TableSchema, Tuple};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

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
    
    pub fn select(&self, table: &str, columns: Vec<String>, where_clause: Option<Expr>, group_by: Option<Vec<String>>, having: Option<Expr>, order_by: Option<Vec<OrderByExpr>>, limit: Option<usize>, offset: Option<usize>) -> Result<Vec<Vec<Value>>, String> {
        let schema = self.get_table(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;
        
        // Check if this is an aggregate query
        if columns.len() == 1 && columns[0].starts_with("AGG:") {
            return self.execute_aggregate(table, &columns[0], where_clause);
        }
        
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
        
        // Apply GROUP BY if specified
        if let Some(group_cols) = group_by {
            results = self.apply_group_by(results, &group_cols, &columns, &schema)?;
            
            // Apply HAVING if specified
            if let Some(having_expr) = having {
                results.retain(|row| {
                    self.evaluate_having(&having_expr, row).unwrap_or(false)
                });
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
        
        // Apply OFFSET and LIMIT
        let start = offset.unwrap_or(0);
        let end = limit.map(|l| start + l).unwrap_or(results.len());
        results = results.into_iter().skip(start).take(end - start).collect();
        
        Ok(results)
    }
    
    
    fn apply_group_by(&self, rows: Vec<Vec<Value>>, group_cols: &[String], select_cols: &[String], schema: &TableSchema) -> Result<Vec<Vec<Value>>, String> {
        use std::collections::HashMap;
        
        let mut groups: HashMap<Vec<Value>, Vec<Vec<Value>>> = HashMap::new();
        
        for row in rows {
            let mut key = Vec::new();
            for col_name in group_cols {
                let idx = schema.columns.iter().position(|c| &c.name == col_name)
                    .ok_or_else(|| format!("Column '{}' not found", col_name))?;
                key.push(row[idx].clone());
            }
            groups.entry(key).or_insert_with(Vec::new).push(row);
        }
        
        let mut result = Vec::new();
        for (key, group_rows) in groups {
            let mut row = Vec::new();
            for col_name in select_cols {
                if group_cols.contains(col_name) {
                    let idx = group_cols.iter().position(|c| c == col_name).unwrap();
                    row.push(key[idx].clone());
                } else {
                    row.push(Value::Int(group_rows.len() as i64));
                }
            }
            result.push(row);
        }
        
        Ok(result)
    }
    
    
    fn evaluate_having(&self, expr: &Expr, row: &[Value]) -> Result<bool, String> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left_val = match **left {
                    Expr::Number(n) => Value::Int(n),
                    _ => row.get(0).cloned().unwrap_or(Value::Int(0)),
                };
                let right_val = match **right {
                    Expr::Number(n) => Value::Int(n),
                    _ => Value::Int(0),
                };
                
                use crate::parser::ast::BinaryOperator;
                match op {
                    BinaryOperator::GreaterThan => Ok(left_val > right_val),
                    BinaryOperator::GreaterThanOrEqual => Ok(left_val >= right_val),
                    BinaryOperator::LessThan => Ok(left_val < right_val),
                    BinaryOperator::LessThanOrEqual => Ok(left_val <= right_val),
                    BinaryOperator::Equals => Ok(left_val == right_val),
                    BinaryOperator::NotEquals => Ok(left_val != right_val),
                    _ => Ok(false),
                }
            }
            _ => Ok(false),
        }
    }
    
    fn evaluate_predicate(&self, expr: &Expr, tuple: &[Value], schema: &TableSchema) -> Result<bool, String> {
        use crate::parser::ast::BinaryOperator;
        
        match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::And => {
                        let left_result = self.evaluate_predicate(left, tuple, schema)?;
                        let right_result = self.evaluate_predicate(right, tuple, schema)?;
                        Ok(left_result && right_result)
                    },
                    BinaryOperator::Or => {
                        let left_result = self.evaluate_predicate(left, tuple, schema)?;
                        let right_result = self.evaluate_predicate(right, tuple, schema)?;
                        Ok(left_result || right_result)
                    },
                    _ => {
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
                            _ => unreachable!(),
                        }
                    }
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
    
    fn execute_aggregate(&self, table: &str, agg_spec: &str, where_clause: Option<Expr>) -> Result<Vec<Vec<Value>>, String> {
        let schema = self.get_table(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;
        
        let data = self.data.read().unwrap();
        let tuples = data.get(table).ok_or_else(|| format!("Table '{}' has no data", table))?;
        
        let snapshot = self.txn_mgr.get_snapshot();
        
        // Parse aggregate spec: "AGG:FUNC:COLUMN"
        let parts: Vec<&str> = agg_spec.split(':').collect();
        if parts.len() < 2 {
            return Err("Invalid aggregate specification".to_string());
        }
        
        let func = parts[1];
        let col_name = if parts.len() > 2 { Some(parts[2]) } else { None };
        
        let mut values = Vec::new();
        for tuple in tuples {
            if tuple.header.is_visible(&snapshot, &self.txn_mgr) {
                if let Some(ref predicate) = where_clause {
                    if !self.evaluate_predicate(predicate, &tuple.data, &schema)? {
                        continue;
                    }
                }
                
                if func == "COUNT" {
                    values.push(Value::Int(1));
                } else if let Some(col) = col_name {
                    let idx = schema.columns.iter().position(|c| c.name == col)
                        .ok_or_else(|| format!("Column '{}' not found", col))?;
                    values.push(tuple.data[idx].clone());
                }
            }
        }
        
        let result = match func {
            "COUNT" => Value::Int(values.len() as i64),
            "SUM" => {
                let sum: i64 = values.iter().filter_map(|v| {
                    if let Value::Int(n) = v { Some(*n) } else { None }
                }).sum();
                Value::Int(sum)
            }
            "AVG" => {
                let nums: Vec<i64> = values.iter().filter_map(|v| {
                    if let Value::Int(n) = v { Some(*n) } else { None }
                }).collect();
                if nums.is_empty() {
                    Value::Int(0)
                } else {
                    Value::Int(nums.iter().sum::<i64>() / nums.len() as i64)
                }
            }
            "MIN" => {
                values.iter().min().cloned().unwrap_or(Value::Int(0))
            }
            "MAX" => {
                values.iter().max().cloned().unwrap_or(Value::Int(0))
            }
            _ => return Err(format!("Unknown aggregate function: {}", func)),
        };
        
        Ok(vec![vec![result]])
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}
