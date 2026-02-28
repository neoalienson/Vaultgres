use crate::parser::ast::{ColumnDef, DataType, Expr, OrderByExpr, SelectStmt, CreateTriggerStmt};
use crate::transaction::{TransactionManager, TupleHeader};
use super::{Value, TableSchema, Tuple};
use super::predicate::PredicateEvaluator;
use super::aggregation::Aggregator;
use super::persistence::Persistence;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct Catalog {
    tables: Arc<RwLock<HashMap<String, TableSchema>>>,
    views: Arc<RwLock<HashMap<String, SelectStmt>>>,
    materialized_views: Arc<RwLock<HashMap<String, (SelectStmt, Vec<Vec<Value>>)>>>,
    triggers: Arc<RwLock<HashMap<String, CreateTriggerStmt>>>,
    data: Arc<RwLock<HashMap<String, Vec<Tuple>>>>,
    txn_mgr: Arc<TransactionManager>,
    data_dir: Option<String>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            views: Arc::new(RwLock::new(HashMap::new())),
            materialized_views: Arc::new(RwLock::new(HashMap::new())),
            triggers: Arc::new(RwLock::new(HashMap::new())),
            data: Arc::new(RwLock::new(HashMap::new())),
            txn_mgr: Arc::new(TransactionManager::new()),
            data_dir: None,
        }
    }
    
    pub fn new_with_data_dir(data_dir: &str) -> Self {
        let catalog = Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            views: Arc::new(RwLock::new(HashMap::new())),
            materialized_views: Arc::new(RwLock::new(HashMap::new())),
            triggers: Arc::new(RwLock::new(HashMap::new())),
            data: Arc::new(RwLock::new(HashMap::new())),
            txn_mgr: Arc::new(TransactionManager::new()),
            data_dir: Some(data_dir.to_string()),
        };
        
        let mut tables = catalog.tables.write().unwrap();
        let mut data = catalog.data.write().unwrap();
        if let Err(e) = Persistence::load(data_dir, &mut tables, &mut data, &catalog.txn_mgr) {
            log::error!("Failed to load catalog: {}", e);
        }
        drop(tables);
        drop(data);
        
        if let Ok(views) = Persistence::load_views(data_dir) {
            *catalog.views.write().unwrap() = views;
        }
        
        if let Ok(triggers) = Persistence::load_triggers(data_dir) {
            *catalog.triggers.write().unwrap() = triggers;
        }
        
        catalog
    }
    
    fn auto_save(&self) {
        if let Some(ref dir) = self.data_dir {
            let tables_clone = self.tables.read().unwrap().clone();
            let data_clone = self.data.read().unwrap().clone();
            
            if let Err(e) = Persistence::save(dir, &tables_clone, &data_clone) {
                log::error!("Auto-save failed: {}", e);
            }
            
            let views_clone = self.views.read().unwrap().clone();
            if let Err(e) = Persistence::save_views(dir, &views_clone) {
                log::error!("Views auto-save failed: {}", e);
            }
            
            let triggers_clone = self.triggers.read().unwrap().clone();
            if let Err(e) = Persistence::save_triggers(dir, &triggers_clone) {
                log::error!("Triggers auto-save failed: {}", e);
            }
        }
    }
    
    pub fn create_table(&self, name: String, columns: Vec<ColumnDef>) -> Result<(), String> {
        let mut tables = self.tables.write().unwrap();
        
        if tables.contains_key(&name) {
            return Err(format!("Table '{}' already exists", name));
        }
        
        tables.insert(name.clone(), TableSchema { name: name.clone(), columns });
        drop(tables);
        
        let mut data = self.data.write().unwrap();
        data.insert(name, Vec::new());
        drop(data);
        
        self.auto_save();
        Ok(())
    }
    
    pub fn drop_table(&self, name: &str, if_exists: bool) -> Result<(), String> {
        let mut tables = self.tables.write().unwrap();
        
        if tables.remove(name).is_none() && !if_exists {
            return Err(format!("Table '{}' does not exist", name));
        }
        drop(tables);
        
        let mut data = self.data.write().unwrap();
        data.remove(name);
        drop(data);
        
        self.auto_save();
        Ok(())
    }
    
    pub fn get_table(&self, name: &str) -> Option<TableSchema> {
        let tables = self.tables.read().unwrap();
        tables.get(name).cloned()
    }
    
    pub fn create_view(&self, name: String, query: SelectStmt) -> Result<(), String> {
        let mut views = self.views.write().unwrap();
        
        if views.contains_key(&name) {
            return Err(format!("View '{}' already exists", name));
        }
        
        views.insert(name, query);
        drop(views);
        self.auto_save();
        Ok(())
    }
    
    pub fn drop_view(&self, name: &str, if_exists: bool) -> Result<(), String> {
        let mut views = self.views.write().unwrap();
        
        if views.remove(name).is_none() && !if_exists {
            return Err(format!("View '{}' does not exist", name));
        }
        drop(views);
        self.auto_save();
        Ok(())
    }
    
    pub fn get_view(&self, name: &str) -> Option<SelectStmt> {
        let views = self.views.read().unwrap();
        views.get(name).cloned()
    }
    
    pub fn create_materialized_view(&self, name: String, query: SelectStmt) -> Result<(), String> {
        let mut mvs = self.materialized_views.write().unwrap();
        
        if mvs.contains_key(&name) {
            return Err(format!("Materialized view '{}' already exists", name));
        }
        
        mvs.insert(name, (query, Vec::new()));
        Ok(())
    }
    
    pub fn refresh_materialized_view(&self, name: &str) -> Result<(), String> {
        let mut mvs = self.materialized_views.write().unwrap();
        
        let (query, data) = mvs.get_mut(name)
            .ok_or_else(|| format!("Materialized view '{}' does not exist", name))?;
        
        data.clear();
        Ok(())
    }
    
    pub fn drop_materialized_view(&self, name: &str, if_exists: bool) -> Result<(), String> {
        let mut mvs = self.materialized_views.write().unwrap();
        
        if mvs.remove(name).is_none() && !if_exists {
            return Err(format!("Materialized view '{}' does not exist", name));
        }
        
        Ok(())
    }
    
    pub fn get_materialized_view(&self, name: &str) -> Option<Vec<Vec<Value>>> {
        let mvs = self.materialized_views.read().unwrap();
        mvs.get(name).map(|(_, data)| data.clone())
    }
    
    pub fn create_trigger(&self, trigger: CreateTriggerStmt) -> Result<(), String> {
        let mut triggers = self.triggers.write().unwrap();
        
        if triggers.contains_key(&trigger.name) {
            return Err(format!("Trigger '{}' already exists", trigger.name));
        }
        
        triggers.insert(trigger.name.clone(), trigger);
        drop(triggers);
        self.auto_save();
        Ok(())
    }
    
    pub fn drop_trigger(&self, name: &str, if_exists: bool) -> Result<(), String> {
        let mut triggers = self.triggers.write().unwrap();
        
        if triggers.remove(name).is_none() && !if_exists {
            return Err(format!("Trigger '{}' does not exist", name));
        }
        drop(triggers);
        self.auto_save();
        Ok(())
    }
    
    pub fn get_trigger(&self, name: &str) -> Option<CreateTriggerStmt> {
        let triggers = self.triggers.read().unwrap();
        triggers.get(name).cloned()
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
        drop(data);
        
        self.txn_mgr.commit(txn.xid).map_err(|e| e.to_string())?;
        self.auto_save();
        Ok(())
    }
    
    pub fn row_count(&self, table: &str) -> usize {
        let data = self.data.read().unwrap();
        data.get(table).map(|rows| rows.len()).unwrap_or(0)
    }
    
    pub fn select(
        &self,
        table: &str,
        distinct: bool,
        columns: Vec<String>,
        where_clause: Option<Expr>,
        group_by: Option<Vec<String>>,
        having: Option<Expr>,
        order_by: Option<Vec<OrderByExpr>>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Vec<Vec<Value>>, String> {
        let schema = self.get_table(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;
        
        if columns.len() == 1 && columns[0].starts_with("AGG:") {
            let data = self.data.read().unwrap();
            let tuples = data.get(table).ok_or_else(|| format!("Table '{}' has no data", table))?;
            return Aggregator::execute(table, &columns[0], where_clause, tuples, &schema, &self.txn_mgr);
        }
        
        let data = self.data.read().unwrap();
        let tuples = data.get(table).ok_or_else(|| format!("Table '{}' has no data", table))?;
        
        let snapshot = self.txn_mgr.get_snapshot();
        let mut results = Vec::new();
        
        for tuple in tuples {
            if tuple.header.is_visible(&snapshot, &self.txn_mgr) {
                if let Some(ref predicate) = where_clause {
                    if !PredicateEvaluator::evaluate(predicate, &tuple.data, &schema)? {
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
        
        if let Some(group_cols) = group_by {
            results = Aggregator::apply_group_by(results, &group_cols, &columns, &schema)?;
            
            if let Some(having_expr) = having {
                results.retain(|row| {
                    PredicateEvaluator::evaluate_having(&having_expr, row).unwrap_or(false)
                });
            }
        }
        
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
        
        let start = offset.unwrap_or(0);
        let end = limit.map(|l| start + l).unwrap_or(results.len());
        results = results.into_iter().skip(start).take(end.saturating_sub(start)).collect();
        
        if distinct {
            let mut seen = std::collections::HashSet::new();
            results.retain(|row| seen.insert(row.clone()));
        }
        
        Ok(results)
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
                if let Some(ref predicate) = where_clause {
                    if !PredicateEvaluator::evaluate(predicate, &tuple.data, &schema)? {
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
        self.auto_save();
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
                if let Some(ref predicate) = where_clause {
                    if !PredicateEvaluator::evaluate(predicate, &tuple.data, &schema)? {
                        continue;
                    }
                }
                
                tuple.header.delete(txn.xid);
                deleted += 1;
            }
        }
        
        self.txn_mgr.commit(txn.xid).map_err(|e| e.to_string())?;
        self.auto_save();
        Ok(deleted)
    }
    
    pub fn save_to_disk(&self, data_dir: &str) -> Result<(), String> {
        let tables = self.tables.read().unwrap();
        let data = self.data.read().unwrap();
        Persistence::save(data_dir, &tables, &data)
    }
    
    pub fn load_from_disk(&self, data_dir: &str) -> Result<(), String> {
        let mut tables = self.tables.write().unwrap();
        let mut data = self.data.write().unwrap();
        Persistence::load(data_dir, &mut tables, &mut data, &self.txn_mgr)
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}
