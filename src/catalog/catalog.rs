use super::aggregation::Aggregator;
use super::persistence::Persistence;
use super::predicate::PredicateEvaluator;
use super::{Function, TableSchema, Tuple, UniqueValidator, Value};
use crate::parser::ast::{
    ColumnDef, CreateIndexStmt, CreateTriggerStmt, DataType, Expr, ForeignKeyAction, ForeignKeyDef,
    OrderByExpr, SelectStmt, UniqueConstraint,
};
use crate::transaction::{IsolationLevel, Transaction, TransactionManager, TupleHeader};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct Catalog {
    tables: Arc<RwLock<HashMap<String, TableSchema>>>,
    views: Arc<RwLock<HashMap<String, SelectStmt>>>,
    materialized_views: Arc<RwLock<HashMap<String, (SelectStmt, Vec<Vec<Value>>)>>>,
    triggers: Arc<RwLock<HashMap<String, CreateTriggerStmt>>>,
    indexes: Arc<RwLock<HashMap<String, CreateIndexStmt>>>,
    functions: Arc<RwLock<HashMap<String, Vec<Function>>>>,
    data: Arc<RwLock<HashMap<String, Vec<Tuple>>>>,
    sequences: Arc<RwLock<HashMap<String, i64>>>,
    active_txn: Arc<RwLock<Option<Transaction>>>,
    savepoints: Arc<RwLock<HashMap<String, Vec<Tuple>>>>,
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
            indexes: Arc::new(RwLock::new(HashMap::new())),
            functions: Arc::new(RwLock::new(HashMap::new())),
            data: Arc::new(RwLock::new(HashMap::new())),
            sequences: Arc::new(RwLock::new(HashMap::new())),
            active_txn: Arc::new(RwLock::new(None)),
            savepoints: Arc::new(RwLock::new(HashMap::new())),
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
            indexes: Arc::new(RwLock::new(HashMap::new())),
            functions: Arc::new(RwLock::new(HashMap::new())),
            data: Arc::new(RwLock::new(HashMap::new())),
            sequences: Arc::new(RwLock::new(HashMap::new())),
            active_txn: Arc::new(RwLock::new(None)),
            savepoints: Arc::new(RwLock::new(HashMap::new())),
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

        if let Ok(indexes) = Persistence::load_indexes(data_dir) {
            *catalog.indexes.write().unwrap() = indexes;
        }

        if let Ok(functions) = Persistence::load_functions(data_dir) {
            *catalog.functions.write().unwrap() = functions;
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

            let indexes_clone = self.indexes.read().unwrap().clone();
            if let Err(e) = Persistence::save_indexes(dir, &indexes_clone) {
                log::error!("Indexes auto-save failed: {}", e);
            }

            let functions_clone = self.functions.read().unwrap().clone();
            if let Err(e) = Persistence::save_functions(dir, &functions_clone) {
                log::error!("Functions auto-save failed: {}", e);
            }
        }
    }

    pub fn create_table(&self, name: String, columns: Vec<ColumnDef>) -> Result<(), String> {
        self.create_table_with_constraints(name, columns, None, Vec::new())
    }

    pub fn create_table_with_constraints(
        &self,
        name: String,
        columns: Vec<ColumnDef>,
        primary_key: Option<Vec<String>>,
        foreign_keys: Vec<ForeignKeyDef>,
    ) -> Result<(), String> {
        let mut tables = self.tables.write().unwrap();

        if tables.contains_key(&name) {
            return Err(format!("Table '{}' already exists", name));
        }

        // Collect primary key from column-level constraints
        let mut pk = primary_key;
        if pk.is_none() {
            let pk_cols: Vec<String> =
                columns.iter().filter(|c| c.is_primary_key).map(|c| c.name.clone()).collect();
            if !pk_cols.is_empty() {
                pk = Some(pk_cols);
            }
        }

        // Collect foreign keys from column-level constraints
        let mut fks = foreign_keys;
        for col in &columns {
            if let Some(ref fk_ref) = col.foreign_key {
                fks.push(ForeignKeyDef {
                    columns: vec![col.name.clone()],
                    ref_table: fk_ref.table.clone(),
                    ref_columns: vec![fk_ref.column.clone()],
                    on_delete: ForeignKeyAction::Restrict,
                    on_update: ForeignKeyAction::Restrict,
                });
            }
        }

        // Validate foreign key references
        for fk in &fks {
            if !tables.contains_key(&fk.ref_table) {
                return Err(format!("Referenced table '{}' does not exist", fk.ref_table));
            }
        }

        tables.insert(name.clone(), TableSchema::with_constraints(name.clone(), columns, pk, fks));
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

        let (_query, data) = mvs
            .get_mut(name)
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

    pub fn create_index(&self, index: CreateIndexStmt) -> Result<(), String> {
        let mut indexes = self.indexes.write().unwrap();

        if indexes.contains_key(&index.name) {
            return Err(format!("Index '{}' already exists", index.name));
        }

        indexes.insert(index.name.clone(), index);
        drop(indexes);
        self.auto_save();
        Ok(())
    }

    pub fn drop_index(&self, name: &str, if_exists: bool) -> Result<(), String> {
        let mut indexes = self.indexes.write().unwrap();

        if indexes.remove(name).is_none() && !if_exists {
            return Err(format!("Index '{}' does not exist", name));
        }
        drop(indexes);
        self.auto_save();
        Ok(())
    }

    pub fn get_index(&self, name: &str) -> Option<CreateIndexStmt> {
        let indexes = self.indexes.read().unwrap();
        indexes.get(name).cloned()
    }

    pub fn list_tables(&self) -> Vec<String> {
        let tables = self.tables.read().unwrap();
        tables.keys().cloned().collect()
    }

    pub fn create_function(&self, func: Function) -> Result<(), String> {
        let mut functions = self.functions.write().unwrap();
        functions.entry(func.name.clone()).or_default().push(func);
        drop(functions);
        self.auto_save();
        Ok(())
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

    pub fn insert(&self, table: &str, values: Vec<Expr>) -> Result<(), String> {
        let schema =
            self.get_table(table).ok_or_else(|| format!("Table '{}' does not exist", table))?;

        let txn = self.txn_mgr.begin();
        let header = TupleHeader::new(txn.xid);

        let mut tuple_data = Vec::new();

        // Handle partial inserts with DEFAULT values and AUTO_INCREMENT
        let num_provided = values.len();
        let num_columns = schema.columns.len();

        if num_provided > num_columns {
            return Err(format!("Too many values: expected {}, got {}", num_columns, num_provided));
        }

        for (i, col) in schema.columns.iter().enumerate() {
            let value = if i < num_provided {
                // Use provided value
                let val = match &values[i] {
                    Expr::Number(n) => Value::Int(*n),
                    Expr::String(s) => Value::Text(s.clone()),
                    _ => return Err("Invalid value expression".to_string()),
                };

                // Type check
                match (&col.data_type, &val) {
                    (DataType::Int, Value::Int(_)) => {}
                    (DataType::Serial, Value::Int(_)) => {}
                    (DataType::Text, Value::Text(_)) => {}
                    (DataType::Varchar(_), Value::Text(_)) => {}
                    _ => return Err(format!("Type mismatch for column '{}'", col.name)),
                }
                val
            } else if col.is_auto_increment || col.data_type == DataType::Serial {
                // Generate next sequence value
                let seq_key = format!("{}_{}", table, col.name);
                let mut sequences = self.sequences.write().unwrap();
                let next_val = sequences.entry(seq_key).or_insert(0);
                *next_val += 1;
                Value::Int(*next_val)
            } else if let Some(ref default_expr) = col.default_value {
                // Use default value
                match default_expr {
                    Expr::Number(n) => Value::Int(*n),
                    Expr::String(s) => Value::Text(s.clone()),
                    _ => return Err("Invalid default value expression".to_string()),
                }
            } else {
                return Err(format!("Column '{}' has no default value", col.name));
            };

            tuple_data.push(value);
        }

        // Validate NOT NULL constraints
        for (i, col) in schema.columns.iter().enumerate() {
            if (col.is_not_null || col.is_primary_key) && tuple_data[i] == Value::Null {
                return Err(format!("Column '{}' cannot be NULL", col.name));
            }
        }

        // Validate PRIMARY KEY uniqueness
        if let Some(ref pk_cols) = schema.primary_key {
            let pk_indices: Vec<usize> = pk_cols
                .iter()
                .map(|col| schema.columns.iter().position(|c| &c.name == col).unwrap())
                .collect();

            // Check for NULL in PK columns
            for &idx in &pk_indices {
                if tuple_data[idx] == Value::Null {
                    return Err(format!(
                        "Primary key column '{}' cannot be NULL",
                        schema.columns[idx].name
                    ));
                }
            }

            // Check for duplicate PK
            let data = self.data.read().unwrap();
            if let Some(tuples) = data.get(table) {
                let snapshot = self.txn_mgr.get_snapshot();
                for existing in tuples {
                    if existing.header.is_visible(&snapshot, &self.txn_mgr) {
                        let mut pk_match = true;
                        for &idx in &pk_indices {
                            if existing.data[idx] != tuple_data[idx] {
                                pk_match = false;
                                break;
                            }
                        }
                        if pk_match {
                            return Err("Primary key violation: duplicate key value".to_string());
                        }
                    }
                }
            }
        }

        // Validate FOREIGN KEY references
        for fk in &schema.foreign_keys {
            let fk_indices: Vec<usize> = fk
                .columns
                .iter()
                .map(|col| schema.columns.iter().position(|c| &c.name == col).unwrap())
                .collect();

            let fk_values: Vec<Value> =
                fk_indices.iter().map(|&idx| tuple_data[idx].clone()).collect();

            // Check if referenced row exists
            let ref_schema = self
                .get_table(&fk.ref_table)
                .ok_or_else(|| format!("Referenced table '{}' does not exist", fk.ref_table))?;

            let ref_indices: Vec<usize> = fk
                .ref_columns
                .iter()
                .map(|col| ref_schema.columns.iter().position(|c| &c.name == col).unwrap())
                .collect();

            let data = self.data.read().unwrap();
            let ref_tuples = data
                .get(&fk.ref_table)
                .ok_or_else(|| format!("Referenced table '{}' has no data", fk.ref_table))?;

            let snapshot = self.txn_mgr.get_snapshot();
            let mut found = false;
            for ref_tuple in ref_tuples {
                if ref_tuple.header.is_visible(&snapshot, &self.txn_mgr) {
                    let mut match_found = true;
                    for (i, &ref_idx) in ref_indices.iter().enumerate() {
                        if ref_tuple.data[ref_idx] != fk_values[i] {
                            match_found = false;
                            break;
                        }
                    }
                    if match_found {
                        found = true;
                        break;
                    }
                }
            }

            if !found {
                return Err(format!(
                    "Foreign key violation: referenced row does not exist in table '{}'",
                    fk.ref_table
                ));
            }
        }

        // Validate UNIQUE constraints
        let data = self.data.read().unwrap();
        if let Some(tuples) = data.get(table) {
            let snapshot = self.txn_mgr.get_snapshot();
            let visible_tuples: Vec<Tuple> = tuples
                .iter()
                .filter(|t| t.header.is_visible(&snapshot, &self.txn_mgr))
                .cloned()
                .collect();

            // Check column-level UNIQUE constraints
            for (i, col) in schema.columns.iter().enumerate() {
                if col.is_unique {
                    let constraint = UniqueConstraint {
                        name: Some(format!("{}_{}_unique", table, col.name)),
                        columns: vec![col.name.clone()],
                    };
                    UniqueValidator::validate(&constraint, &tuple_data, &visible_tuples, &[i])?;
                }
            }

            // Check table-level UNIQUE constraints
            for unique_constraint in &schema.unique_constraints {
                let indices: Vec<usize> = unique_constraint
                    .columns
                    .iter()
                    .map(|col| schema.columns.iter().position(|c| &c.name == col).unwrap())
                    .collect();
                UniqueValidator::validate(
                    unique_constraint,
                    &tuple_data,
                    &visible_tuples,
                    &indices,
                )?;
            }
        }
        drop(data);

        let tuple = Tuple { header, data: tuple_data, column_map: HashMap::new() };

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
        let schema =
            self.get_table(table).ok_or_else(|| format!("Table '{}' does not exist", table))?;

        if columns.len() == 1 && columns[0].starts_with("AGG:") {
            let data = self.data.read().unwrap();
            let tuples = data.get(table).ok_or_else(|| format!("Table '{}' has no data", table))?;
            return Aggregator::execute(
                table,
                &columns[0],
                where_clause,
                tuples,
                &schema,
                &self.txn_mgr,
            );
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
                let col_idx = schema
                    .columns
                    .iter()
                    .position(|c| c.name == order_expr.column)
                    .ok_or_else(|| format!("Column '{}' not found", order_expr.column))?;

                results.sort_by(|a, b| {
                    let cmp = a[col_idx].cmp(&b[col_idx]);
                    if order_expr.ascending {
                        cmp
                    } else {
                        cmp.reverse()
                    }
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

    pub fn update(
        &self,
        table: &str,
        assignments: Vec<(String, Expr)>,
        where_clause: Option<Expr>,
    ) -> Result<usize, String> {
        let schema =
            self.get_table(table).ok_or_else(|| format!("Table '{}' does not exist", table))?;

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
                    let idx = schema
                        .columns
                        .iter()
                        .position(|c| &c.name == col_name)
                        .ok_or_else(|| format!("Column '{}' not found", col_name))?;

                    let value = match expr {
                        Expr::Number(n) => Value::Int(*n),
                        Expr::String(s) => Value::Text(s.clone()),
                        _ => return Err("Invalid value expression".to_string()),
                    };

                    match (&schema.columns[idx].data_type, &value) {
                        (DataType::Int, Value::Int(_)) => {}
                        (DataType::Text, Value::Text(_)) => {}
                        (DataType::Varchar(_), Value::Text(_)) => {}
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
        let schema =
            self.get_table(table).ok_or_else(|| format!("Table '{}' does not exist", table))?;

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

    pub fn begin_transaction(&self) -> Result<(), String> {
        self.begin_transaction_with_isolation(IsolationLevel::ReadCommitted)
    }

    pub fn begin_transaction_with_isolation(&self, level: IsolationLevel) -> Result<(), String> {
        let mut active = self.active_txn.write().unwrap();
        if active.is_some() {
            return Err("Transaction already in progress".to_string());
        }
        *active = Some(self.txn_mgr.begin_with_isolation(level));
        Ok(())
    }

    pub fn set_transaction_isolation(&self, level: IsolationLevel) -> Result<(), String> {
        let mut active = self.active_txn.write().unwrap();
        if let Some(ref mut txn) = *active {
            txn.isolation_level = level;
            if level == IsolationLevel::RepeatableRead || level == IsolationLevel::Serializable {
                txn.snapshot = self.txn_mgr.get_snapshot();
            }
            Ok(())
        } else {
            Err("No active transaction".to_string())
        }
    }

    pub fn commit_transaction(&self) -> Result<(), String> {
        let mut active = self.active_txn.write().unwrap();
        let txn = active.take().ok_or("No active transaction")?;
        self.txn_mgr.commit(txn.xid).map_err(|e| e.to_string())?;
        Ok(())
    }

    pub fn rollback_transaction(&self) -> Result<(), String> {
        let mut active = self.active_txn.write().unwrap();
        let txn = active.take().ok_or("No active transaction")?;
        self.txn_mgr.abort(txn.xid).map_err(|e| e.to_string())?;
        self.savepoints.write().unwrap().clear();
        Ok(())
    }

    pub fn savepoint(&self, name: String) -> Result<(), String> {
        let active = self.active_txn.read().unwrap();
        if active.is_none() {
            return Err("No active transaction".to_string());
        }
        drop(active);

        let data = self.data.read().unwrap();
        let snapshot: Vec<Tuple> = data.values().flat_map(|v| v.clone()).collect();
        self.savepoints.write().unwrap().insert(name, snapshot);
        Ok(())
    }

    pub fn rollback_to_savepoint(&self, name: &str) -> Result<(), String> {
        let active = self.active_txn.read().unwrap();
        if active.is_none() {
            return Err("No active transaction".to_string());
        }
        drop(active);

        let snapshot = {
            let savepoints = self.savepoints.read().unwrap();
            savepoints.get(name).ok_or("Savepoint does not exist")?.clone()
        };

        let mut data = self.data.write().unwrap();
        data.clear();
        for tuple in &snapshot {
            let table_name = self
                .tables
                .read()
                .unwrap()
                .iter()
                .find(|(_, schema)| schema.columns.len() == tuple.data.len())
                .map(|(name, _)| name.clone());

            if let Some(table) = table_name {
                data.entry(table).or_insert_with(Vec::new).push(tuple.clone());
            }
        }
        Ok(())
    }

    pub fn release_savepoint(&self, name: &str) -> Result<(), String> {
        let active = self.active_txn.read().unwrap();
        if active.is_none() {
            return Err("No active transaction".to_string());
        }
        drop(active);

        self.savepoints.write().unwrap().remove(name).ok_or("Savepoint does not exist")?;
        Ok(())
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}
