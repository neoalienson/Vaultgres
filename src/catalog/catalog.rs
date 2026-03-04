use super::crud_helper::CrudHelper;
use super::insert_validator::InsertValidator;
use super::persistence::Persistence;
use super::update_delete_executor::UpdateDeleteExecutor;
use super::{Function, TableSchema, Value};
use crate::parser::ast::{
    ColumnDef, CreateIndexStmt, CreateTriggerStmt, Expr, ForeignKeyAction, ForeignKeyDef,
    OrderByExpr, SelectStmt,
};
use crate::transaction::{IsolationLevel, Transaction, TransactionManager};
use std::collections::HashMap;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

#[derive(Clone)]
pub struct Catalog {
    pub(crate) tables: Arc<RwLock<HashMap<String, TableSchema>>>,
    pub(crate) views: Arc<RwLock<HashMap<String, SelectStmt>>>,
    pub(crate) materialized_views: Arc<RwLock<HashMap<String, (SelectStmt, Vec<Vec<Value>>)>>>,
    pub(crate) triggers: Arc<RwLock<HashMap<String, CreateTriggerStmt>>>,
    pub(crate) indexes: Arc<RwLock<HashMap<String, CreateIndexStmt>>>,
    pub(crate) functions: Arc<RwLock<HashMap<String, Vec<Function>>>>,
    pub(crate) data: Arc<RwLock<HashMap<String, Vec<crate::catalog::tuple::Tuple>>>>,
    pub(crate) sequences: Arc<RwLock<HashMap<String, i64>>>,
    pub(crate) active_txn: Arc<RwLock<Option<Transaction>>>,
    pub(crate) savepoints: Arc<RwLock<HashMap<String, Vec<crate::catalog::tuple::Tuple>>>>,
    pub(crate) txn_mgr: Arc<TransactionManager>,
    pub(crate) data_dir: Option<String>,
    pub(crate) save_tx: Option<Sender<()>>,
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
            save_tx: None,
        }
    }

    pub fn new_with_data_dir(data_dir: &str) -> Self {
        let (tx, rx) = channel();

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
            save_tx: Some(tx),
        };

        let tables = Arc::clone(&catalog.tables);
        let views = Arc::clone(&catalog.views);
        let triggers = Arc::clone(&catalog.triggers);
        let indexes = Arc::clone(&catalog.indexes);
        let functions = Arc::clone(&catalog.functions);
        let data = Arc::clone(&catalog.data);
        let dir = data_dir.to_string();

        thread::spawn(move || {
            let mut last_save = std::time::Instant::now();
            while rx.recv().is_ok() {
                if last_save.elapsed() < Duration::from_millis(100) {
                    thread::sleep(Duration::from_millis(100) - last_save.elapsed());
                }

                let tables_clone = tables.read().unwrap().clone();
                let data_clone = data.read().unwrap().clone();
                if let Err(e) = Persistence::save(&dir, &tables_clone, &data_clone) {
                    log::error!("Async save failed: {}", e);
                }

                let views_clone = views.read().unwrap().clone();
                if let Err(e) = Persistence::save_views(&dir, &views_clone) {
                    log::error!("Async views save failed: {}", e);
                }

                let triggers_clone = triggers.read().unwrap().clone();
                if let Err(e) = Persistence::save_triggers(&dir, &triggers_clone) {
                    log::error!("Async triggers save failed: {}", e);
                }

                let indexes_clone = indexes.read().unwrap().clone();
                if let Err(e) = Persistence::save_indexes(&dir, &indexes_clone) {
                    log::error!("Async indexes save failed: {}", e);
                }

                let functions_clone = functions.read().unwrap().clone();
                if let Err(e) = Persistence::save_functions(&dir, &functions_clone) {
                    log::error!("Async functions save failed: {}", e);
                }

                last_save = std::time::Instant::now();
            }
        });

        let mut tables_lock = catalog.tables.write().unwrap();
        let mut data_lock = catalog.data.write().unwrap();
        if let Err(e) =
            Persistence::load(data_dir, &mut tables_lock, &mut data_lock, &catalog.txn_mgr)
        {
            log::error!("Failed to load catalog: {}", e);
        }
        drop(tables_lock);
        drop(data_lock);

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
        if let Some(ref tx) = self.save_tx {
            let _ = tx.send(());
        }
    }

    pub fn flush_saves(&self) {
        if self.data_dir.is_some() {
            std::thread::sleep(std::time::Duration::from_millis(150));
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
        CrudHelper::create(&self.views, name, query, "View")?;
        self.auto_save();
        self.flush_saves();
        Ok(())
    }

    pub fn drop_view(&self, name: &str, if_exists: bool) -> Result<(), String> {
        CrudHelper::drop(&self.views, name, if_exists, "View")?;
        self.auto_save();
        self.flush_saves();
        Ok(())
    }

    pub fn get_view(&self, name: &str) -> Option<SelectStmt> {
        self.views.read().unwrap().get(name).cloned()
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
        CrudHelper::create(&self.triggers, trigger.name.clone(), trigger, "Trigger")?;
        self.auto_save();
        self.flush_saves();
        Ok(())
    }

    pub fn drop_trigger(&self, name: &str, if_exists: bool) -> Result<(), String> {
        CrudHelper::drop(&self.triggers, name, if_exists, "Trigger")?;
        self.auto_save();
        self.flush_saves();
        Ok(())
    }

    pub fn get_trigger(&self, name: &str) -> Option<CreateTriggerStmt> {
        CrudHelper::get(&self.triggers, name)
    }

    pub fn create_index(&self, index: CreateIndexStmt) -> Result<(), String> {
        CrudHelper::create(&self.indexes, index.name.clone(), index, "Index")?;
        self.auto_save();
        self.flush_saves();
        Ok(())
    }

    pub fn drop_index(&self, name: &str, if_exists: bool) -> Result<(), String> {
        CrudHelper::drop(&self.indexes, name, if_exists, "Index")?;
        self.auto_save();
        Ok(())
    }

    pub fn get_index(&self, name: &str) -> Option<CreateIndexStmt> {
        CrudHelper::get(&self.indexes, name)
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

        if values.len() > schema.columns.len() {
            return Err(format!(
                "Too many values: expected {}, got {}",
                schema.columns.len(),
                values.len()
            ));
        }

        let txn = self.txn_mgr.begin();
        let header = crate::transaction::TupleHeader::new(txn.xid);

        let tuple_data: Result<Vec<Value>, String> = schema
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| InsertValidator::resolve_value(col, i, &values, table, &self.sequences))
            .collect();
        let tuple_data = tuple_data?;

        InsertValidator::validate_not_null(&schema, &tuple_data)?;

        let data = self.data.read().unwrap();
        InsertValidator::validate_primary_key(&schema, &tuple_data, table, &data, &self.txn_mgr)?;

        let tables = self.tables.read().unwrap();
        InsertValidator::validate_foreign_keys(
            &schema,
            &tuple_data,
            &data,
            &tables,
            &self.txn_mgr,
        )?;
        drop(tables);

        InsertValidator::validate_unique(&schema, &tuple_data, table, &data, &self.txn_mgr)?;
        drop(data);

        let tuple =
            crate::catalog::tuple::Tuple { header, data: tuple_data, column_map: HashMap::new() };

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

    pub fn batch_insert(&self, table: &str, batch: Vec<Vec<Expr>>) -> Result<usize, String> {
        if batch.is_empty() {
            return Ok(0);
        }

        let schema =
            self.get_table(table).ok_or_else(|| format!("Table '{}' does not exist", table))?;

        let txn = self.txn_mgr.begin();
        let header = crate::transaction::TupleHeader::new(txn.xid);

        let tuples: Result<Vec<crate::catalog::tuple::Tuple>, String> = batch
            .into_iter()
            .map(|values| {
                if values.len() > schema.columns.len() {
                    return Err(format!(
                        "Too many values: expected {}, got {}",
                        schema.columns.len(),
                        values.len()
                    ));
                }

                let tuple_data: Result<Vec<Value>, String> = schema
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, col)| {
                        InsertValidator::resolve_value(col, i, &values, table, &self.sequences)
                    })
                    .collect();

                Ok(crate::catalog::tuple::Tuple {
                    header,
                    data: tuple_data?,
                    column_map: HashMap::new(),
                })
            })
            .collect();

        let tuples = tuples?;
        let count = tuples.len();

        let mut data = self.data.write().unwrap();
        data.get_mut(table).unwrap().extend(tuples);
        drop(data);

        self.txn_mgr.commit(txn.xid).map_err(|e| e.to_string())?;
        self.auto_save();
        Ok(count)
    }

    pub fn select(
        &self,
        table_name: &str,
        distinct: bool,
        columns: Vec<Expr>,
        where_clause: Option<Expr>,
        group_by: Option<Vec<Expr>>,
        having: Option<Expr>,
        order_by: Option<Vec<OrderByExpr>>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Vec<Vec<Value>>, String> {
        // Build a SelectStmt from the parameters
        let select_stmt = SelectStmt {
            distinct,
            columns,
            from: table_name.to_string(),
            table_alias: None,
            joins: Vec::new(),
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        };

        // Use the planner to build and execute the query plan
        // Note: This creates a new planner without catalog for simple queries
        // For queries with subqueries or views, use select_with_catalog
        use crate::planner::planner::Planner;
        let planner = Planner::new_without_catalog();
        let mut plan = planner.plan(&select_stmt).map_err(|e| format!("{:?}", e))?;

        // Collect results by calling next() on the plan
        let mut rows: Vec<Vec<Value>> = Vec::new();
        let mut output_column_names: Option<Vec<String>> = None;

        loop {
            match plan.next() {
                Ok(Some(tuple_hashmap)) => {
                    let mut row = Vec::new();

                    // Determine column names from the first tuple
                    // Use sorted keys to ensure consistent column order
                    if output_column_names.is_none() {
                        let mut keys: Vec<String> = tuple_hashmap.keys().cloned().collect();
                        keys.sort();
                        output_column_names = Some(keys);
                    }

                    // Collect values in the order of column names
                    if let Some(ref col_names) = output_column_names {
                        for col_name in col_names {
                            row.push(tuple_hashmap.get(col_name).cloned().unwrap_or(Value::Null));
                        }
                    }
                    rows.push(row);
                }
                Ok(None) => break, // End of data
                Err(e) => return Err(format!("{:?}", e)),
            }
        }

        Ok(rows)
    }

    /// Select with Arc<Catalog> for subquery and view support
    pub fn select_with_catalog(
        catalog_arc: &Arc<Catalog>,
        table_name: &str,
        distinct: bool,
        columns: Vec<Expr>,
        where_clause: Option<Expr>,
        group_by: Option<Vec<Expr>>,
        having: Option<Expr>,
        order_by: Option<Vec<OrderByExpr>>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Vec<Vec<Value>>, String> {
        // Build a SelectStmt from the parameters
        let select_stmt = SelectStmt {
            distinct,
            columns,
            from: table_name.to_string(),
            table_alias: None,
            joins: Vec::new(),
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        };

        // Use the planner to build and execute the query plan
        use crate::planner::planner::Planner;
        let planner = Planner::new_with_catalog(catalog_arc.clone());
        let mut plan = planner.plan(&select_stmt).map_err(|e| format!("{:?}", e))?;

        // Collect results by calling next() on the plan
        let mut rows: Vec<Vec<Value>> = Vec::new();
        let mut output_column_names: Option<Vec<String>> = None;

        loop {
            match plan.next() {
                Ok(Some(tuple_hashmap)) => {
                    let mut row = Vec::new();

                    // Determine column names from the first tuple
                    // Use sorted keys to ensure consistent column order
                    if output_column_names.is_none() {
                        let mut keys: Vec<String> = tuple_hashmap.keys().cloned().collect();
                        keys.sort();
                        output_column_names = Some(keys);
                    }

                    // Collect values in the order of column names
                    if let Some(ref col_names) = output_column_names {
                        for col_name in col_names {
                            row.push(tuple_hashmap.get(col_name).cloned().unwrap_or(Value::Null));
                        }
                    }
                    rows.push(row);
                }
                Ok(None) => break, // End of data
                Err(e) => return Err(format!("{:?}", e)),
            }
        }

        Ok(rows)
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

        let updated = UpdateDeleteExecutor::update(
            tuples,
            &assignments,
            &where_clause,
            &schema,
            &snapshot,
            &self.txn_mgr,
        )?;

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

        let deleted = UpdateDeleteExecutor::delete(
            tuples,
            &where_clause,
            &schema,
            &snapshot,
            &self.txn_mgr,
            txn.xid,
        )?;

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
        let snapshot: Vec<crate::catalog::tuple::Tuple> =
            data.values().flat_map(|v| v.clone()).collect();
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
                data.entry(table).or_default().push(tuple.clone());
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
