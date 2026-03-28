use super::aggregate_manager::AggregateManager;
use super::crud_helper::CrudHelper;
use super::data_manager::DataManager;
use super::function_manager::FunctionManager;
use super::index_manager::IndexManager;
use super::insert_validator::InsertValidator;
use super::persistence::Persistence;
use super::table_manager::TableManager;
use super::transaction_manager::TransactionManager2;
use super::trigger_manager::TriggerManager;
use super::type_manager::TypeManager;
use super::update_delete_executor::UpdateDeleteExecutor;
use super::view_manager::ViewManager;
use super::{Aggregate, Function, TableSchema, Value};
use crate::parser::ast::{
    AttachPartitionStmt, ColumnDef, CompositeTypeDef, CreateIndexStmt, CreateTriggerStmt, DataType,
    DetachPartitionStmt, EnumTypeDef, Expr, ForeignKeyAction, ForeignKeyDef, OrderByExpr,
    PartitionBoundSpec, SelectStmt,
};
use crate::transaction::{IsolationLevel, Transaction, TransactionManager};
use std::collections::HashMap;
use std::sync::mpsc::{Sender, channel};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

#[derive(Clone)]
pub struct Catalog {
    pub(crate) tables: Arc<RwLock<HashMap<String, TableSchema>>>,
    pub(crate) views: Arc<RwLock<HashMap<String, SelectStmt>>>,
    pub(crate) materialized_views:
        Arc<RwLock<HashMap<String, (SelectStmt, Vec<Vec<Value>>, Vec<String>)>>>,
    pub(crate) triggers: Arc<RwLock<HashMap<String, CreateTriggerStmt>>>,
    pub(crate) indexes: Arc<RwLock<HashMap<String, CreateIndexStmt>>>,
    pub(crate) functions: Arc<RwLock<HashMap<String, Vec<Function>>>>,
    pub(crate) aggregates: Arc<RwLock<HashMap<String, Aggregate>>>,
    pub(crate) enum_types: Arc<RwLock<HashMap<String, EnumTypeDef>>>,
    pub(crate) composite_types: Arc<RwLock<HashMap<String, CompositeTypeDef>>>,
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
            aggregates: Arc::new(RwLock::new(HashMap::new())),
            enum_types: Arc::new(RwLock::new(HashMap::new())),
            composite_types: Arc::new(RwLock::new(HashMap::new())),
            data: Arc::new(RwLock::new(HashMap::new())),
            sequences: Arc::new(RwLock::new(HashMap::new())),
            active_txn: Arc::new(RwLock::new(None)),
            savepoints: Arc::new(RwLock::new(HashMap::new())),
            txn_mgr: Arc::new(TransactionManager::new()),
            data_dir: None,
            save_tx: None,
        }
    }

    pub fn new_with_data_dir(data_dir: &str) -> Arc<Self> {
        let (tx, rx) = channel();

        let catalog = Arc::new(Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            views: Arc::new(RwLock::new(HashMap::new())),
            materialized_views: Arc::new(RwLock::new(HashMap::new())),
            triggers: Arc::new(RwLock::new(HashMap::new())),
            indexes: Arc::new(RwLock::new(HashMap::new())),
            functions: Arc::new(RwLock::new(HashMap::new())),
            aggregates: Arc::new(RwLock::new(HashMap::new())),
            enum_types: Arc::new(RwLock::new(HashMap::new())),
            composite_types: Arc::new(RwLock::new(HashMap::new())),
            data: Arc::new(RwLock::new(HashMap::new())),
            sequences: Arc::new(RwLock::new(HashMap::new())),
            active_txn: Arc::new(RwLock::new(None)),
            savepoints: Arc::new(RwLock::new(HashMap::new())),
            txn_mgr: Arc::new(TransactionManager::new()),
            data_dir: Some(data_dir.to_string()),
            save_tx: Some(tx),
        });

        let tables = Arc::clone(&catalog.tables);
        let views = Arc::clone(&catalog.views);
        let materialized_views = Arc::clone(&catalog.materialized_views);
        let triggers = Arc::clone(&catalog.triggers);
        let indexes = Arc::clone(&catalog.indexes);
        let functions = Arc::clone(&catalog.functions);
        let aggregates = Arc::clone(&catalog.aggregates);
        let enum_types = Arc::clone(&catalog.enum_types);
        let composite_types = Arc::clone(&catalog.composite_types);
        let data = Arc::clone(&catalog.data);
        let dir = data_dir.to_string();

        thread::spawn(move || {
            let mut last_save = std::time::Instant::now();
            while rx.recv().is_ok() {
                log::debug!("Background save thread: received save signal");
                if last_save.elapsed() < Duration::from_millis(100) {
                    thread::sleep(Duration::from_millis(100) - last_save.elapsed());
                }

                let tables_clone = tables.read().unwrap().clone();
                let data_clone = data.read().unwrap().clone();
                log::debug!(
                    "Background save: saving {} tables with {} rows total",
                    tables_clone.len(),
                    data_clone.values().map(|v| v.len()).sum::<usize>()
                );
                if let Err(e) = Persistence::save(&dir, &tables_clone, &data_clone) {
                    log::error!("Async save failed: {}", e);
                } else {
                    log::debug!("Background save: tables saved successfully");
                }

                let views_clone = views.read().unwrap().clone();
                if let Err(e) = Persistence::save_views(&dir, &views_clone) {
                    log::error!("Async views save failed: {}", e);
                }

                let materialized_views_clone = materialized_views.read().unwrap().clone();
                if let Err(e) =
                    Persistence::save_materialized_views(&dir, &materialized_views_clone)
                {
                    log::error!("Async materialized views save failed: {}", e);
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

                let aggregates_clone = aggregates.read().unwrap().clone();
                if let Err(e) = Persistence::save_aggregates(&dir, &aggregates_clone) {
                    log::error!("Async aggregates save failed: {}", e);
                }

                let tables_for_partitions = tables.read().unwrap().clone();
                if let Err(e) = Persistence::save_partitions(&dir, &tables_for_partitions) {
                    log::error!("Async partitions save failed: {}", e);
                }

                let enum_types_clone = enum_types.read().unwrap().clone();
                if let Err(e) = Persistence::save_enum_types(&dir, &enum_types_clone) {
                    log::error!("Async enum types save failed: {}", e);
                }

                let composite_types_clone = composite_types.read().unwrap().clone();
                if let Err(e) = Persistence::save_composite_types(&dir, &composite_types_clone) {
                    log::error!("Async composite types save failed: {}", e);
                }

                last_save = std::time::Instant::now();
                log::debug!("Background save: cycle complete");
            }
            log::debug!("Background save thread: channel closed, exiting");
        });

        let mut tables_lock = catalog.tables.write().unwrap();
        let mut data_lock = catalog.data.write().unwrap();
        log::info!("📂 Catalog::new_with_data_dir: Loading from disk: {}", data_dir);
        if let Err(e) =
            Persistence::load(data_dir, &mut tables_lock, &mut data_lock, &catalog.txn_mgr)
        {
            log::error!("📂 Failed to load catalog from {}: {}", data_dir, e);
        } else {
            log::info!(
                "✅ Loaded {} tables with {} rows total from {}",
                tables_lock.len(),
                data_lock.values().map(|v| v.len()).sum::<usize>(),
                data_dir
            );
        }
        drop(tables_lock);
        drop(data_lock);

        if let Ok(views) = Persistence::load_views(data_dir) {
            log::info!("📂 Loaded {} views from {}", views.len(), data_dir);
            *catalog.views.write().unwrap() = views;
        } else {
            log::debug!("No views found at {}", data_dir);
        }

        if let Ok(materialized_views) = Persistence::load_materialized_views(data_dir) {
            log::info!(
                "📂 Loaded {} materialized views from {}",
                materialized_views.len(),
                data_dir
            );
            *catalog.materialized_views.write().unwrap() = materialized_views;
        } else {
            log::debug!("No materialized views found at {}", data_dir);
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

        if let Ok(aggregates) = Persistence::load_aggregates(data_dir) {
            *catalog.aggregates.write().unwrap() = aggregates;
        }

        if let Ok(partitions) = Persistence::load_partitions(data_dir) {
            if !partitions.is_empty() {
                let num_partitions = partitions.len();
                let mut tables_lock = catalog.tables.write().unwrap();
                for (name, partition_info) in partitions {
                    if let Some(table) = tables_lock.get_mut(&name) {
                        table.partition_method = partition_info.partition_method;
                        table.partition_keys = partition_info.partition_keys;
                        table.is_partition = partition_info.is_partition;
                        table.parent_table = partition_info.parent_table;
                        table.partition_bound = partition_info.partition_bound;
                    }
                }
                log::info!("📂 Applied partition info to {} tables", num_partitions);
            }
        }

        if let Ok(enum_types) = Persistence::load_enum_types(data_dir) {
            *catalog.enum_types.write().unwrap() = enum_types;
            log::info!(
                "📂 Loaded {} enum types from {}",
                catalog.enum_types.read().unwrap().len(),
                data_dir
            );
        }

        if let Ok(composite_types) = Persistence::load_composite_types(data_dir) {
            *catalog.composite_types.write().unwrap() = composite_types;
            log::info!(
                "📂 Loaded {} composite types from {}",
                catalog.composite_types.read().unwrap().len(),
                data_dir
            );
        }

        catalog
    }

    pub(crate) fn auto_save(&self) {
        if let Some(ref tx) = self.save_tx {
            log::debug!("auto_save: sending save signal, data_dir={:?}", self.data_dir);
            let _ = tx.send(());
        } else {
            log::debug!("auto_save: no save_tx channel (data_dir not set)");
        }
    }

    pub fn flush_saves(&self) {
        if self.data_dir.is_some() {
            log::info!(
                "flush_saves: waiting for background save to complete, data_dir={:?}",
                self.data_dir
            );
            // Wait longer to ensure background save completes
            std::thread::sleep(std::time::Duration::from_secs(2));
            log::info!("flush_saves: done waiting for background save");
        } else {
            log::debug!("flush_saves: no data_dir, skipping");
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
        let manager = TableManager::with_all(
            Arc::clone(&self.tables),
            Arc::clone(&self.data),
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
        );
        manager.create_table_with_constraints(name.clone(), columns, primary_key, foreign_keys)?;
        log::debug!("create_table: created table '{}', triggering synchronous save", name);
        self.force_save()?;
        Ok(())
    }

    pub fn create_partitioned_table(&self, schema: TableSchema) -> Result<(), String> {
        let manager = TableManager::with_all(
            Arc::clone(&self.tables),
            Arc::clone(&self.data),
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
        );
        manager.create_partitioned_table(schema.clone())?;
        log::debug!("create_partitioned_table: created partitioned table '{}'", schema.name);
        self.auto_save();
        Ok(())
    }

    pub fn create_partition(&self, schema: TableSchema) -> Result<(), String> {
        let manager = TableManager::with_all(
            Arc::clone(&self.tables),
            Arc::clone(&self.data),
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
        );
        manager.create_partition(schema.clone())?;
        log::debug!("create_partition: created partition '{}'", schema.name);
        self.auto_save();
        Ok(())
    }

    pub fn attach_partition(&self, stmt: &AttachPartitionStmt) -> Result<(), String> {
        let manager = TableManager::with_all(
            Arc::clone(&self.tables),
            Arc::clone(&self.data),
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
        );
        manager.attach_partition(stmt)?;
        self.auto_save();
        Ok(())
    }

    pub fn detach_partition(&self, stmt: &DetachPartitionStmt) -> Result<(), String> {
        let manager = TableManager::with_all(
            Arc::clone(&self.tables),
            Arc::clone(&self.data),
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
        );
        manager.detach_partition(stmt)?;
        self.auto_save();
        Ok(())
    }

    pub fn get_partitions(&self, parent_table: &str) -> Vec<String> {
        let manager = TableManager::with_all(
            Arc::clone(&self.tables),
            Arc::clone(&self.data),
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
        );
        manager.get_partitions(parent_table)
    }

    pub fn get_partitions_for_predicate(
        &self,
        parent_table: &str,
        where_clause: &Option<Expr>,
    ) -> Vec<String> {
        let manager = TableManager::with_all(
            Arc::clone(&self.tables),
            Arc::clone(&self.data),
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
        );
        manager.get_partitions_for_predicate(parent_table, where_clause)
    }

    pub fn is_partitioned_table(&self, name: &str) -> bool {
        let manager = TableManager::with_all(
            Arc::clone(&self.tables),
            Arc::clone(&self.data),
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
        );
        manager.is_partitioned_table(name)
    }

    pub fn is_partition(&self, name: &str) -> bool {
        let manager = TableManager::with_all(
            Arc::clone(&self.tables),
            Arc::clone(&self.data),
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
        );
        manager.is_partition(name)
    }

    pub fn get_parent_table(&self, partition: &str) -> Option<String> {
        let manager = TableManager::with_all(
            Arc::clone(&self.tables),
            Arc::clone(&self.data),
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
        );
        manager.get_parent_table(partition)
    }

    pub fn create_type(&self, type_name: String, labels: Vec<String>) -> Result<(), String> {
        let manager = TypeManager::with_types(
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
            Arc::clone(&self.tables),
            Arc::new(self.clone()),
        );
        manager.create_type(type_name, labels)?;
        self.auto_save();
        Ok(())
    }

    pub fn create_composite_type(
        &self,
        type_name: String,
        fields: Vec<(String, DataType)>,
    ) -> Result<(), String> {
        let manager = TypeManager::with_types(
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
            Arc::clone(&self.tables),
            Arc::new(self.clone()),
        );
        manager.create_composite_type(type_name, fields)?;
        self.auto_save();
        Ok(())
    }

    pub fn get_composite_type(&self, type_name: &str) -> Option<CompositeTypeDef> {
        let manager = TypeManager::with_types(
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
            Arc::clone(&self.tables),
            Arc::new(self.clone()),
        );
        manager.get_composite_type(type_name)
    }

    pub fn drop_type(&self, type_name: &str, if_exists: bool, cascade: bool) -> Result<(), String> {
        let manager = TypeManager::with_types(
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
            Arc::clone(&self.tables),
            Arc::new(self.clone()),
        );
        manager.drop_type(type_name, if_exists, cascade)?;
        self.auto_save();
        Ok(())
    }

    pub fn alter_type_add_value(
        &self,
        type_name: &str,
        new_label: String,
        after_label: Option<String>,
    ) -> Result<(), String> {
        let manager = TypeManager::with_types(
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
            Arc::clone(&self.tables),
            Arc::new(self.clone()),
        );
        manager.alter_type_add_value(type_name, new_label, after_label)?;
        self.auto_save();
        Ok(())
    }

    pub fn get_enum_type(&self, type_name: &str) -> Option<EnumTypeDef> {
        let manager = TypeManager::with_types(
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
            Arc::clone(&self.tables),
            Arc::new(self.clone()),
        );
        manager.get_enum_type(type_name)
    }

    pub fn get_enum_label_index(&self, type_name: &str, label: &str) -> Option<i32> {
        let manager = TypeManager::with_types(
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
            Arc::clone(&self.tables),
            Arc::new(self.clone()),
        );
        manager.get_enum_label_index(type_name, label)
    }

    pub fn drop_table(&self, name: &str, if_exists: bool) -> Result<(), String> {
        let manager = TableManager::with_all(
            Arc::clone(&self.tables),
            Arc::clone(&self.data),
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
        );
        manager.drop_table(name, if_exists)?;
        self.auto_save();
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<TableSchema> {
        let manager = TableManager::with_all(
            Arc::clone(&self.tables),
            Arc::clone(&self.data),
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
        );
        manager.get_table(name)
    }

    pub fn create_view(&self, name: String, query: SelectStmt) -> Result<(), String> {
        let manager = ViewManager::with_views(
            Arc::clone(&self.views),
            Arc::clone(&self.materialized_views),
            Arc::new(self.clone()),
        );
        manager.create_view(name.clone(), query)?;
        log::debug!("create_view: created view '{}', triggering synchronous save", name);
        self.force_save()?;
        Ok(())
    }

    pub fn drop_view(&self, name: &str, if_exists: bool) -> Result<(), String> {
        let manager = ViewManager::with_views(
            Arc::clone(&self.views),
            Arc::clone(&self.materialized_views),
            Arc::new(self.clone()),
        );
        manager.drop_view(name, if_exists)?;
        log::debug!("drop_view: dropped view '{}', triggering synchronous save", name);
        self.force_save()?;
        Ok(())
    }

    pub fn get_view(&self, name: &str) -> Option<SelectStmt> {
        let manager = ViewManager::with_views(
            Arc::clone(&self.views),
            Arc::clone(&self.materialized_views),
            Arc::new(self.clone()),
        );
        manager.get_view(name)
    }

    pub fn create_materialized_view(
        self: &Arc<Self>,
        name: String,
        query: SelectStmt,
    ) -> Result<(), String> {
        let mut mvs = self.materialized_views.write().unwrap();

        if mvs.contains_key(&name) {
            return Err(format!("Materialized view '{}' already exists", name));
        }

        // Use the planner directly with the full query (including JOINs)
        use crate::planner::planner::Planner;
        let planner = Planner::new_with_catalog(self.clone());
        let mut plan = planner.plan(&query).map_err(|e| format!("{:?}", e))?;

        // Collect results and column names
        let mut data: Vec<Vec<Value>> = Vec::new();
        let mut column_names: Vec<String> = Vec::new();

        loop {
            match plan.next() {
                Ok(Some(tuple_hashmap)) => {
                    if column_names.is_empty() {
                        // Get column names from first row and sort them for consistent ordering
                        let mut names: Vec<String> = tuple_hashmap.keys().cloned().collect();
                        names.sort();
                        column_names = names;
                    }
                    let row: Vec<Value> = column_names
                        .iter()
                        .map(|col| tuple_hashmap.get(col).cloned().unwrap_or(Value::Null))
                        .collect();
                    data.push(row);
                }
                Ok(None) => break,
                Err(e) => return Err(format!("{:?}", e)),
            }
        }

        mvs.insert(name.clone(), (query, data, column_names));
        drop(mvs);

        log::debug!(
            "create_materialized_view: created materialized view '{}', triggering synchronous save",
            name
        );
        self.force_save()?;
        Ok(())
    }

    pub fn refresh_materialized_view(self: &Arc<Self>, name: &str) -> Result<(), String> {
        let mut mvs = self.materialized_views.write().unwrap();

        let (query, data, column_names) = mvs
            .get_mut(name)
            .ok_or_else(|| format!("Materialized view '{}' does not exist", name))?;

        // Use the planner directly with the full query (including JOINs)
        use crate::planner::planner::Planner;
        let planner = Planner::new_with_catalog(self.clone());
        let mut plan = planner.plan(query).map_err(|e| format!("{:?}", e))?;

        // Collect results
        let mut new_data: Vec<Vec<Value>> = Vec::new();
        loop {
            match plan.next() {
                Ok(Some(tuple_hashmap)) => {
                    let row: Vec<Value> = column_names
                        .iter()
                        .map(|col| tuple_hashmap.get(col).cloned().unwrap_or(Value::Null))
                        .collect();
                    new_data.push(row);
                }
                Ok(None) => break,
                Err(e) => return Err(format!("{:?}", e)),
            }
        }

        *data = new_data;
        drop(mvs);

        self.auto_save();
        Ok(())
    }

    pub fn drop_materialized_view(&self, name: &str, if_exists: bool) -> Result<(), String> {
        let mut mvs = self.materialized_views.write().unwrap();

        if mvs.remove(name).is_none() && !if_exists {
            return Err(format!("Materialized view '{}' does not exist", name));
        }
        drop(mvs);

        self.auto_save();
        Ok(())
    }

    pub fn get_materialized_view(&self, name: &str) -> Option<Vec<Vec<Value>>> {
        let mvs = self.materialized_views.read().unwrap();
        mvs.get(name).map(|(_, data, _)| data.clone())
    }

    pub fn get_materialized_view_with_columns(
        &self,
        name: &str,
    ) -> Option<(Vec<Vec<Value>>, Vec<String>)> {
        let mvs = self.materialized_views.read().unwrap();
        mvs.get(name).map(|(_, data, columns)| (data.clone(), columns.clone()))
    }

    pub fn create_trigger(&self, trigger: CreateTriggerStmt) -> Result<(), String> {
        let manager = TriggerManager::with_triggers(Arc::clone(&self.triggers));
        manager.create_trigger(trigger)?;
        self.auto_save();
        self.flush_saves();
        Ok(())
    }

    pub fn drop_trigger(&self, name: &str, if_exists: bool) -> Result<(), String> {
        let manager = TriggerManager::with_triggers(Arc::clone(&self.triggers));
        manager.drop_trigger(name, if_exists)?;
        self.auto_save();
        self.flush_saves();
        Ok(())
    }

    pub fn get_trigger(&self, name: &str) -> Option<CreateTriggerStmt> {
        let manager = TriggerManager::with_triggers(Arc::clone(&self.triggers));
        manager.get_trigger(name)
    }

    pub fn create_index(&self, index: CreateIndexStmt) -> Result<(), String> {
        let manager = IndexManager::with_indexes(Arc::clone(&self.indexes));
        manager.create_index(index)?;
        self.auto_save();
        self.flush_saves();
        Ok(())
    }

    pub fn drop_index(&self, name: &str, if_exists: bool) -> Result<(), String> {
        let manager = IndexManager::with_indexes(Arc::clone(&self.indexes));
        manager.drop_index(name, if_exists)?;
        self.auto_save();
        Ok(())
    }

    pub fn get_index(&self, name: &str) -> Option<CreateIndexStmt> {
        let manager = IndexManager::with_indexes(Arc::clone(&self.indexes));
        manager.get_index(name)
    }

    pub fn list_tables(&self) -> Vec<String> {
        let manager = TableManager::with_all(
            Arc::clone(&self.tables),
            Arc::clone(&self.data),
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
        );
        manager.list_tables()
    }

    pub fn create_function(&self, func: Function) -> Result<(), String> {
        let manager = FunctionManager::with_functions(Arc::clone(&self.functions));
        manager.create_function(func)?;
        self.auto_save();
        Ok(())
    }

    pub fn drop_function(&self, name: &str, if_exists: bool) -> Result<(), String> {
        let manager = FunctionManager::with_functions(Arc::clone(&self.functions));
        manager.drop_function(name, if_exists)?;
        self.auto_save();
        Ok(())
    }

    pub fn get_function(&self, name: &str, arg_types: &[String]) -> Option<Function> {
        let manager = FunctionManager::with_functions(Arc::clone(&self.functions));
        manager.get_function(name, arg_types)
    }

    pub fn create_aggregate(&self, agg: Aggregate) -> Result<(), String> {
        let manager = AggregateManager::with_aggregates(Arc::clone(&self.aggregates));
        manager.create_aggregate(agg)?;
        self.auto_save();
        Ok(())
    }

    pub fn drop_aggregate(&self, name: &str, if_exists: bool) -> Result<(), String> {
        let manager = AggregateManager::with_aggregates(Arc::clone(&self.aggregates));
        manager.drop_aggregate(name, if_exists)?;
        self.auto_save();
        Ok(())
    }

    pub fn get_aggregate(&self, name: &str) -> Option<Aggregate> {
        let manager = AggregateManager::with_aggregates(Arc::clone(&self.aggregates));
        manager.get_aggregate(name)
    }

    pub fn insert(&self, table: &str, columns: &[String], values: Vec<Expr>) -> Result<(), String> {
        let manager = DataManager::with_all(
            Arc::clone(&self.data),
            Arc::clone(&self.tables),
            Arc::clone(&self.sequences),
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
            Arc::clone(&self.txn_mgr),
            Arc::new(self.clone()),
        );
        manager.insert(table, columns, values)
    }

    pub fn insert_with_txn(
        &self,
        table: &str,
        columns: &[String],
        values: Vec<Expr>,
        txn: &Transaction,
    ) -> Result<(), String> {
        let manager = DataManager::with_all(
            Arc::clone(&self.data),
            Arc::clone(&self.tables),
            Arc::clone(&self.sequences),
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
            Arc::clone(&self.txn_mgr),
            Arc::new(self.clone()),
        );
        manager.insert_with_txn(table, columns, values, txn)
    }

    pub fn row_count(&self, table: &str) -> usize {
        let manager = DataManager::with_all(
            Arc::clone(&self.data),
            Arc::clone(&self.tables),
            Arc::clone(&self.sequences),
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
            Arc::clone(&self.txn_mgr),
            Arc::new(self.clone()),
        );
        manager.row_count(table)
    }

    pub fn batch_insert(
        &self,
        table: &str,
        columns: &[String],
        batch: Vec<Vec<Expr>>,
    ) -> Result<usize, String> {
        let manager = DataManager::with_all(
            Arc::clone(&self.data),
            Arc::clone(&self.tables),
            Arc::clone(&self.sequences),
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
            Arc::clone(&self.txn_mgr),
            Arc::new(self.clone()),
        );
        manager.batch_insert(table, columns, batch)
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
        let manager = DataManager::with_all(
            Arc::clone(&self.data),
            Arc::clone(&self.tables),
            Arc::clone(&self.sequences),
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
            Arc::clone(&self.txn_mgr),
            Arc::new(self.clone()),
        );
        manager.select(
            table_name,
            distinct,
            columns,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        )
    }

    pub fn select_with_catalog(
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
        let manager = DataManager::with_all(
            Arc::clone(&self.data),
            Arc::clone(&self.tables),
            Arc::clone(&self.sequences),
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
            Arc::clone(&self.txn_mgr),
            Arc::new(self.clone()),
        );
        manager.select_with_catalog(
            table_name,
            distinct,
            columns,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        )
    }

    pub fn update(
        &self,
        table: &str,
        assignments: Vec<(String, Expr)>,
        where_clause: Option<Expr>,
    ) -> Result<usize, String> {
        let manager = DataManager::with_all(
            Arc::clone(&self.data),
            Arc::clone(&self.tables),
            Arc::clone(&self.sequences),
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
            Arc::clone(&self.txn_mgr),
            Arc::new(self.clone()),
        );
        manager.update(table, assignments, where_clause)
    }

    pub fn update_with_txn(
        &self,
        table: &str,
        assignments: Vec<(String, Expr)>,
        where_clause: Option<Expr>,
        txn: &Transaction,
    ) -> Result<usize, String> {
        let manager = DataManager::with_all(
            Arc::clone(&self.data),
            Arc::clone(&self.tables),
            Arc::clone(&self.sequences),
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
            Arc::clone(&self.txn_mgr),
            Arc::new(self.clone()),
        );
        manager.update_with_txn(table, assignments, where_clause, txn)
    }

    pub fn delete(&self, table: &str, where_clause: Option<Expr>) -> Result<usize, String> {
        let manager = DataManager::with_all(
            Arc::clone(&self.data),
            Arc::clone(&self.tables),
            Arc::clone(&self.sequences),
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
            Arc::clone(&self.txn_mgr),
            Arc::new(self.clone()),
        );
        manager.delete(table, where_clause)
    }

    pub fn delete_with_txn(
        &self,
        table: &str,
        where_clause: Option<Expr>,
        txn: &Transaction,
    ) -> Result<usize, String> {
        let manager = DataManager::with_all(
            Arc::clone(&self.data),
            Arc::clone(&self.tables),
            Arc::clone(&self.sequences),
            Arc::clone(&self.enum_types),
            Arc::clone(&self.composite_types),
            Arc::clone(&self.txn_mgr),
            Arc::new(self.clone()),
        );
        manager.delete_with_txn(table, where_clause, txn)
    }

    pub fn save_to_disk(&self, data_dir: &str) -> Result<(), String> {
        log::info!("💾 save_to_disk: saving to data_dir={}", data_dir);
        let tables = self.tables.read().unwrap();
        let data = self.data.read().unwrap();
        log::info!(
            "💾 save_to_disk: {} tables, {} total rows",
            tables.len(),
            data.values().map(|v| v.len()).sum::<usize>()
        );
        let result = Persistence::save(data_dir, &tables, &data);
        if result.is_ok() {
            Persistence::save_partitions(data_dir, &tables)?;
            log::info!("✅ save_to_disk: save completed successfully");
        } else {
            log::error!("❌ save_to_disk: save failed: {:?}", result);
        }
        result
    }

    /// Force an immediate synchronous save of all catalog data
    /// This blocks until the save is complete
    pub fn force_save(&self) -> Result<(), String> {
        if let Some(ref data_dir) = self.data_dir {
            log::info!("💾 force_save: forcing immediate synchronous save to {}", data_dir);

            // Wait for any pending background saves to complete
            std::thread::sleep(std::time::Duration::from_millis(500));

            // Do a direct synchronous save to ensure data is persisted
            let tables = self.tables.read().unwrap();
            let data = self.data.read().unwrap();
            let views = self.views.read().unwrap();
            let materialized_views = self.materialized_views.read().unwrap();
            let triggers = self.triggers.read().unwrap();
            let indexes = self.indexes.read().unwrap();
            let functions = self.functions.read().unwrap();

            log::info!(
                "💾 force_save: saving {} tables with {} rows",
                tables.len(),
                data.values().map(|v| v.len()).sum::<usize>()
            );

            // Save all catalog components
            Persistence::save(data_dir, &tables, &data)?;
            Persistence::save_views(data_dir, &views)?;
            Persistence::save_materialized_views(data_dir, &materialized_views)?;
            Persistence::save_triggers(data_dir, &triggers)?;
            Persistence::save_indexes(data_dir, &indexes)?;
            Persistence::save_functions(data_dir, &functions)?;
            Persistence::save_partitions(data_dir, &tables)?;

            log::info!("✅ force_save: synchronous save completed successfully");
            return Ok(());
        }
        log::debug!("force_save: no data_dir configured, skipping");
        Ok(())
    }

    pub fn load_from_disk(&self, data_dir: &str) -> Result<(), String> {
        log::info!("📂 load_from_disk: loading from data_dir={}", data_dir);
        let mut tables = self.tables.write().unwrap();
        let mut data = self.data.write().unwrap();
        let result = Persistence::load(data_dir, &mut tables, &mut data, &self.txn_mgr);
        if result.is_ok() {
            log::info!(
                "✅ load_from_disk: loaded {} tables, {} total rows",
                tables.len(),
                data.values().map(|v| v.len()).sum::<usize>()
            );

            drop(tables);
            drop(data);

            if let Ok(partitions) = Persistence::load_partitions(data_dir) {
                if !partitions.is_empty() {
                    let num_partitions = partitions.len();
                    let mut tables = self.tables.write().unwrap();
                    for (name, partition_info) in partitions {
                        if let Some(table) = tables.get_mut(&name) {
                            table.partition_method = partition_info.partition_method;
                            table.partition_keys = partition_info.partition_keys;
                            table.is_partition = partition_info.is_partition;
                            table.parent_table = partition_info.parent_table;
                            table.partition_bound = partition_info.partition_bound;
                        }
                    }
                    log::info!(
                        "📂 load_from_disk: applied partition info to {} tables",
                        num_partitions
                    );
                }
            }
        } else {
            log::error!("❌ load_from_disk: load failed: {:?}", result);
        }
        result
    }

    pub fn begin_transaction(&self) -> Result<Transaction, String> {
        let manager = TransactionManager2::with_all(
            Arc::clone(&self.active_txn),
            Arc::clone(&self.txn_mgr),
            Arc::clone(&self.savepoints),
            Arc::clone(&self.data),
            Arc::clone(&self.tables),
        );
        manager.begin_transaction()
    }

    pub fn begin_transaction_with_isolation(
        &self,
        level: IsolationLevel,
    ) -> Result<Transaction, String> {
        let manager = TransactionManager2::with_all(
            Arc::clone(&self.active_txn),
            Arc::clone(&self.txn_mgr),
            Arc::clone(&self.savepoints),
            Arc::clone(&self.data),
            Arc::clone(&self.tables),
        );
        manager.begin_transaction_with_isolation(level)
    }

    pub fn set_transaction_isolation(&self, level: IsolationLevel) -> Result<(), String> {
        let manager = TransactionManager2::with_all(
            Arc::clone(&self.active_txn),
            Arc::clone(&self.txn_mgr),
            Arc::clone(&self.savepoints),
            Arc::clone(&self.data),
            Arc::clone(&self.tables),
        );
        manager.set_transaction_isolation(level)
    }

    pub fn commit_transaction(&self) -> Result<(), String> {
        let manager = TransactionManager2::with_all(
            Arc::clone(&self.active_txn),
            Arc::clone(&self.txn_mgr),
            Arc::clone(&self.savepoints),
            Arc::clone(&self.data),
            Arc::clone(&self.tables),
        );
        manager.commit_transaction()
    }

    pub fn rollback_transaction(&self) -> Result<(), String> {
        let manager = TransactionManager2::with_all(
            Arc::clone(&self.active_txn),
            Arc::clone(&self.txn_mgr),
            Arc::clone(&self.savepoints),
            Arc::clone(&self.data),
            Arc::clone(&self.tables),
        );
        manager.rollback_transaction()
    }

    pub fn savepoint(&self, name: String) -> Result<(), String> {
        let manager = TransactionManager2::with_all(
            Arc::clone(&self.active_txn),
            Arc::clone(&self.txn_mgr),
            Arc::clone(&self.savepoints),
            Arc::clone(&self.data),
            Arc::clone(&self.tables),
        );
        manager.savepoint(name)
    }

    pub fn rollback_to_savepoint(&self, name: &str) -> Result<(), String> {
        let manager = TransactionManager2::with_all(
            Arc::clone(&self.active_txn),
            Arc::clone(&self.txn_mgr),
            Arc::clone(&self.savepoints),
            Arc::clone(&self.data),
            Arc::clone(&self.tables),
        );
        manager.rollback_to_savepoint(name)
    }

    pub fn release_savepoint(&self, name: &str) -> Result<(), String> {
        let manager = TransactionManager2::with_all(
            Arc::clone(&self.active_txn),
            Arc::clone(&self.txn_mgr),
            Arc::clone(&self.savepoints),
            Arc::clone(&self.data),
            Arc::clone(&self.tables),
        );
        manager.release_savepoint(name)
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
    use crate::parser::ast::{ColumnDef, DataType};

    #[test]
    fn test_create_and_get_table() {
        let catalog = Catalog::new();
        let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];
        assert!(catalog.create_table("users".to_string(), columns.clone()).is_ok());

        let schema = catalog.get_table("users").unwrap();
        assert_eq!(schema.name, "users");
        assert_eq!(schema.columns, columns);
    }

    #[test]
    fn test_create_table_already_exists() {
        let catalog = Catalog::new();
        let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];
        catalog.create_table("users".to_string(), columns.clone()).unwrap();

        let result = catalog.create_table("users".to_string(), columns);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Table 'users' already exists");
    }

    #[test]
    fn test_get_table_not_exists() {
        let catalog = Catalog::new();
        assert!(catalog.get_table("users").is_none());
    }

    #[test]
    fn test_drop_table() {
        let catalog = Catalog::new();
        let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];
        catalog.create_table("users".to_string(), columns).unwrap();

        assert!(catalog.drop_table("users", false).is_ok());
        assert!(catalog.get_table("users").is_none());
    }

    #[test]
    fn test_drop_table_not_exists() {
        let catalog = Catalog::new();
        let result = catalog.drop_table("users", false);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Table 'users' does not exist");
    }

    #[test]
    fn test_drop_table_if_exists() {
        let catalog = Catalog::new();
        assert!(catalog.drop_table("users", true).is_ok());
    }
}
