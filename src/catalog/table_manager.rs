use super::partition_pruning::PartitionPruner;
use crate::catalog::schema::TableSchema;
use crate::parser::ast::{
    AttachPartitionStmt, ColumnDef, DataType, DetachPartitionStmt, Expr, ForeignKeyAction,
    ForeignKeyDef, PartitionBoundSpec,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct TableManager {
    pub(crate) tables: Arc<RwLock<HashMap<String, TableSchema>>>,
    pub(crate) data: Arc<RwLock<HashMap<String, Vec<crate::catalog::tuple::Tuple>>>>,
    pub(crate) enum_types: Arc<RwLock<HashMap<String, crate::parser::ast::EnumTypeDef>>>,
    pub(crate) composite_types: Arc<RwLock<HashMap<String, crate::parser::ast::CompositeTypeDef>>>,
}

impl TableManager {
    pub fn new() -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            data: Arc::new(RwLock::new(HashMap::new())),
            enum_types: Arc::new(RwLock::new(HashMap::new())),
            composite_types: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn with_all(
        tables: Arc<RwLock<HashMap<String, TableSchema>>>,
        data: Arc<RwLock<HashMap<String, Vec<crate::catalog::tuple::Tuple>>>>,
        enum_types: Arc<RwLock<HashMap<String, crate::parser::ast::EnumTypeDef>>>,
        composite_types: Arc<RwLock<HashMap<String, crate::parser::ast::CompositeTypeDef>>>,
    ) -> Self {
        Self { tables, data, enum_types, composite_types }
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

        let mut pk = primary_key;
        if pk.is_none() {
            let pk_cols: Vec<String> =
                columns.iter().filter(|c| c.is_primary_key).map(|c| c.name.clone()).collect();
            if !pk_cols.is_empty() {
                pk = Some(pk_cols);
            }
        }

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

        for fk in &fks {
            if !tables.contains_key(&fk.ref_table) {
                return Err(format!("Referenced table '{}' does not exist", fk.ref_table));
            }
        }

        let composite_types = self.composite_types.read().unwrap();
        let enum_types = self.enum_types.read().unwrap();
        for col in &columns {
            if let DataType::Composite(ref type_name) = col.data_type {
                if !composite_types.contains_key(type_name) && !enum_types.contains_key(type_name) {
                    return Err(format!("type '{}' does not exist", type_name));
                }
            }
            if let DataType::Enum(ref type_name) = col.data_type {
                if !enum_types.contains_key(type_name) {
                    return Err(format!("type '{}' does not exist", type_name));
                }
            }
        }
        drop(composite_types);
        drop(enum_types);

        tables.insert(name.clone(), TableSchema::with_constraints(name.clone(), columns, pk, fks));
        drop(tables);

        let mut data = self.data.write().unwrap();
        data.insert(name.clone(), Vec::new());
        Ok(())
    }

    pub fn create_partitioned_table(&self, schema: TableSchema) -> Result<(), String> {
        let name = schema.name.clone();

        {
            let enum_types = self.enum_types.read().unwrap();
            let composite_types = self.composite_types.read().unwrap();
            for col in &schema.columns {
                if let DataType::Enum(ref type_name) = col.data_type {
                    if !enum_types.contains_key(type_name) {
                        return Err(format!("type '{}' does not exist", type_name));
                    }
                }
                if let DataType::Composite(ref type_name) = col.data_type {
                    if !composite_types.contains_key(type_name) {
                        return Err(format!("type '{}' does not exist", type_name));
                    }
                }
            }
        }

        let mut tables = self.tables.write().unwrap();

        if tables.contains_key(&name) {
            return Err(format!("Table '{}' already exists", name));
        }

        tables.insert(name.clone(), schema);
        drop(tables);

        let mut data = self.data.write().unwrap();
        data.insert(name.clone(), Vec::new());
        Ok(())
    }

    pub fn create_partition(&self, schema: TableSchema) -> Result<(), String> {
        let name = schema.name.clone();
        let parent_table = schema.parent_table.clone();

        {
            let tables = self.tables.read().unwrap();
            if !parent_table.as_ref().map(|p| tables.contains_key(p)).unwrap_or(false) {
                return Err(format!(
                    "Parent table '{}' does not exist",
                    parent_table.as_ref().unwrap()
                ));
            }
        }

        let mut tables = self.tables.write().unwrap();
        tables.insert(name.clone(), schema);
        drop(tables);

        let mut data = self.data.write().unwrap();
        data.insert(name.clone(), Vec::new());
        Ok(())
    }

    pub fn attach_partition(&self, stmt: &AttachPartitionStmt) -> Result<(), String> {
        let mut tables = self.tables.write().unwrap();

        let parent_exists = tables.contains_key(&stmt.parent_table);
        if !parent_exists {
            return Err(format!("Parent table '{}' does not exist", stmt.parent_table));
        }

        let partition_exists = tables.contains_key(&stmt.partition_name);
        if !partition_exists {
            return Err(format!("Partition '{}' does not exist", stmt.partition_name));
        }

        let partition_schema = tables.get_mut(&stmt.partition_name).unwrap();
        partition_schema.is_partition = true;
        partition_schema.parent_table = Some(stmt.parent_table.clone());
        partition_schema.partition_bound = Some(stmt.bound.clone());

        log::debug!(
            "attach_partition: attached partition '{}' to '{}'",
            stmt.partition_name,
            stmt.parent_table
        );
        Ok(())
    }

    pub fn detach_partition(&self, stmt: &DetachPartitionStmt) -> Result<(), String> {
        let mut tables = self.tables.write().unwrap();

        let partition_exists = tables.contains_key(&stmt.partition_name);
        if !partition_exists {
            return Err(format!("Partition '{}' does not exist", stmt.partition_name));
        }

        let partition_schema = tables.get_mut(&stmt.partition_name).unwrap();
        if !partition_schema.is_partition {
            return Err(format!("Table '{}' is not a partition", stmt.partition_name));
        }
        if partition_schema.parent_table.as_ref() != Some(&stmt.parent_table) {
            return Err(format!(
                "Partition '{}' is not attached to '{}'",
                stmt.partition_name, stmt.parent_table
            ));
        }

        partition_schema.is_partition = false;
        partition_schema.parent_table = None;
        partition_schema.partition_bound = None;

        log::debug!(
            "detach_partition: detached partition '{}' from '{}'",
            stmt.partition_name,
            stmt.parent_table
        );
        Ok(())
    }

    pub fn get_partitions(&self, parent_table: &str) -> Vec<String> {
        let tables = self.tables.read().unwrap();
        tables
            .values()
            .filter(|s| {
                s.is_partition && s.parent_table.as_ref() == Some(&parent_table.to_string())
            })
            .map(|s| s.name.clone())
            .collect()
    }

    pub fn get_partitions_for_predicate(
        &self,
        parent_table: &str,
        where_clause: &Option<Expr>,
    ) -> Vec<String> {
        let tables = self.tables.read().unwrap();

        let parent = match tables.get(parent_table) {
            Some(t) if t.partition_method.is_some() => t,
            _ => return Vec::new(),
        };

        let partition_method = parent.partition_method.as_ref().unwrap();
        let partition_keys = &parent.partition_keys;

        let partitions: Vec<(String, PartitionBoundSpec)> = tables
            .values()
            .filter(|s| {
                s.is_partition && s.parent_table.as_ref() == Some(&parent_table.to_string())
            })
            .filter_map(|s| s.partition_bound.as_ref().map(|bound| (s.name.clone(), bound.clone())))
            .collect();

        let predicates = PartitionPruner::extract_predicates(where_clause, partition_keys);

        match partition_method {
            crate::parser::ast::PartitionMethod::Range => {
                let range_partitions: Vec<(String, crate::parser::ast::PartitionRangeBound)> =
                    partitions
                        .into_iter()
                        .filter_map(|(name, bound)| {
                            if let PartitionBoundSpec::Range(rb) = bound {
                                Some((name, rb))
                            } else {
                                None
                            }
                        })
                        .collect();
                PartitionPruner::prune_partitions_range(&range_partitions, &predicates)
            }
            crate::parser::ast::PartitionMethod::List => {
                let list_partitions: Vec<(String, crate::parser::ast::PartitionListBound)> =
                    partitions
                        .into_iter()
                        .filter_map(|(name, bound)| {
                            if let PartitionBoundSpec::List(lb) = bound {
                                Some((name, lb))
                            } else {
                                None
                            }
                        })
                        .collect();
                PartitionPruner::prune_partitions_list(&list_partitions, &predicates)
            }
            crate::parser::ast::PartitionMethod::Hash => {
                let hash_partitions: Vec<(String, crate::parser::ast::PartitionHashBound)> =
                    partitions
                        .into_iter()
                        .filter_map(|(name, bound)| {
                            if let PartitionBoundSpec::Hash(hb) = bound {
                                Some((name, hb))
                            } else {
                                None
                            }
                        })
                        .collect();
                PartitionPruner::prune_partitions_hash(&hash_partitions, &predicates)
            }
        }
    }

    pub fn is_partitioned_table(&self, name: &str) -> bool {
        let tables = self.tables.read().unwrap();
        if let Some(schema) = tables.get(name) { schema.partition_method.is_some() } else { false }
    }

    pub fn is_partition(&self, name: &str) -> bool {
        let tables = self.tables.read().unwrap();
        if let Some(schema) = tables.get(name) { schema.is_partition } else { false }
    }

    pub fn get_parent_table(&self, partition: &str) -> Option<String> {
        let tables = self.tables.read().unwrap();
        tables.get(partition).and_then(|s| s.parent_table.clone())
    }

    pub fn drop_table(&self, name: &str, if_exists: bool) -> Result<(), String> {
        let mut tables = self.tables.write().unwrap();

        if tables.remove(name).is_none() && !if_exists {
            return Err(format!("Table '{}' does not exist", name));
        }
        drop(tables);

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
}
