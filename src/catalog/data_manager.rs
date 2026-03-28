use super::insert_validator::InsertValidator;
use super::tuple::Tuple;
use super::update_delete_executor::UpdateDeleteExecutor;
use super::value::Value;
use crate::catalog::schema::TableSchema;
use crate::parser::ast::{
    ColumnDef, CompositeTypeDef, DataType, EnumTypeDef, Expr, ForeignKeyAction, ForeignKeyDef,
    OrderByExpr, SelectStmt,
};
use crate::transaction::{Transaction, TransactionManager};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

pub struct DataManager {
    pub(crate) data: Arc<RwLock<HashMap<String, Vec<Tuple>>>>,
    pub(crate) tables: Arc<RwLock<HashMap<String, TableSchema>>>,
    pub(crate) sequences: Arc<RwLock<HashMap<String, i64>>>,
    pub(crate) enum_types: Arc<RwLock<HashMap<String, EnumTypeDef>>>,
    pub(crate) composite_types: Arc<RwLock<HashMap<String, CompositeTypeDef>>>,
    pub(crate) txn_mgr: Arc<TransactionManager>,
    pub(crate) catalog: Arc<crate::catalog::Catalog>,
}

impl DataManager {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            tables: Arc::new(RwLock::new(HashMap::new())),
            sequences: Arc::new(RwLock::new(HashMap::new())),
            enum_types: Arc::new(RwLock::new(HashMap::new())),
            composite_types: Arc::new(RwLock::new(HashMap::new())),
            txn_mgr: Arc::new(TransactionManager::new()),
            catalog: Arc::new(crate::catalog::Catalog::new()),
        }
    }

    pub fn with_all(
        data: Arc<RwLock<HashMap<String, Vec<Tuple>>>>,
        tables: Arc<RwLock<HashMap<String, TableSchema>>>,
        sequences: Arc<RwLock<HashMap<String, i64>>>,
        enum_types: Arc<RwLock<HashMap<String, EnumTypeDef>>>,
        composite_types: Arc<RwLock<HashMap<String, CompositeTypeDef>>>,
        txn_mgr: Arc<TransactionManager>,
        catalog: Arc<crate::catalog::Catalog>,
    ) -> Self {
        Self { data, tables, sequences, enum_types, composite_types, txn_mgr, catalog }
    }

    pub fn insert(&self, table: &str, columns: &[String], values: Vec<Expr>) -> Result<(), String> {
        let txn = self.txn_mgr.begin();
        let result = self.insert_with_txn(table, columns, values, &txn);
        self.txn_mgr.commit(txn.xid).map_err(|e| e.to_string())?;
        result
    }

    pub fn insert_with_txn(
        &self,
        table: &str,
        columns: &[String],
        values: Vec<Expr>,
        txn: &Transaction,
    ) -> Result<(), String> {
        let schema = self
            .tables
            .read()
            .unwrap()
            .get(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?
            .clone();

        let num_cols = if columns.is_empty() { schema.columns.len() } else { columns.len() };

        if values.len() > num_cols {
            return Err(format!("Too many values: expected {}, got {}", num_cols, values.len()));
        }

        let header = crate::transaction::TupleHeader::new(txn.xid);

        let tuple_data: Result<Vec<Value>, String> = schema
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                if columns.is_empty() {
                    InsertValidator::resolve_value(
                        col,
                        i,
                        &values,
                        table,
                        &self.sequences,
                        &self.enum_types,
                        &self.composite_types,
                    )
                } else {
                    if let Some(value_idx) = columns.iter().position(|c| c == &col.name) {
                        if value_idx < values.len() {
                            InsertValidator::resolve_value(
                                col,
                                value_idx,
                                &values,
                                table,
                                &self.sequences,
                                &self.enum_types,
                                &self.composite_types,
                            )
                        } else {
                            Err(format!("Column {} has no value", col.name))
                        }
                    } else {
                        InsertValidator::resolve_value(
                            col,
                            schema.columns.len(),
                            &values,
                            table,
                            &self.sequences,
                            &self.enum_types,
                            &self.composite_types,
                        )
                    }
                }
            })
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

        let tuple = Tuple { header, data: tuple_data, column_map: HashMap::new() };

        let mut data = self.data.write().unwrap();
        data.get_mut(table).unwrap().push(tuple);
        drop(data);

        log::debug!("insert: inserted row into '{}', triggering synchronous save", table);
        self.catalog.force_save()?;
        Ok(())
    }

    pub fn row_count(&self, table: &str) -> usize {
        let data = self.data.read().unwrap();
        data.get(table)
            .map(|rows| {
                let snapshot = self.txn_mgr.get_snapshot();
                rows.iter()
                    .filter(|tuple| tuple.header.is_visible(&snapshot, &self.txn_mgr))
                    .count()
            })
            .unwrap_or(0)
    }

    pub fn batch_insert(
        &self,
        table: &str,
        columns: &[String],
        batch: Vec<Vec<Expr>>,
    ) -> Result<usize, String> {
        if batch.is_empty() {
            return Ok(0);
        }

        let schema = self
            .tables
            .read()
            .unwrap()
            .get(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?
            .clone();

        let num_cols = if columns.is_empty() { schema.columns.len() } else { columns.len() };

        let txn = self.txn_mgr.begin();
        let header = crate::transaction::TupleHeader::new(txn.xid);

        let tuples: Result<Vec<Tuple>, String> = batch
            .into_iter()
            .map(|values| {
                if values.len() > num_cols {
                    return Err(format!(
                        "Too many values: expected {}, got {}",
                        num_cols,
                        values.len()
                    ));
                }

                let tuple_data: Result<Vec<Value>, String> = schema
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, col)| {
                        if columns.is_empty() {
                            InsertValidator::resolve_value(
                                col,
                                i,
                                &values,
                                table,
                                &self.sequences,
                                &self.enum_types,
                                &self.composite_types,
                            )
                        } else {
                            if let Some(value_idx) = columns.iter().position(|c| c == &col.name) {
                                if value_idx < values.len() {
                                    InsertValidator::resolve_value(
                                        col,
                                        value_idx,
                                        &values,
                                        table,
                                        &self.sequences,
                                        &self.enum_types,
                                        &self.composite_types,
                                    )
                                } else {
                                    Err(format!("Column {} has no value", col.name))
                                }
                            } else {
                                InsertValidator::resolve_value(
                                    col,
                                    schema.columns.len(),
                                    &values,
                                    table,
                                    &self.sequences,
                                    &self.enum_types,
                                    &self.composite_types,
                                )
                            }
                        }
                    })
                    .collect();

                Ok(Tuple { header, data: tuple_data?, column_map: HashMap::new() })
            })
            .collect();

        let tuples = tuples?;
        let count = tuples.len();

        let mut data = self.data.write().unwrap();
        data.get_mut(table).unwrap().extend(tuples);
        drop(data);

        self.txn_mgr.commit(txn.xid).map_err(|e| e.to_string())?;
        log::debug!(
            "batch_insert: inserted {} rows into '{}', triggering synchronous save",
            count,
            table
        );
        self.catalog.force_save()?;
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

        use crate::planner::planner::Planner;
        let planner = Planner::new_without_catalog();
        let mut plan = planner.plan(&select_stmt).map_err(|e| format!("{:?}", e))?;

        let mut rows: Vec<Vec<Value>> = Vec::new();
        let mut output_column_names: Option<Vec<String>> = None;

        loop {
            match plan.next() {
                Ok(Some(tuple_hashmap)) => {
                    let mut row = Vec::new();

                    if output_column_names.is_none() {
                        let mut keys: Vec<String> = tuple_hashmap.keys().cloned().collect();
                        keys.sort();
                        output_column_names = Some(keys);
                    }

                    if let Some(ref col_names) = output_column_names {
                        for col_name in col_names {
                            row.push(tuple_hashmap.get(col_name).cloned().unwrap_or(Value::Null));
                        }
                    }
                    rows.push(row);
                }
                Ok(None) => break,
                Err(e) => return Err(format!("{:?}", e)),
            }
        }

        Ok(rows)
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

        use crate::planner::planner::Planner;
        let planner = Planner::new_with_catalog(self.catalog.clone());
        let mut plan = planner.plan(&select_stmt).map_err(|e| format!("{:?}", e))?;

        let mut rows: Vec<Vec<Value>> = Vec::new();
        let mut output_column_names: Option<Vec<String>> = None;

        loop {
            match plan.next() {
                Ok(Some(tuple_hashmap)) => {
                    let mut row = Vec::new();

                    if output_column_names.is_none() {
                        let mut keys: Vec<String> = tuple_hashmap.keys().cloned().collect();
                        keys.sort();
                        output_column_names = Some(keys);
                    }

                    if let Some(ref col_names) = output_column_names {
                        for col_name in col_names {
                            row.push(tuple_hashmap.get(col_name).cloned().unwrap_or(Value::Null));
                        }
                    }
                    rows.push(row);
                }
                Ok(None) => break,
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
        let txn = self.txn_mgr.begin();
        let result = self.update_with_txn(table, assignments, where_clause, &txn);
        self.txn_mgr.commit(txn.xid).map_err(|e| e.to_string())?;
        result
    }

    pub fn update_with_txn(
        &self,
        table: &str,
        assignments: Vec<(String, Expr)>,
        where_clause: Option<Expr>,
        txn: &Transaction,
    ) -> Result<usize, String> {
        let schema = self
            .tables
            .read()
            .unwrap()
            .get(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?
            .clone();

        let snapshot = txn.snapshot.clone();

        let table_tuples = {
            let data = self.data.read().unwrap();
            data.get(table).ok_or_else(|| format!("Table '{}' has no data", table))?.clone()
        };

        let mut data = self.data.write().unwrap();
        let tuples = data.get_mut(table).ok_or_else(|| format!("Table '{}' has no data", table))?;

        let updated = UpdateDeleteExecutor::update_with_tuples(
            tuples,
            &assignments,
            &where_clause,
            &schema,
            &snapshot,
            &self.txn_mgr,
            &self.catalog,
            &table_tuples,
        )?;

        self.catalog.auto_save();
        Ok(updated)
    }

    pub fn delete(&self, table: &str, where_clause: Option<Expr>) -> Result<usize, String> {
        let txn = self.txn_mgr.begin();
        let result = self.delete_with_txn(table, where_clause, &txn);
        self.txn_mgr.commit(txn.xid).map_err(|e| e.to_string())?;
        result
    }

    pub fn delete_with_txn(
        &self,
        table: &str,
        where_clause: Option<Expr>,
        txn: &Transaction,
    ) -> Result<usize, String> {
        let schema = self
            .tables
            .read()
            .unwrap()
            .get(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?
            .clone();

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
            &self.catalog,
        )?;

        self.catalog.auto_save();
        Ok(deleted)
    }
}
