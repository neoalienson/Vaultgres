use super::executor::{Executor, ExecutorError, Tuple};
use crate::catalog::TableSchema;
use crate::transaction::TransactionManager;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct SeqScanExecutor {
    schema: TableSchema,
    data: Vec<crate::catalog::Tuple>,
    current_idx: usize,
    txn_mgr: Arc<TransactionManager>,
}

impl SeqScanExecutor {
    pub fn new(
        table: String,
        schema: TableSchema,
        data: Arc<RwLock<HashMap<String, Vec<crate::catalog::Tuple>>>>,
        txn_mgr: Arc<TransactionManager>,
    ) -> Self {
        let data_guard = data.read().unwrap();
        let table_data = data_guard.get(&table).cloned().unwrap_or_default();
        drop(data_guard);

        Self { schema, data: table_data, current_idx: 0, txn_mgr }
    }
}

impl Executor for SeqScanExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        while self.current_idx < self.data.len() {
            let catalog_tuple = &self.data[self.current_idx];
            self.current_idx += 1;

            // Check tuple visibility using transaction manager
            // For now, skip visibility check to debug aggregation issues
            // let snapshot = self.txn_mgr.get_snapshot();
            // if !catalog_tuple.header.is_visible(&snapshot, &self.txn_mgr) {
            //     continue; // Skip invisible tuples
            // }

            // Convert catalog tuple to HashMap format
            let mut tuple_map = HashMap::new();
            for (i, column) in self.schema.columns.iter().enumerate() {
                tuple_map.insert(column.name.clone(), catalog_tuple.data[i].clone());
            }

            return Ok(Some(tuple_map));
        }

        Ok(None)
    }
}
