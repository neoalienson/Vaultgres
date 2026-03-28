use crate::catalog::tuple::Tuple;
use crate::transaction::{IsolationLevel, Transaction, TransactionManager};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct TransactionManager2 {
    pub(crate) active_txn: Arc<RwLock<Option<Transaction>>>,
    pub(crate) txn_mgr: Arc<TransactionManager>,
    pub(crate) savepoints: Arc<RwLock<HashMap<String, Vec<Tuple>>>>,
    pub(crate) data: Arc<RwLock<HashMap<String, Vec<Tuple>>>>,
    pub(crate) tables: Arc<RwLock<HashMap<String, crate::catalog::TableSchema>>>,
}

impl TransactionManager2 {
    pub fn new() -> Self {
        Self {
            active_txn: Arc::new(RwLock::new(None)),
            txn_mgr: Arc::new(TransactionManager::new()),
            savepoints: Arc::new(RwLock::new(HashMap::new())),
            data: Arc::new(RwLock::new(HashMap::new())),
            tables: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn with_all(
        active_txn: Arc<RwLock<Option<Transaction>>>,
        txn_mgr: Arc<TransactionManager>,
        savepoints: Arc<RwLock<HashMap<String, Vec<Tuple>>>>,
        data: Arc<RwLock<HashMap<String, Vec<Tuple>>>>,
        tables: Arc<RwLock<HashMap<String, crate::catalog::TableSchema>>>,
    ) -> Self {
        Self { active_txn, txn_mgr, savepoints, data, tables }
    }

    pub fn begin_transaction(&self) -> Result<Transaction, String> {
        self.begin_transaction_with_isolation(IsolationLevel::ReadCommitted)
    }

    pub fn begin_transaction_with_isolation(
        &self,
        level: IsolationLevel,
    ) -> Result<Transaction, String> {
        let mut active = self.active_txn.write().unwrap();
        if active.is_some() {
            return Err("Transaction already in progress".to_string());
        }
        let txn = self.txn_mgr.begin_with_isolation(level);
        *active = Some(txn.clone());
        Ok(txn)
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
