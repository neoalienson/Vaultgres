use super::error::{Result, TransactionError};
use super::manager::TransactionId;
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Lock mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockMode {
    Shared,
    Exclusive,
}

/// Lock key (table or tuple identifier)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LockKey {
    pub table_id: u32,
    pub tuple_id: Option<u64>,
}

impl LockKey {
    /// Creates a table-level lock key
    pub fn table(table_id: u32) -> Self {
        Self { table_id, tuple_id: None }
    }

    /// Creates a tuple-level lock key
    pub fn tuple(table_id: u32, tuple_id: u64) -> Self {
        Self { table_id, tuple_id: Some(tuple_id) }
    }
}

/// Lock entry
#[derive(Debug)]
struct LockEntry {
    holders: Vec<(TransactionId, LockMode)>,
    waiters: Vec<(TransactionId, LockMode, Instant)>,
}

/// Lock manager
pub struct LockManager {
    locks: Arc<DashMap<LockKey, LockEntry>>,
    wait_for: Arc<DashMap<TransactionId, HashSet<TransactionId>>>,
    lock_timeout: Duration,
}

impl LockManager {
    /// Creates a new lock manager
    pub fn new() -> Self {
        Self::with_timeout(Duration::from_secs(30))
    }

    /// Creates a new lock manager with custom timeout
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            locks: Arc::new(DashMap::new()),
            wait_for: Arc::new(DashMap::new()),
            lock_timeout: timeout,
        }
    }

    /// Acquires a lock
    pub fn acquire(&self, xid: TransactionId, key: LockKey, mode: LockMode) -> Result<()> {
        let mut entry =
            self.locks.entry(key).or_insert(LockEntry { holders: Vec::new(), waiters: Vec::new() });

        // Check compatibility
        let mut blocking_txns = HashSet::new();
        for (holder_xid, holder_mode) in &entry.holders {
            if *holder_xid == xid {
                return Ok(());
            }

            if !self.is_compatible(mode, *holder_mode) {
                blocking_txns.insert(*holder_xid);
            }
        }

        if !blocking_txns.is_empty() {
            // Check for deadlock
            if self.would_deadlock(xid, &blocking_txns) {
                return Err(TransactionError::Deadlock);
            }

            // Check for timeout
            let now = Instant::now();
            entry.waiters.retain(|(waiter_xid, _, start_time)| {
                *waiter_xid != xid && now.duration_since(*start_time) < self.lock_timeout
            });

            // Check if this transaction has been waiting too long
            if let Some((_, _, start_time)) = entry.waiters.iter().find(|(w, _, _)| *w == xid) {
                if now.duration_since(*start_time) >= self.lock_timeout {
                    entry.waiters.retain(|(w, _, _)| *w != xid);
                    return Err(TransactionError::LockTimeout);
                }
            } else {
                entry.waiters.push((xid, mode, now));
            }

            // Record wait-for relationship
            self.wait_for.insert(xid, blocking_txns);
            return Err(TransactionError::Deadlock);
        }

        entry.holders.push((xid, mode));
        entry.waiters.retain(|(w, _, _)| *w != xid);
        self.wait_for.remove(&xid);
        Ok(())
    }

    /// Releases a lock
    pub fn release(&self, xid: TransactionId, key: LockKey) -> Result<()> {
        if let Some(mut entry) = self.locks.get_mut(&key) {
            entry.holders.retain(|(holder_xid, _)| *holder_xid != xid);

            if entry.holders.is_empty() {
                drop(entry);
                self.locks.remove(&key);
            }
        }
        Ok(())
    }

    /// Releases all locks held by a transaction
    pub fn release_all(&self, xid: TransactionId) {
        let keys: Vec<LockKey> = self
            .locks
            .iter()
            .filter(|entry| entry.holders.iter().any(|(holder_xid, _)| *holder_xid == xid))
            .map(|entry| *entry.key())
            .collect();

        for key in keys {
            let _ = self.release(xid, key);
        }

        self.wait_for.remove(&xid);
    }

    /// Detects deadlock using cycle detection in wait-for graph
    fn would_deadlock(&self, xid: TransactionId, blocking: &HashSet<TransactionId>) -> bool {
        let mut visited = HashSet::new();
        let mut stack = HashSet::new();

        for &blocker in blocking {
            if self.has_cycle(blocker, xid, &mut visited, &mut stack) {
                return true;
            }
        }
        false
    }

    /// DFS to detect cycle in wait-for graph
    fn has_cycle(
        &self,
        current: TransactionId,
        target: TransactionId,
        visited: &mut HashSet<TransactionId>,
        stack: &mut HashSet<TransactionId>,
    ) -> bool {
        if current == target {
            return true;
        }

        if visited.contains(&current) {
            return false;
        }

        visited.insert(current);
        stack.insert(current);

        if let Some(waiting_for) = self.wait_for.get(&current) {
            for &next in waiting_for.iter() {
                if stack.contains(&next) || self.has_cycle(next, target, visited, stack) {
                    return true;
                }
            }
        }

        stack.remove(&current);
        false
    }

    /// Checks if two lock modes are compatible
    fn is_compatible(&self, mode1: LockMode, mode2: LockMode) -> bool {
        matches!((mode1, mode2), (LockMode::Shared, LockMode::Shared))
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acquire_shared_lock() {
        let mgr = LockManager::new();
        let key = LockKey::table(1);

        mgr.acquire(1, key, LockMode::Shared).unwrap();
        mgr.acquire(2, key, LockMode::Shared).unwrap();
    }

    #[test]
    fn test_acquire_exclusive_lock() {
        let mgr = LockManager::new();
        let key = LockKey::table(1);

        mgr.acquire(1, key, LockMode::Exclusive).unwrap();
        assert!(mgr.acquire(2, key, LockMode::Exclusive).is_err());
    }

    #[test]
    fn test_release_lock() {
        let mgr = LockManager::new();
        let key = LockKey::table(1);

        mgr.acquire(1, key, LockMode::Exclusive).unwrap();
        mgr.release(1, key).unwrap();
        mgr.acquire(2, key, LockMode::Exclusive).unwrap();
    }

    #[test]
    fn test_release_all_locks() {
        let mgr = LockManager::new();

        mgr.acquire(1, LockKey::table(1), LockMode::Shared).unwrap();
        mgr.acquire(1, LockKey::table(2), LockMode::Shared).unwrap();

        mgr.release_all(1);

        mgr.acquire(2, LockKey::table(1), LockMode::Exclusive).unwrap();
    }
}
