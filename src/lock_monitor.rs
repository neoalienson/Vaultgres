use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct LockEvent {
    pub txn_id: u64,
    pub resource: String,
    pub lock_type: LockType,
    pub wait_start: Instant,
    pub acquired: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LockType {
    Shared,
    Exclusive,
}

pub struct LockMonitor {
    events: Arc<DashMap<u64, Vec<LockEvent>>>,
}

impl LockMonitor {
    pub fn new() -> Self {
        Self { events: Arc::new(DashMap::new()) }
    }

    pub fn record_wait(&self, txn_id: u64, resource: String, lock_type: LockType) {
        self.events.entry(txn_id).or_insert_with(Vec::new).push(LockEvent {
            txn_id,
            resource,
            lock_type,
            wait_start: Instant::now(),
            acquired: false,
        });
    }

    pub fn record_acquired(&self, txn_id: u64) {
        if let Some(mut events) = self.events.get_mut(&txn_id) {
            if let Some(last) = events.last_mut() {
                last.acquired = true;
            }
        }
    }

    pub fn get_wait_events(&self) -> Vec<(u64, Duration)> {
        let mut waits = Vec::new();
        for entry in self.events.iter() {
            for event in entry.value() {
                if !event.acquired {
                    waits.push((*entry.key(), event.wait_start.elapsed()));
                }
            }
        }
        waits
    }

    pub fn clear(&self, txn_id: u64) {
        self.events.remove(&txn_id);
    }
}

impl Default for LockMonitor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_wait() {
        let monitor = LockMonitor::new();
        monitor.record_wait(1, "table1".to_string(), LockType::Exclusive);
        let waits = monitor.get_wait_events();
        assert_eq!(waits.len(), 1);
    }

    #[test]
    fn test_record_acquired() {
        let monitor = LockMonitor::new();
        monitor.record_wait(1, "table1".to_string(), LockType::Shared);
        monitor.record_acquired(1);
        let waits = monitor.get_wait_events();
        assert_eq!(waits.len(), 0);
    }

    #[test]
    fn test_clear() {
        let monitor = LockMonitor::new();
        monitor.record_wait(1, "table1".to_string(), LockType::Exclusive);
        monitor.clear(1);
        let waits = monitor.get_wait_events();
        assert_eq!(waits.len(), 0);
    }
}
