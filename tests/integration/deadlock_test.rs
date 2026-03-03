#[cfg(test)]
mod tests {
    use rustgres::transaction::{LockKey, LockManager, LockMode};

    #[test]
    fn test_deadlock_detection_simple() {
        let mgr = LockManager::new();
        let key1 = LockKey::table(1);
        let key2 = LockKey::table(2);

        // T1 acquires lock on key1
        mgr.acquire(1, key1, LockMode::Exclusive).unwrap();

        // T2 acquires lock on key2
        mgr.acquire(2, key2, LockMode::Exclusive).unwrap();

        // T1 tries to acquire lock on key2 (blocked by T2)
        let result1 = mgr.acquire(1, key2, LockMode::Exclusive);
        assert!(result1.is_err());

        // T2 tries to acquire lock on key1 (would create deadlock)
        let result2 = mgr.acquire(2, key1, LockMode::Exclusive);
        assert!(result2.is_err());
    }

    #[test]
    fn test_no_deadlock_with_release() {
        let mgr = LockManager::new();
        let key1 = LockKey::table(1);
        let key2 = LockKey::table(2);

        mgr.acquire(1, key1, LockMode::Exclusive).unwrap();
        mgr.acquire(2, key2, LockMode::Exclusive).unwrap();

        // Release T1's lock
        mgr.release(1, key1).unwrap();

        // Now T2 can acquire key1
        mgr.acquire(2, key1, LockMode::Exclusive).unwrap();
    }

    #[test]
    fn test_shared_locks_no_deadlock() {
        let mgr = LockManager::new();
        let key = LockKey::table(1);

        mgr.acquire(1, key, LockMode::Shared).unwrap();
        mgr.acquire(2, key, LockMode::Shared).unwrap();
        mgr.acquire(3, key, LockMode::Shared).unwrap();
    }

    #[test]
    fn test_release_all_clears_wait_for() {
        let mgr = LockManager::new();
        let key1 = LockKey::table(1);
        let key2 = LockKey::table(2);

        mgr.acquire(1, key1, LockMode::Exclusive).unwrap();
        mgr.acquire(1, key2, LockMode::Exclusive).unwrap();

        mgr.release_all(1);

        mgr.acquire(2, key1, LockMode::Exclusive).unwrap();
        mgr.acquire(2, key2, LockMode::Exclusive).unwrap();
    }

    #[test]
    fn test_three_way_deadlock() {
        let mgr = LockManager::new();
        let key1 = LockKey::table(1);
        let key2 = LockKey::table(2);
        let key3 = LockKey::table(3);

        // T1 -> key1, T2 -> key2, T3 -> key3
        mgr.acquire(1, key1, LockMode::Exclusive).unwrap();
        mgr.acquire(2, key2, LockMode::Exclusive).unwrap();
        mgr.acquire(3, key3, LockMode::Exclusive).unwrap();

        // T1 waits for key2 (held by T2)
        let _ = mgr.acquire(1, key2, LockMode::Exclusive);

        // T2 waits for key3 (held by T3)
        let _ = mgr.acquire(2, key3, LockMode::Exclusive);

        // T3 tries to acquire key1 (would create cycle: T3->T1->T2->T3)
        let result = mgr.acquire(3, key1, LockMode::Exclusive);
        assert!(result.is_err());
    }

    #[test]
    fn test_tuple_level_locks() {
        let mgr = LockManager::new();
        let tuple1 = LockKey::tuple(1, 100);
        let tuple2 = LockKey::tuple(1, 200);

        mgr.acquire(1, tuple1, LockMode::Exclusive).unwrap();
        mgr.acquire(2, tuple2, LockMode::Exclusive).unwrap();

        // Different tuples, no conflict
        assert!(mgr.acquire(1, tuple2, LockMode::Exclusive).is_err());
    }

    #[test]
    fn test_same_transaction_reacquire() {
        let mgr = LockManager::new();
        let key = LockKey::table(1);

        mgr.acquire(1, key, LockMode::Exclusive).unwrap();
        mgr.acquire(1, key, LockMode::Exclusive).unwrap(); // Should succeed
    }
}
