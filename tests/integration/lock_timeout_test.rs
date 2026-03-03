#[cfg(test)]
mod tests {
    use vaultgres::transaction::{LockKey, LockManager, LockMode};
    use std::time::Duration;

    #[test]
    fn test_lock_timeout_default() {
        let mgr = LockManager::new();
        let key = LockKey::table(1);

        mgr.acquire(1, key, LockMode::Exclusive).unwrap();

        // Second transaction should fail immediately (deadlock detection)
        let result = mgr.acquire(2, key, LockMode::Exclusive);
        assert!(result.is_err());
    }

    #[test]
    fn test_lock_timeout_custom() {
        let mgr = LockManager::with_timeout(Duration::from_millis(100));
        let key = LockKey::table(1);

        mgr.acquire(1, key, LockMode::Exclusive).unwrap();

        let result = mgr.acquire(2, key, LockMode::Exclusive);
        assert!(result.is_err());
    }

    #[test]
    fn test_lock_timeout_zero() {
        let mgr = LockManager::with_timeout(Duration::from_secs(0));
        let key = LockKey::table(1);

        mgr.acquire(1, key, LockMode::Exclusive).unwrap();

        let result = mgr.acquire(2, key, LockMode::Exclusive);
        assert!(result.is_err());
    }

    #[test]
    fn test_lock_release_before_timeout() {
        let mgr = LockManager::with_timeout(Duration::from_secs(10));
        let key = LockKey::table(1);

        mgr.acquire(1, key, LockMode::Exclusive).unwrap();
        mgr.release(1, key).unwrap();

        // Should succeed after release
        mgr.acquire(2, key, LockMode::Exclusive).unwrap();
    }

    #[test]
    fn test_multiple_waiters_timeout() {
        let mgr = LockManager::with_timeout(Duration::from_millis(50));
        let key = LockKey::table(1);

        mgr.acquire(1, key, LockMode::Exclusive).unwrap();

        let _ = mgr.acquire(2, key, LockMode::Exclusive);
        let _ = mgr.acquire(3, key, LockMode::Exclusive);

        // All should fail due to conflict
        assert!(mgr.acquire(2, key, LockMode::Exclusive).is_err());
        assert!(mgr.acquire(3, key, LockMode::Exclusive).is_err());
    }

    #[test]
    fn test_shared_locks_no_timeout() {
        let mgr = LockManager::with_timeout(Duration::from_millis(10));
        let key = LockKey::table(1);

        mgr.acquire(1, key, LockMode::Shared).unwrap();
        mgr.acquire(2, key, LockMode::Shared).unwrap();
        mgr.acquire(3, key, LockMode::Shared).unwrap();
    }

    #[test]
    fn test_timeout_with_different_keys() {
        let mgr = LockManager::with_timeout(Duration::from_millis(100));
        let key1 = LockKey::table(1);
        let key2 = LockKey::table(2);

        mgr.acquire(1, key1, LockMode::Exclusive).unwrap();
        mgr.acquire(2, key2, LockMode::Exclusive).unwrap();

        // Different keys, no conflict
        assert!(mgr.acquire(1, key2, LockMode::Exclusive).is_err());
        assert!(mgr.acquire(2, key1, LockMode::Exclusive).is_err());
    }
}
