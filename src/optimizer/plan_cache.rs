use crate::optimizer::plan::LogicalPlan;
use dashmap::DashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct PlanCache {
    cache: Arc<DashMap<String, CachedPlan>>,
    max_size: usize,
}

#[derive(Clone)]
struct CachedPlan {
    plan: LogicalPlan,
    hit_count: usize,
}

impl PlanCache {
    pub fn new(max_size: usize) -> Self {
        Self { cache: Arc::new(DashMap::new()), max_size }
    }

    pub fn get(&self, sql: &str) -> Option<LogicalPlan> {
        self.cache.get_mut(sql).map(|mut entry| {
            entry.hit_count += 1;
            entry.plan.clone()
        })
    }

    pub fn insert(&self, sql: String, plan: LogicalPlan) {
        if self.max_size == 0 {
            return;
        }
        if self.cache.len() >= self.max_size {
            self.evict_lru();
        }
        self.cache.insert(sql, CachedPlan { plan, hit_count: 0 });
    }

    pub fn clear(&self) {
        self.cache.clear();
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    pub fn hit_count(&self, sql: &str) -> Option<usize> {
        self.cache.get(sql).map(|entry| entry.hit_count)
    }

    fn evict_lru(&self) {
        if let Some(lru_key) = self
            .cache
            .iter()
            .min_by_key(|entry| entry.value().hit_count)
            .map(|entry| entry.key().clone())
        {
            self.cache.remove(&lru_key);
        }
    }
}

impl Default for PlanCache {
    fn default() -> Self {
        Self::new(1000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::plan::LogicalPlan;

    fn create_test_plan(id: usize) -> LogicalPlan {
        LogicalPlan::Scan { table: format!("table{}", id), filter: None, columns: vec![] }
    }

    #[test]
    fn test_cache_insert_and_get() {
        let cache = PlanCache::new(10);
        let plan = create_test_plan(1);

        cache.insert("SELECT * FROM t1".to_string(), plan.clone());
        let retrieved = cache.get("SELECT * FROM t1");

        assert!(retrieved.is_some());
    }

    #[test]
    fn test_cache_miss() {
        let cache = PlanCache::new(10);
        assert!(cache.get("SELECT * FROM t1").is_none());
    }

    #[test]
    fn test_cache_hit_count() {
        let cache = PlanCache::new(10);
        let plan = create_test_plan(1);

        cache.insert("SELECT * FROM t1".to_string(), plan);
        assert_eq!(cache.hit_count("SELECT * FROM t1"), Some(0));

        cache.get("SELECT * FROM t1");
        assert_eq!(cache.hit_count("SELECT * FROM t1"), Some(1));

        cache.get("SELECT * FROM t1");
        assert_eq!(cache.hit_count("SELECT * FROM t1"), Some(2));
    }

    #[test]
    fn test_cache_clear() {
        let cache = PlanCache::new(10);
        cache.insert("SELECT * FROM t1".to_string(), create_test_plan(1));
        cache.insert("SELECT * FROM t2".to_string(), create_test_plan(2));

        assert_eq!(cache.len(), 2);
        cache.clear();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_cache_eviction() {
        let cache = PlanCache::new(3);

        cache.insert("q1".to_string(), create_test_plan(1));
        cache.insert("q2".to_string(), create_test_plan(2));
        cache.insert("q3".to_string(), create_test_plan(3));

        cache.get("q1");
        cache.get("q2");

        cache.insert("q4".to_string(), create_test_plan(4));

        assert_eq!(cache.len(), 3);
        assert!(cache.get("q3").is_none());
        assert!(cache.get("q1").is_some());
    }

    #[test]
    fn test_default_cache() {
        let cache = PlanCache::default();
        assert_eq!(cache.max_size, 1000);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_multiple_hits() {
        let cache = PlanCache::new(10);
        cache.insert("q1".to_string(), create_test_plan(1));

        for _ in 0..5 {
            cache.get("q1");
        }

        assert_eq!(cache.hit_count("q1"), Some(5));
    }

    #[test]
    fn test_cache_size_limit() {
        let cache = PlanCache::new(2);

        cache.insert("q1".to_string(), create_test_plan(1));
        cache.insert("q2".to_string(), create_test_plan(2));
        assert_eq!(cache.len(), 2);

        cache.insert("q3".to_string(), create_test_plan(3));
        assert_eq!(cache.len(), 2);
    }
}

#[cfg(test)]
mod edge_tests {
    use super::*;

    fn create_test_plan(id: usize) -> LogicalPlan {
        LogicalPlan::Scan { table: format!("table{}", id), filter: None, columns: vec![] }
    }

    #[test]
    fn test_empty_cache() {
        let cache = PlanCache::new(10);
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert!(cache.get("any").is_none());
    }

    #[test]
    fn test_zero_size_cache() {
        let cache = PlanCache::new(0);
        cache.insert("q1".to_string(), create_test_plan(1));
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_single_entry_cache() {
        let cache = PlanCache::new(1);
        cache.insert("q1".to_string(), create_test_plan(1));
        assert_eq!(cache.len(), 1);

        cache.insert("q2".to_string(), create_test_plan(2));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_duplicate_insert() {
        let cache = PlanCache::new(10);
        cache.insert("q1".to_string(), create_test_plan(1));
        cache.get("q1");

        cache.insert("q1".to_string(), create_test_plan(2));
        assert_eq!(cache.hit_count("q1"), Some(0));
    }

    #[test]
    fn test_empty_sql_string() {
        let cache = PlanCache::new(10);
        cache.insert("".to_string(), create_test_plan(1));
        assert!(cache.get("").is_some());
    }

    #[test]
    fn test_very_long_sql() {
        let cache = PlanCache::new(10);
        let long_sql =
            "SELECT * FROM table WHERE ".to_string() + &"x = 1 AND ".repeat(1000) + "y = 2";
        cache.insert(long_sql.clone(), create_test_plan(1));
        assert!(cache.get(&long_sql).is_some());
    }

    #[test]
    fn test_special_characters_in_sql() {
        let cache = PlanCache::new(10);
        let sql = "SELECT * FROM t WHERE x = 'a''b' AND y = \"c\"";
        cache.insert(sql.to_string(), create_test_plan(1));
        assert!(cache.get(sql).is_some());
    }

    #[test]
    fn test_unicode_in_sql() {
        let cache = PlanCache::new(10);
        let sql = "SELECT * FROM 用户 WHERE 名字 = '张三'";
        cache.insert(sql.to_string(), create_test_plan(1));
        assert!(cache.get(sql).is_some());
    }

    #[test]
    fn test_eviction_with_equal_hits() {
        let cache = PlanCache::new(2);
        cache.insert("q1".to_string(), create_test_plan(1));
        cache.insert("q2".to_string(), create_test_plan(2));

        cache.insert("q3".to_string(), create_test_plan(3));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_large_cache() {
        let cache = PlanCache::new(10000);
        for i in 0..5000 {
            cache.insert(format!("q{}", i), create_test_plan(i));
        }
        assert_eq!(cache.len(), 5000);
    }

    #[test]
    fn test_concurrent_access() {
        use std::thread;

        let cache = PlanCache::new(100);
        let cache_clone = cache.clone();

        let handle = thread::spawn(move || {
            for i in 0..50 {
                cache_clone.insert(format!("q{}", i), create_test_plan(i));
            }
        });

        for i in 50..100 {
            cache.insert(format!("q{}", i), create_test_plan(i));
        }

        handle.join().unwrap();
        assert_eq!(cache.len(), 100);
    }

    #[test]
    fn test_hit_count_after_eviction() {
        let cache = PlanCache::new(2);
        cache.insert("q1".to_string(), create_test_plan(1));
        cache.insert("q2".to_string(), create_test_plan(2));

        cache.get("q2");
        cache.insert("q3".to_string(), create_test_plan(3));

        assert!(cache.get("q1").is_none());
    }
}
