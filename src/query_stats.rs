use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct QueryStats {
    pub query: String,
    pub calls: u64,
    pub total_time: Duration,
    pub min_time: Duration,
    pub max_time: Duration,
    pub mean_time: Duration,
}

pub struct QueryStatsCollector {
    stats: Arc<DashMap<String, QueryStats>>,
}

impl QueryStatsCollector {
    pub fn new() -> Self {
        Self { stats: Arc::new(DashMap::new()) }
    }

    pub fn record(&self, query: &str, duration: Duration) {
        let normalized = Self::normalize_query(query);
        self.stats
            .entry(normalized.clone())
            .and_modify(|s| {
                s.calls += 1;
                s.total_time += duration;
                s.min_time = s.min_time.min(duration);
                s.max_time = s.max_time.max(duration);
                s.mean_time = s.total_time / s.calls as u32;
            })
            .or_insert(QueryStats {
                query: normalized,
                calls: 1,
                total_time: duration,
                min_time: duration,
                max_time: duration,
                mean_time: duration,
            });
    }

    pub fn get_stats(&self) -> Vec<QueryStats> {
        self.stats.iter().map(|e| e.value().clone()).collect()
    }

    pub fn reset(&self) {
        self.stats.clear();
    }

    fn normalize_query(query: &str) -> String {
        query.split_whitespace().collect::<Vec<_>>().join(" ").to_uppercase()
    }
}

impl Default for QueryStatsCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_query() {
        let collector = QueryStatsCollector::new();
        collector.record("SELECT * FROM users", Duration::from_millis(10));
        collector.record("SELECT * FROM users", Duration::from_millis(20));
        let stats = collector.get_stats();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].calls, 2);
    }

    #[test]
    fn test_normalize_query() {
        let q1 = QueryStatsCollector::normalize_query("select  *  from users");
        let q2 = QueryStatsCollector::normalize_query("SELECT * FROM users");
        assert_eq!(q1, q2);
    }

    #[test]
    fn test_reset() {
        let collector = QueryStatsCollector::new();
        collector.record("SELECT 1", Duration::from_millis(5));
        assert_eq!(collector.get_stats().len(), 1);
        collector.reset();
        assert_eq!(collector.get_stats().len(), 0);
    }
}
