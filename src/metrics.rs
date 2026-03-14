use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Metrics {
    queries_total: AtomicU64,
    queries_slow: AtomicU64,
    connections_active: AtomicU64,
    buffer_hits: AtomicU64,
    buffer_misses: AtomicU64,
    disk_reads: AtomicU64,
    disk_writes: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            queries_total: AtomicU64::new(0),
            queries_slow: AtomicU64::new(0),
            connections_active: AtomicU64::new(0),
            buffer_hits: AtomicU64::new(0),
            buffer_misses: AtomicU64::new(0),
            disk_reads: AtomicU64::new(0),
            disk_writes: AtomicU64::new(0),
        })
    }

    pub fn inc_queries(&self) {
        self.queries_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_slow_queries(&self) {
        self.queries_slow.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_connections(&self) {
        self.connections_active.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_connections(&self) {
        self.connections_active.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn inc_buffer_hits(&self) {
        self.buffer_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_buffer_misses(&self) {
        self.buffer_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_disk_reads(&self) {
        self.disk_reads.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_disk_writes(&self) {
        self.disk_writes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn export_prometheus(&self) -> String {
        format!(
            "# HELP vaultgres_queries_total Total queries executed\n\
             # TYPE vaultgres_queries_total counter\n\
             vaultgres_queries_total {}\n\
             # HELP vaultgres_queries_slow Slow queries executed\n\
             # TYPE vaultgres_queries_slow counter\n\
             vaultgres_queries_slow {}\n\
             # HELP vaultgres_connections_active Active connections\n\
             # TYPE vaultgres_connections_active gauge\n\
             vaultgres_connections_active {}\n\
             # HELP vaultgres_buffer_hits Buffer pool hits\n\
             # TYPE vaultgres_buffer_hits counter\n\
             vaultgres_buffer_hits {}\n\
             # HELP vaultgres_buffer_misses Buffer pool misses\n\
             # TYPE vaultgres_buffer_misses counter\n\
             vaultgres_buffer_misses {}\n\
             # HELP vaultgres_disk_reads Disk read operations\n\
             # TYPE vaultgres_disk_reads counter\n\
             vaultgres_disk_reads {}\n\
             # HELP vaultgres_disk_writes Disk write operations\n\
             # TYPE vaultgres_disk_writes counter\n\
             vaultgres_disk_writes {}\n",
            self.queries_total.load(Ordering::Relaxed),
            self.queries_slow.load(Ordering::Relaxed),
            self.connections_active.load(Ordering::Relaxed),
            self.buffer_hits.load(Ordering::Relaxed),
            self.buffer_misses.load(Ordering::Relaxed),
            self.disk_reads.load(Ordering::Relaxed),
            self.disk_writes.load(Ordering::Relaxed),
        )
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            queries_total: AtomicU64::new(0),
            queries_slow: AtomicU64::new(0),
            connections_active: AtomicU64::new(0),
            buffer_hits: AtomicU64::new(0),
            buffer_misses: AtomicU64::new(0),
            disk_reads: AtomicU64::new(0),
            disk_writes: AtomicU64::new(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_increment() {
        let m = Metrics::new();
        m.inc_queries();
        m.inc_queries();
        assert_eq!(m.queries_total.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_connections() {
        let m = Metrics::new();
        m.inc_connections();
        m.inc_connections();
        assert_eq!(m.connections_active.load(Ordering::Relaxed), 2);
        m.dec_connections();
        assert_eq!(m.connections_active.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_prometheus_export() {
        let m = Metrics::new();
        m.inc_queries();
        let output = m.export_prometheus();
        assert!(output.contains("vaultgres_queries_total 1"));
    }
}
