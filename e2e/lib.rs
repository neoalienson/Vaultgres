use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

pub struct TestEnv {
    vaultgres_enabled: bool,
    postgres_enabled: bool,
    monitoring_enabled: bool,
    persistence_enabled: bool,
    compose_project: String,
}

impl TestEnv {
    pub fn new() -> Self {
        Self {
            vaultgres_enabled: false,
            postgres_enabled: false,
            monitoring_enabled: false,
            persistence_enabled: false,
            compose_project: format!("e2e-{}", std::process::id()),
        }
    }

    pub fn with_vaultgres(mut self) -> Self {
        self.vaultgres_enabled = true;
        self
    }

    pub fn with_postgres(mut self) -> Self {
        self.postgres_enabled = true;
        self
    }

    pub fn with_monitoring(mut self) -> Self {
        self.monitoring_enabled = true;
        self
    }

    pub fn with_persistence(mut self) -> Self {
        self.persistence_enabled = true;
        self
    }

    pub fn start(self) -> RunningEnv {
        eprintln!("[TestEnv] Starting containers...");
        let mut services = vec![];
        if self.vaultgres_enabled { services.push("vaultgres"); }
        if self.postgres_enabled { services.push("postgres"); }
        if self.monitoring_enabled {
            services.extend(&["prometheus", "cadvisor", "grafana"]);
        }

        eprintln!("[TestEnv] Services: {:?}", services);
        Command::new("docker")
            .args(&["compose", "-p", &self.compose_project, "up", "-d"])
            .args(&services)
            .stdout(Stdio::null())
            .status()
            .expect("Failed to start containers");

        eprintln!("[TestEnv] Waiting 10s for containers to be ready...");
        thread::sleep(Duration::from_secs(10));
        eprintln!("[TestEnv] Ready!");

        RunningEnv {
            compose_project: self.compose_project,
            vaultgres_port: if self.vaultgres_enabled { Some(5432) } else { None },
            postgres_port: if self.postgres_enabled { Some(5433) } else { None },
            persistence_enabled: self.persistence_enabled,
        }
    }
}

pub struct RunningEnv {
    compose_project: String,
    vaultgres_port: Option<u16>,
    postgres_port: Option<u16>,
    persistence_enabled: bool,
}

impl RunningEnv {
    pub fn vaultgres(&self) -> DbConnection {
        DbConnection::new("localhost", self.vaultgres_port.expect("VaultGres not enabled"))
    }

    pub fn postgres(&self) -> DbConnection {
        DbConnection::new("localhost", self.postgres_port.expect("Postgres not enabled"))
    }

    pub fn kill_container(&self) {
        eprintln!("[TestEnv] Killing container...");
        // Find vaultgres container by name pattern
        let output = Command::new("docker")
            .args(&["ps", "--filter", "name=vaultgres", "--format", "{{.Names}}"])
            .output()
            .expect("Failed to list containers");
        
        let container_name = String::from_utf8_lossy(&output.stdout)
            .lines()
            .next()
            .unwrap_or("vaultgres-test")
            .to_string();

        Command::new("docker")
            .args(&["kill", &container_name])
            .output()
            .expect("Failed to kill container");
        eprintln!("[TestEnv] Container killed");
    }

    pub fn restart(&self) {
        eprintln!("[TestEnv] Restarting container...");
        Command::new("docker")
            .args(&["compose", "-p", &self.compose_project, "restart", "vaultgres"])
            .output()
            .expect("Failed to restart");
        eprintln!("[TestEnv] Waiting 3s for restart...");
        thread::sleep(Duration::from_secs(3));
        eprintln!("[TestEnv] Restarted!");
    }

    pub fn start_monitor(&self) -> MetricsMonitor {
        MetricsMonitor::new(&self.compose_project)
    }
}

impl Drop for RunningEnv {
    fn drop(&mut self) {
        if !self.persistence_enabled {
            let _ = Command::new("docker")
                .args(&["compose", "-p", &self.compose_project, "down", "-v"])
                .output();
        } else {
            let _ = Command::new("docker")
                .args(&["compose", "-p", &self.compose_project, "stop"])
                .output();
        }
    }
}

pub struct DbConnection {
    host: String,
    port: u16,
}

impl DbConnection {
    fn new(host: &str, port: u16) -> Self {
        Self { host: host.to_string(), port }
    }

    pub fn connect(host: &str, port: u16) -> Self {
        Self::new(host, port)
    }

    pub fn execute(&self, sql: &str) -> Result<String, String> {
        eprintln!("[DB] Executing: {}", sql);
        let output = Command::new("psql")
            .args(&[
                "-h", &self.host,
                "-p", &self.port.to_string(),
                "-U", "postgres",
                "-d", "postgres",
                "-c", sql,
            ])
            .output()
            .map_err(|e| format!("psql failed: {}", e))?;

        if output.status.success() {
            let result = String::from_utf8_lossy(&output.stdout).to_string();
            eprintln!("[DB] Success (output length: {} bytes)", result.len());
            if sql.to_uppercase().starts_with("SELECT") && result.len() > 100 {
                eprintln!("[DB] Output preview: {}...", result.chars().take(200).collect::<String>());
            }
            Ok(result)
        } else {
            let err = String::from_utf8_lossy(&output.stderr).to_string();
            eprintln!("[DB] Error: {}", err);
            Err(err)
        }
    }

    pub fn query_scalar<T: std::str::FromStr>(&self, sql: &str) -> T {
        let result = self.execute(sql).expect("Query failed");
        result.lines()
            .nth(2)
            .and_then(|line| line.trim().parse().ok())
            .expect("Failed to parse scalar")
    }

    pub fn time_query(&self, sql: &str) -> Duration {
        let start = Instant::now();
        self.execute(sql).expect("Query failed");
        start.elapsed()
    }
}

pub struct MetricsMonitor {
    start_metrics: ContainerMetrics,
}

impl MetricsMonitor {
    fn new(_compose_project: &str) -> Self {
        let start_metrics = Self::collect_metrics();
        Self { start_metrics }
    }

    fn collect_metrics() -> ContainerMetrics {
        // Try to find vaultgres container by name pattern
        let output = Command::new("docker")
            .args(&["ps", "--filter", "name=vaultgres", "--format", "{{.Names}}"])
            .output()
            .expect("Failed to list containers");
        
        let container_name = String::from_utf8_lossy(&output.stdout)
            .lines()
            .next()
            .unwrap_or("vaultgres-test")
            .to_string();

        let output = Command::new("docker")
            .args(&["stats", "--no-stream", "--format", "{{.MemUsage}}\t{{.CPUPerc}}", &container_name])
            .output()
            .expect("Failed to collect metrics");

        let stats = String::from_utf8_lossy(&output.stdout);
        let parts: Vec<&str> = stats.split_whitespace().collect();

        ContainerMetrics {
            memory_mb: Self::parse_memory(parts.get(0).unwrap_or(&"0MiB")),
            cpu_percent: Self::parse_cpu(parts.get(2).unwrap_or(&"0%")),
        }
    }

    fn parse_memory(s: &str) -> f64 {
        s.trim_end_matches("MiB").parse().unwrap_or(0.0)
    }

    fn parse_cpu(s: &str) -> f64 {
        s.trim_end_matches('%').parse().unwrap_or(0.0)
    }

    pub fn stop(self) -> MonitoringResult {
        let end_metrics = Self::collect_metrics();
        MonitoringResult {
            memory_growth_mb: end_metrics.memory_mb - self.start_metrics.memory_mb,
            avg_cpu_percent: (self.start_metrics.cpu_percent + end_metrics.cpu_percent) / 2.0,
        }
    }
}

#[derive(Debug)]
struct ContainerMetrics {
    memory_mb: f64,
    cpu_percent: f64,
}

#[derive(Debug)]
pub struct MonitoringResult {
    pub memory_growth_mb: f64,
    pub avg_cpu_percent: f64,
}

pub struct WorkloadResult {
    pub duration: Duration,
    pub queries_executed: u64,
    pub errors: u64,
    pub avg_latency_ms: f64,
    pub p99_latency_ms: f64,
}

impl WorkloadResult {
    pub fn qps(&self) -> f64 {
        self.queries_executed as f64 / self.duration.as_secs_f64()
    }

    pub fn error_rate(&self) -> f64 {
        self.errors as f64 / self.queries_executed as f64
    }
}
