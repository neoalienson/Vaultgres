use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

pub struct TestEnv {
    vaultgres_enabled: bool,
    postgres_enabled: bool,
    monitoring_enabled: bool,
    compose_project: String,
}

impl TestEnv {
    pub fn new() -> Self {
        Self {
            vaultgres_enabled: false,
            postgres_enabled: false,
            monitoring_enabled: false,
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

    pub fn start(self) -> RunningEnv {
        // Always clean up any containers from this project first
        eprintln!(
            "[TestEnv] Cleaning up any existing containers for project '{}'...",
            self.compose_project
        );

        // First, stop and remove any containers from this specific project
        let _ = Command::new("docker")
            .args(&[
                "compose",
                "-p",
                &self.compose_project,
                "down",
                "-v",
                "--remove-orphans",
                "-t",
                "5",
            ])
            .status();

        // Also remove any vaultgres containers (from crashed tests or previous runs)
        eprintln!("[TestEnv] Removing any orphan vaultgres containers...");
        let output = Command::new("docker")
            .args(&["ps", "-a", "-q", "--filter", "name=vaultgres"])
            .output()
            .expect("Failed to check containers");

        let output_str = String::from_utf8_lossy(&output.stdout);
        let container_ids: Vec<&str> = output_str.lines().filter(|s| !s.is_empty()).collect();

        if !container_ids.is_empty() {
            eprintln!(
                "[TestEnv] Found {} orphan vaultgres container(s), removing...",
                container_ids.len()
            );
            for id in &container_ids {
                let _ = Command::new("docker").args(&["rm", "-f", id]).status();
            }
        }

        // Remove any networks from this project
        eprintln!("[TestEnv] Removing project networks...");
        let _ = Command::new("docker")
            .args(&["network", "rm", &format!("{}_test-network", self.compose_project)])
            .status();

        // Poll until no containers exist for this project
        eprintln!("[TestEnv] Waiting for cleanup to complete...");
        for i in 0..60 {
            let output = Command::new("docker")
                .args(&["ps", "-a", "-q", "--filter", &format!("name={}", self.compose_project)])
                .output()
                .expect("Failed to check containers");

            if output.stdout.is_empty() {
                eprintln!("[TestEnv] Cleanup completed after {} iterations", i + 1);
                break;
            }

            if i % 10 == 0 {
                eprintln!("[TestEnv] Still waiting for containers to be removed... ({}s)", i);
            }
            thread::sleep(Duration::from_millis(500));
        }

        // Also check if port 5432 is free
        for i in 0..30 {
            let output = Command::new("docker")
                .args(&["ps", "-q", "--filter", "publish=5432"])
                .output()
                .expect("Failed to check port");

            if output.stdout.is_empty() {
                break;
            }

            if i % 10 == 0 {
                eprintln!("[TestEnv] Waiting for port 5432 to be free... ({}s)", i);
            }
            thread::sleep(Duration::from_millis(500));
        }

        eprintln!("[TestEnv] Starting containers...");
        let mut services = vec![];
        if self.vaultgres_enabled {
            services.push("vaultgres");
        }
        if self.postgres_enabled {
            services.push("postgres");
        }
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
            postgres_port: if self.postgres_enabled { Some(5432) } else { None },
        }
    }
}

pub struct RunningEnv {
    compose_project: String,
    vaultgres_port: Option<u16>,
    postgres_port: Option<u16>,
}

impl RunningEnv {
    pub fn vaultgres(&self) -> DbConnection {
        DbConnection::new("localhost", self.vaultgres_port.expect("VaultGres not enabled"))
    }

    pub fn postgres(&self) -> DbConnection {
        DbConnection::new("localhost", self.postgres_port.expect("Postgres not enabled"))
    }

    /// Fetch and display server logs (useful for debugging crashes)
    pub fn fetch_server_logs(&self) {
        eprintln!("[TestEnv] Fetching server logs...");
        let port = self.vaultgres_port.unwrap_or(5432);
        DbConnection::fetch_server_logs(port);
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
        self.restart_graceful(5);
    }

    pub fn restart_graceful(&self, wait_secs: u64) {
        eprintln!("[TestEnv] Restarting container gracefully (SIGTERM via docker kill)...");

        // Check volume before restart using docker inspect
        eprintln!("[TestEnv] Checking volume mount before restart...");
        let container_name = format!("{}-vaultgres-1", self.compose_project);
        let volume_info = Command::new("docker")
            .args(&[
                "inspect",
                "-f",
                "{{range .Mounts}}{{.Destination}} => {{.Name}}{{end}}",
                &container_name,
            ])
            .output();
        if let Ok(output) = volume_info {
            eprintln!("[TestEnv] Volume mounts: {}", String::from_utf8_lossy(&output.stdout));
        }

        // Send SIGTERM directly to the container using docker kill
        // This ensures the signal reaches PID 1 (vaultgres)
        eprintln!("[TestEnv] Sending SIGTERM to container {}...", container_name);
        let kill_result =
            Command::new("docker").args(&["kill", "-s", "SIGTERM", &container_name]).output();

        match kill_result {
            Ok(output) => {
                if output.status.success() {
                    eprintln!("[TestEnv] SIGTERM sent successfully");
                } else {
                    eprintln!("[TestEnv] Kill output: {}", String::from_utf8_lossy(&output.stderr));
                }
            }
            Err(e) => eprintln!("[TestEnv] Failed to send SIGTERM: {}", e),
        }

        // Wait for container to stop (with timeout)
        eprintln!("[TestEnv] Waiting up to 30s for container to stop...");
        for i in 0..30 {
            let check = Command::new("docker")
                .args(&["inspect", "-f", "{{.State.Running}}", &container_name])
                .output();

            if let Ok(output) = check {
                let running = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if running == "false" {
                    eprintln!("[TestEnv] Container stopped after {}s", i);
                    break;
                }
            }
            thread::sleep(Duration::from_secs(1));
        }

        eprintln!("[TestEnv] Waiting 2s after stop...");
        thread::sleep(Duration::from_secs(2));

        eprintln!("[TestEnv] Starting container...");
        Command::new("docker")
            .args(&["compose", "-p", &self.compose_project, "start", "vaultgres"])
            .output()
            .expect("Failed to start");
        eprintln!("[TestEnv] Waiting {}s for container to be ready...", wait_secs);
        thread::sleep(Duration::from_secs(wait_secs));

        // Check volume after restart
        eprintln!("[TestEnv] Checking volume mount after restart...");
        let volume_info = Command::new("docker")
            .args(&[
                "inspect",
                "-f",
                "{{range .Mounts}}{{.Destination}} => {{.Name}}{{end}}",
                &container_name,
            ])
            .output();
        if let Ok(output) = volume_info {
            eprintln!("[TestEnv] Volume mounts: {}", String::from_utf8_lossy(&output.stdout));
        }

        eprintln!("[TestEnv] Restarted!");
    }

    pub fn restart_with_kill(&self, wait_secs: u64) {
        eprintln!("[TestEnv] Killing container (SIGKILL - simulating crash)...");
        // Kill sends SIGKILL - no graceful shutdown, simulates crash
        Command::new("docker")
            .args(&["compose", "-p", &self.compose_project, "kill", "vaultgres"])
            .output()
            .expect("Failed to kill");
        eprintln!("[TestEnv] Waiting 1s...");
        thread::sleep(Duration::from_secs(1));

        eprintln!("[TestEnv] Starting container...");
        Command::new("docker")
            .args(&["compose", "-p", &self.compose_project, "start", "vaultgres"])
            .output()
            .expect("Failed to start");
        eprintln!("[TestEnv] Waiting {}s for restart...", wait_secs);
        thread::sleep(Duration::from_secs(wait_secs));
        eprintln!("[TestEnv] Restarted after kill!");
    }

    pub fn restart_with_stop_start(&self, wait_secs: u64) {
        eprintln!("[TestEnv] Stopping container (SIGSTOP)...");
        // Stop sends SIGSTOP - may or may not allow graceful shutdown depending on docker config
        Command::new("docker")
            .args(&["compose", "-p", &self.compose_project, "stop", "vaultgres", "-t", "5"])
            .output()
            .expect("Failed to stop");
        eprintln!("[TestEnv] Waiting 1s...");
        thread::sleep(Duration::from_secs(1));

        eprintln!("[TestEnv] Starting container...");
        Command::new("docker")
            .args(&["compose", "-p", &self.compose_project, "start", "vaultgres"])
            .output()
            .expect("Failed to start");
        eprintln!("[TestEnv] Waiting {}s for restart...", wait_secs);
        thread::sleep(Duration::from_secs(wait_secs));
        eprintln!("[TestEnv] Restarted after stop/start!");
    }

    pub fn start_monitor(&self) -> MetricsMonitor {
        MetricsMonitor::new(&self.compose_project)
    }
}

impl Drop for RunningEnv {
    fn drop(&mut self) {
        eprintln!("[TestEnv] Cleaning up test environment (containers and volumes)...");
        let _ = Command::new("docker")
            .args(&["compose", "-p", &self.compose_project, "down", "-v", "--remove-orphans"])
            .output();
        eprintln!("[TestEnv] Cleanup complete");
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
                "-h",
                &self.host,
                "-p",
                &self.port.to_string(),
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-c",
                sql,
            ])
            .output()
            .map_err(|e| format!("psql failed: {}", e))?;

        if output.status.success() {
            let result = String::from_utf8_lossy(&output.stdout).to_string();
            eprintln!("[DB] Success (output length: {} bytes)", result.len());
            if sql.to_uppercase().starts_with("SELECT") && result.len() > 100 {
                eprintln!(
                    "[DB] Output preview: {}...",
                    result.chars().take(200).collect::<String>()
                );
            }
            Ok(result)
        } else {
            let err = String::from_utf8_lossy(&output.stderr).to_string();
            eprintln!("[DB] Error: {}", err);

            // Check if error indicates server crash/connection loss
            if err.contains("server closed the connection unexpectedly")
                || err.contains("connection to server was lost")
                || err.contains("terminating connection due to")
            {
                eprintln!("\n[!!!] SERVER CRASH DETECTED! Fetching server logs...");
                Self::fetch_server_logs(self.port);
            }

            Err(err)
        }
    }

    /// Fetch and display recent server logs from the container
    fn fetch_server_logs(port: u16) {
        eprintln!("[Logs] Fetching logs for container on port {}...", port);

        // Find container by port
        let container_output = Command::new("docker")
            .args(&["ps", "-q", "--filter", &format!("publish={}", port)])
            .output();

        if let Ok(output) = container_output {
            let container_id_raw = String::from_utf8_lossy(&output.stdout);
            let container_id = container_id_raw.trim();

            if !container_id.is_empty() {
                // Get last 50 lines of logs
                let logs_output =
                    Command::new("docker").args(&["logs", "--tail", "50", container_id]).output();

                if let Ok(logs) = logs_output {
                    let stdout = String::from_utf8_lossy(&logs.stdout);
                    let stderr = String::from_utf8_lossy(&logs.stderr);

                    eprintln!("\n{}", "=".repeat(80));
                    eprintln!("[Logs] === VAULTGRES SERVER LOGS (Last 50 lines) ===");
                    eprintln!("{}", "=".repeat(80));
                    if !stdout.is_empty() {
                        eprintln!("STDOUT:\n{}", stdout);
                    }
                    if !stderr.is_empty() {
                        eprintln!("STDERR:\n{}", stderr);
                    }
                    eprintln!("{}", "=".repeat(80));
                    eprintln!("[Logs] === END OF LOGS ===\n");
                } else {
                    eprintln!("[Logs] Failed to fetch logs: {}", logs_output.unwrap_err());
                }
            } else {
                eprintln!("[Logs] No container found on port {}", port);
            }
        } else {
            eprintln!("[Logs] Failed to find container: {}", container_output.unwrap_err());
        }
    }

    pub fn query_scalar<T: std::str::FromStr>(&self, sql: &str) -> T {
        let result = self.execute(sql).expect("Query failed");
        result
            .lines()
            .nth(2)
            .and_then(|line| line.trim().parse().ok())
            .expect("Failed to parse scalar")
    }

    pub fn time_query(&self, sql: &str) -> Duration {
        let start = Instant::now();
        self.execute(sql).expect("Query failed");
        start.elapsed()
    }

    /// Verify database connection is available, panics if not reachable
    pub fn verify_connection(&self) {
        eprintln!("[DB] Verifying connection to {}:{}...", self.host, self.port);
        let max_retries = 30;
        let retry_delay = Duration::from_secs(1);

        for attempt in 1..=max_retries {
            match self.execute("SELECT 1") {
                Ok(_) => {
                    eprintln!("[DB] Connection verified successfully!");
                    return;
                }
                Err(e) => {
                    if attempt < max_retries {
                        eprintln!(
                            "[DB] Connection attempt {} failed: {}. Retrying in {:?}...",
                            attempt, e, retry_delay
                        );
                        thread::sleep(retry_delay);
                    } else {
                        panic!("[DB] Failed to connect to database at {}:{} after {} attempts. Ensure the server is running.", self.host, self.port, max_retries);
                    }
                }
            }
        }
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
            .args(&[
                "stats",
                "--no-stream",
                "--format",
                "{{.MemUsage}}\t{{.CPUPerc}}",
                &container_name,
            ])
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
