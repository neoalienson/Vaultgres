# E2E Testing Strategy

## Overview

Comprehensive end-to-end testing framework for VaultGres using Docker containers to simulate real-world scenarios, compare performance with PostgreSQL, and conduct load/soak tests.

## Architecture

```
e2e/
├── STRATEGY.md                 # This file
├── docker-compose.yml          # Multi-container orchestration
├── scenarios/                  # Test scenario definitions
│   ├── oltp_workload.rs       # OLTP transaction patterns
│   ├── analytics_workload.rs  # OLAP query patterns
│   ├── mixed_workload.rs      # Combined OLTP/OLAP
│   └── concurrent_users.rs    # Multi-client scenarios
├── load/                       # Load testing
│   ├── ramp_up.rs             # Gradual load increase
│   ├── spike.rs               # Sudden traffic spikes
│   └── sustained.rs           # Steady-state load
├── soak/                       # Soak testing
│   ├── memory_leak.rs         # Long-running memory checks
│   ├── connection_pool.rs     # Connection lifecycle
│   └── disk_growth.rs         # Storage growth monitoring
├── comparison/                 # VaultGres vs PostgreSQL
│   ├── benchmark.rs           # Performance comparison
│   └── compatibility.rs       # Feature parity checks
├── persistence/                # State persistence tests
│   ├── crash_recovery.rs      # Crash and recovery
│   └── data_integrity.rs      # Data consistency
├── lib.rs                      # Shared test infrastructure
└── Cargo.toml                  # Test dependencies
```

## Test Categories

### 1. Scenario Tests (Functional)
Real-world usage patterns with complex queries and transactions.

**Stateless**: Each test starts fresh container
**Stateful**: Tests require data persistence across restarts

### 2. Load Tests (Performance)
Measure throughput, latency, and resource usage under various loads.

**Metrics**: QPS, latency (p50/p95/p99), error rate

### 3. Soak Tests (Stability)
Long-running tests (hours/days) to detect memory leaks, resource exhaustion.

**Metrics**: Memory growth, CPU usage, disk I/O, connection count

### 4. Comparison Tests (Benchmarking)
Side-by-side comparison with PostgreSQL for performance and compatibility.

**Metrics**: Relative performance, feature coverage

## Test Infrastructure

### Docker Compose Setup
```yaml
services:
  vaultgres:
    image: vaultgres:latest
    ports: ["5432:5432"]
    volumes:
      - vaultgres-data:/var/lib/vaultgres/data
    environment:
      - RUST_LOG=info
    healthcheck:
      test: ["CMD", "pg_isready", "-h", "localhost"]
      interval: 5s
      timeout: 3s
      retries: 5
  
  postgres:
    image: postgres:16-alpine
    ports: ["5433:5432"]
    volumes:
      - postgres-data:/var/lib/postgresql/data
    environment:
      - POSTGRES_PASSWORD=postgres
    healthcheck:
      test: ["CMD", "pg_isready"]
      interval: 5s
      timeout: 3s
      retries: 5
  
  prometheus:
    image: prom/prometheus
    ports: ["9090:9090"]
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
  
  grafana:
    image: grafana/grafana
    ports: ["3000:3000"]
    volumes:
      - grafana-data:/var/lib/grafana
```

### Monitoring Stack
- **Prometheus**: Metrics collection (CPU, memory, disk, query stats)
- **Grafana**: Visualization dashboards
- **cAdvisor**: Container resource monitoring

## Test Patterns

### Pattern 1: Stateless Scenario Test
```rust
#[test]
fn test_oltp_workload() {
    let env = TestEnv::new()
        .with_vaultgres()
        .start();
    
    // Run workload
    let result = env.run_scenario("oltp_workload");
    
    // Assert metrics
    assert!(result.avg_latency_ms < 10.0);
    assert!(result.error_rate < 0.01);
}
```

### Pattern 2: Stateful Persistence Test
```rust
#[test]
fn test_crash_recovery() {
    let env = TestEnv::new()
        .with_vaultgres()
        .with_persistence()
        .start();
    
    // Insert data
    env.execute("INSERT INTO orders VALUES (1, 'item', 100)");
    
    // Simulate crash
    env.kill_container();
    
    // Restart and verify
    env.restart();
    let count = env.query_scalar("SELECT COUNT(*) FROM orders");
    assert_eq!(count, 1);
}
```

### Pattern 3: Comparison Test
```rust
#[test]
fn test_join_performance() {
    let env = TestEnv::new()
        .with_vaultgres()
        .with_postgres()
        .start();
    
    // Load same dataset
    env.load_dataset("tpch_1gb");
    
    // Run query on both
    let vaultgres_time = env.vaultgres().time_query(COMPLEX_JOIN);
    let postgres_time = env.postgres().time_query(COMPLEX_JOIN);
    
    // Compare
    let speedup = postgres_time / vaultgres_time;
    assert!(speedup > 0.8, "VaultGres should be competitive");
}
```

### Pattern 4: Soak Test
```rust
#[test]
#[ignore] // Run separately with --ignored
fn test_memory_leak_24h() {
    let env = TestEnv::new()
        .with_vaultgres()
        .with_monitoring()
        .start();
    
    let monitor = env.start_monitor();
    
    // Run for 24 hours
    for _ in 0..86400 {
        env.execute_random_query();
        thread::sleep(Duration::from_secs(1));
    }
    
    let metrics = monitor.stop();
    
    // Memory should be stable
    assert!(metrics.memory_growth_mb < 100.0);
}
```

## Workload Scenarios

### OLTP Workload
- **Pattern**: Short transactions, high concurrency
- **Operations**: INSERT, UPDATE, DELETE, simple SELECT
- **Metrics**: Transactions/sec, latency p99
- **Duration**: 5 minutes

### Analytics Workload
- **Pattern**: Complex queries, aggregations, joins
- **Operations**: Multi-table JOINs, GROUP BY, window functions
- **Metrics**: Query execution time, memory usage
- **Duration**: 10 minutes

### Mixed Workload
- **Pattern**: 70% OLTP, 30% analytics
- **Operations**: Concurrent short and long queries
- **Metrics**: Overall throughput, resource contention
- **Duration**: 15 minutes

### Concurrent Users
- **Pattern**: 100+ simultaneous connections
- **Operations**: Random mix of queries
- **Metrics**: Connection pool efficiency, deadlocks
- **Duration**: 10 minutes

## Load Test Profiles

### Ramp-Up Test
- Start: 10 users
- End: 1000 users
- Duration: 30 minutes
- Increment: +10 users/minute

### Spike Test
- Baseline: 50 users
- Spike: 500 users for 2 minutes
- Recovery: Back to 50 users
- Measure: Recovery time, error rate

### Sustained Load
- Users: 200 constant
- Duration: 2 hours
- Measure: Throughput stability, resource usage

## Soak Test Profiles

### Memory Leak Detection
- Duration: 24 hours
- Load: Constant 100 users
- Monitor: Memory RSS, heap size
- Alert: >10% growth/hour

### Connection Pool Stress
- Duration: 12 hours
- Pattern: Rapid connect/disconnect
- Monitor: Connection count, file descriptors
- Alert: Resource exhaustion

### Disk Growth Monitoring
- Duration: 48 hours
- Load: Heavy writes
- Monitor: Data dir size, WAL size
- Alert: Unexpected growth rate

## Metrics Collection

### System Metrics
- CPU usage (%)
- Memory RSS (MB)
- Disk I/O (MB/s)
- Network I/O (MB/s)
- File descriptors

### Database Metrics
- Queries per second
- Transactions per second
- Active connections
- Cache hit ratio
- WAL write rate

### Query Metrics
- Latency (p50, p95, p99)
- Error rate
- Slow queries (>1s)
- Deadlocks
- Lock waits

## Success Criteria

### Performance
- VaultGres >= 80% of PostgreSQL throughput
- p99 latency < 100ms for simple queries
- p99 latency < 5s for complex queries

### Stability
- Zero crashes in 24h soak test
- Memory growth < 10% over 24h
- CPU usage stable (no runaway)

### Correctness
- 100% data integrity after crashes
- Zero data loss with WAL enabled
- Correct query results vs PostgreSQL

## Running Tests

### Quick Smoke Test
```bash
cargo test --package e2e --test scenarios
```

### Full Test Suite
```bash
./e2e/run_all.sh
```

### Load Test
```bash
cargo test --package e2e --test load -- --ignored
```

### Soak Test (24h)
```bash
cargo test --package e2e --test soak -- --ignored --test-threads=1
```

### Comparison Benchmark
```bash
./e2e/benchmark.sh --compare-postgres
```

## CI/CD Integration

### PR Checks (Fast)
- Scenario tests (stateless)
- 5-minute load test
- Comparison smoke test

### Nightly (Comprehensive)
- Full scenario suite
- 1-hour load test
- 12-hour soak test
- Full comparison benchmark

### Weekly (Exhaustive)
- 24-hour soak test
- Stress tests
- Performance regression analysis
