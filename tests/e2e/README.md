# E2E Testing Framework ✅

Comprehensive end-to-end testing for VaultGres using Docker containers.

**Status**: Working! 3/3 smoke tests passing.

## Quick Start

```bash
cd e2e

# Run all smoke tests
./run_all.sh quick

# Run specific smoke test
./run_all.sh quick test_basic_create_table

# List available smoke tests
./run_all.sh quick list

# Run all scenarios
./run_all.sh scenarios

# Run specific scenario
./run_all.sh scenarios test_oltp_simple_transactions

# List available scenarios
./run_all.sh scenarios list
```

## Test Results

```
=== Test: Basic CREATE TABLE ===
[TestEnv] Starting containers...
[TestEnv] Services: ["vaultgres"]
[TestEnv] Waiting 10s for containers to be ready...
[TestEnv] Ready!
[DB] Executing: CREATE TABLE test (id INT, name TEXT)
[DB] Success
=== Test PASSED ===

test test_basic_create_table ... ok
test test_basic_insert_select ... ok  
test test_multiple_inserts ... ok

test result: ok. 3 passed; 0 failed
```

## Architecture

```
e2e/
├── STRATEGY.md              # Detailed testing strategy
├── docker-compose.yml       # Multi-container orchestration
├── prometheus.yml           # Metrics collection config
├── lib.rs                   # Shared test infrastructure
├── scenarios/               # Real-world workload tests
├── load/                    # Load testing (ramp-up, spike)
├── soak/                    # Long-running stability tests
├── comparison/              # VaultGres vs PostgreSQL benchmarks
└── run_all.sh              # Test runner script
```

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Rust toolchain
- psql client

### Build VaultGres Image
```bash
docker build -f docker/Dockerfile -t vaultgres:latest .
```

### Run Tests

**Quick smoke test:**
```bash
./e2e/run_all.sh quick
./e2e/run_all.sh quick test_basic_create_table  # specific test
./e2e/run_all.sh quick list                     # list tests
```

**Scenario tests:**
```bash
./e2e/run_all.sh scenarios
./e2e/run_all.sh scenarios test_oltp_simple_transactions  # specific
./e2e/run_all.sh scenarios list                           # list
```

**Full test suite (30 min):**
```bash
./e2e/run_all.sh full
```

**Load tests only:**
```bash
./e2e/run_all.sh load
```

**Soak tests (24h):**
```bash
./e2e/run_all.sh soak
```

**Compare with PostgreSQL:**
```bash
./e2e/run_all.sh compare
```

**Start monitoring stack:**
```bash
./e2e/run_all.sh monitor
# Grafana: http://localhost:3000
# Prometheus: http://localhost:9090
# cAdvisor: http://localhost:8080
```

## Test Categories

### 1. Scenario Tests (`scenarios/`)
Real-world workload patterns:
- **OLTP**: High-frequency transactions, concurrent inserts
- **Analytics**: Complex joins, aggregations
- **Mixed**: Combined OLTP/OLAP workloads
- **Concurrent**: Multi-client stress tests

**Run:** `cargo test --package e2e --test scenarios`

### 2. Load Tests (`load/`)
Performance under varying load:
- **Ramp-up**: 10 → 1000 users over 30 min
- **Spike**: Sudden traffic bursts
- **Sustained**: Steady-state 200 users for 2h

**Run:** `cargo test --package e2e --test load -- --ignored`

### 3. Soak Tests (`soak/`)
Long-running stability (hours/days):
- **Memory leak**: 24h continuous operation
- **Connection churn**: 12h rapid connect/disconnect
- **Disk growth**: 48h write-heavy workload

**Run:** `cargo test --package e2e --test soak -- --ignored`

### 4. Comparison Tests (`comparison/`)
Side-by-side benchmarks vs PostgreSQL:
- Simple queries
- Complex joins
- Aggregations
- Bulk operations

**Run:** `cargo test --package e2e --test comparison`

## Monitoring

### Metrics Collected
- **System**: CPU, memory, disk I/O, network
- **Database**: QPS, TPS, connections, cache hit ratio
- **Query**: Latency (p50/p95/p99), errors, slow queries

### Dashboards
Access Grafana at `http://localhost:3000` (admin/admin) after running:
```bash
./e2e/run_all.sh monitor
```

## Test Infrastructure

### TestEnv Builder
```rust
let env = TestEnv::new()
    .with_vaultgres()      // Start VaultGres container
    .with_postgres()      // Start PostgreSQL for comparison
    .with_monitoring()    // Enable Prometheus/Grafana
    .with_persistence()   // Keep data between restarts
    .start();
```

### Database Operations
```rust
let db = env.vaultgres();
db.execute("CREATE TABLE users (id INT, name TEXT)").unwrap();
db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
let count: i32 = db.query_scalar("SELECT COUNT(*) FROM users");
let duration = db.time_query("SELECT * FROM users");
```

### Monitoring
```rust
let monitor = env.start_monitor();
// Run workload...
let metrics = monitor.stop();
assert!(metrics.memory_growth_mb < 100.0);
```

## Success Criteria

### Performance
- ✓ VaultGres >= 80% of PostgreSQL throughput
- ✓ p99 latency < 100ms (simple queries)
- ✓ p99 latency < 5s (complex queries)

### Stability
- ✓ Zero crashes in 24h soak test
- ✓ Memory growth < 10% over 24h
- ✓ CPU usage stable (no runaway)

### Correctness
- ✓ 100% data integrity after crashes
- ✓ Zero data loss with WAL enabled
- ✓ Correct query results vs PostgreSQL

## CI/CD Integration

### GitHub Actions
```yaml
- name: E2E Quick Tests
  run: ./e2e/run_all.sh quick

- name: E2E Comparison
  run: ./e2e/run_all.sh compare
```

### Nightly Tests
```yaml
- name: E2E Full Suite
  run: ./e2e/run_all.sh full
  
- name: E2E Soak Test
  run: ./e2e/run_all.sh soak
```

## Troubleshooting

### Containers won't start
```bash
docker-compose logs vaultgres
docker-compose logs postgres
```

### Tests fail
```bash
# Run with verbose output
cargo test --package e2e -- --nocapture

# Check container status
docker ps -a
docker stats
```

### Clean up
```bash
docker-compose down -v
docker system prune -f
```

## Future Enhancements

- [ ] Distributed testing (multiple nodes)
- [ ] Chaos engineering (network partitions, disk failures)
- [ ] Replication testing
- [ ] Backup/restore validation
- [ ] Security testing (SQL injection, auth)
- [ ] Custom workload generator (TPC-C, TPC-H)
