# Better E2E Test Frameworks for RustGres

## Current Issues

1. **Port conflicts** - All tests use same port (5433)
2. **State persistence** - Data directories shared across tests
3. **Process management** - Manual server start/stop is fragile
4. **No isolation** - Tests interfere with each other

## Recommended Solutions

### 1. **testcontainers-rs** (Best for Docker-based testing)

```rust
use testcontainers::*;

#[test]
fn test_with_container() {
    let docker = clients::Cli::default();
    let container = docker.run(RustGresImage::default());
    let port = container.get_host_port_ipv4(5433);
    
    // Each test gets isolated container with unique port
    let client = postgres::Client::connect(
        &format!("host=localhost port={} user=postgres", port),
        postgres::NoTls
    ).unwrap();
    
    // Test runs in complete isolation
}
```

**Pros:**
- Complete isolation per test
- Automatic port allocation
- Clean state guaranteed
- Parallel test execution
- Industry standard

**Cons:**
- Requires Docker
- Slower startup (~2-3s per container)

### 2. **sqlx** with embedded testing (Best for Rust-native)

```rust
use sqlx::postgres::PgPoolOptions;

#[sqlx::test]
async fn test_create_table(pool: PgPool) -> sqlx::Result<()> {
    sqlx::query("CREATE TABLE users (id INT, name TEXT)")
        .execute(&pool)
        .await?;
    
    // Automatic cleanup after test
    Ok(())
}
```

**Pros:**
- Native Rust async
- Automatic migrations
- Built-in test isolation
- Fast execution
- Type-safe queries

**Cons:**
- Requires PostgreSQL installed
- Need to adapt for RustGres protocol

### 3. **rstest** with fixtures (Best for current setup)

```rust
use rstest::*;
use std::sync::Arc;
use tokio::sync::Mutex;

#[fixture]
fn test_server() -> TestServer {
    let port = get_random_port(); // Use random port!
    TestServer::start_on_port(port)
}

#[rstest]
fn test_create_table(test_server: TestServer) {
    // Each test gets fresh server on unique port
    test_server.execute("CREATE TABLE users (id INT)").unwrap();
}
```

**Pros:**
- Minimal changes to existing code
- Fixtures handle setup/teardown
- Can use random ports
- Works with current architecture

**Cons:**
- Still requires process management
- Not as isolated as containers

### 4. **cucumber-rs** (Best for BDD/acceptance testing)

```gherkin
Feature: Database Operations
  Scenario: Create and query table
    Given a fresh database
    When I execute "CREATE TABLE users (id INT, name TEXT)"
    And I execute "INSERT INTO users VALUES (1, 'Alice')"
    Then the table "users" should have 1 row
```

```rust
#[given("a fresh database")]
async fn fresh_database(world: &mut World) {
    world.server = TestServer::start().await;
}

#[when(expr = "I execute {string}")]
async fn execute_sql(world: &mut World, sql: String) {
    world.result = world.server.execute(&sql).await;
}
```

**Pros:**
- Human-readable tests
- Great for acceptance testing
- Non-technical stakeholders can read
- Excellent documentation

**Cons:**
- More boilerplate
- Overkill for unit-style E2E tests

### 5. **Custom Test Harness with Port Pool** (Recommended for RustGres)

```rust
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(5433);

struct TestServer {
    port: u16,
    process: Child,
    data_dir: TempDir,
}

impl TestServer {
    fn start() -> Self {
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        let data_dir = TempDir::new().unwrap();
        
        let process = Command::new("./target/release/rustgres")
            .env("RUSTGRES_PORT", port.to_string())
            .env("RUSTGRES_DATA_DIR", data_dir.path())
            .spawn()
            .unwrap();
        
        thread::sleep(Duration::from_secs(1));
        
        Self { port, process, data_dir }
    }
    
    fn execute_sql(&self, sql: &str) -> Result<String, String> {
        let output = Command::new("psql")
            .args(&[
                "-h", "localhost",
                "-p", &self.port.to_string(),
                "-U", "postgres",
                "-d", "postgres",
                "-c", sql
            ])
            .output()
            .unwrap();
        
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        } else {
            Err(String::from_utf8_lossy(&output.stderr).to_string())
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        // data_dir automatically cleaned up by TempDir
    }
}
```

**Pros:**
- Each test gets unique port
- Automatic temp directory cleanup
- Parallel execution safe
- Minimal dependencies
- Works with current code

**Cons:**
- Need to make RustGres port configurable
- Need to make data directory configurable

## Recommended Approach for RustGres

**Short term (Quick fix):**
1. Use **rstest** with random ports
2. Use **tempfile** for data directories
3. Keep current psql-based approach

**Medium term (Better isolation):**
1. Implement **custom test harness** with port pool
2. Make RustGres configurable via env vars
3. Add proper async test support

**Long term (Production-ready):**
1. Use **testcontainers-rs** for full isolation
2. Add **sqlx** integration tests
3. Add **cucumber-rs** for acceptance tests

## Implementation Plan

### Phase 1: Make RustGres Configurable (1 hour)

```rust
// src/main.rs
let port = env::var("RUSTGRES_PORT")
    .unwrap_or_else(|_| "5433".to_string())
    .parse()
    .unwrap();

let data_dir = env::var("RUSTGRES_DATA_DIR")
    .unwrap_or_else(|_| "./data".to_string());
```

### Phase 2: Update Test Framework (2 hours)

```toml
[dev-dependencies]
rstest = "0.18"
tempfile = "3.8"
```

```rust
// tests/e2e_tests.rs
use rstest::*;
use tempfile::TempDir;

#[fixture]
fn test_server() -> TestServer {
    TestServer::start_with_random_port()
}

#[rstest]
fn test_create_table(test_server: TestServer) {
    // Test code
}
```

### Phase 3: Run Tests in Parallel (immediate)

```bash
cargo test --test e2e_tests -- --test-threads=4
```

## Comparison Matrix

| Framework | Isolation | Speed | Setup | Parallel | Recommended |
|-----------|-----------|-------|-------|----------|-------------|
| Current | ❌ Poor | ⚡ Fast | ✅ Easy | ❌ No | ❌ |
| testcontainers | ✅ Perfect | 🐌 Slow | ⚠️ Medium | ✅ Yes | ✅ Long-term |
| sqlx | ✅ Good | ⚡ Fast | ⚠️ Medium | ✅ Yes | ✅ Medium-term |
| rstest | ⚠️ Good | ⚡ Fast | ✅ Easy | ✅ Yes | ✅ Short-term |
| cucumber | ✅ Good | ⚡ Fast | ❌ Hard | ✅ Yes | ⚠️ Optional |
| Custom Harness | ✅ Good | ⚡ Fast | ⚠️ Medium | ✅ Yes | ✅ Recommended |

## Immediate Action

**Fix current tests in 30 minutes:**

1. Add port configuration to RustGres
2. Use atomic counter for port allocation
3. Use tempfile for data directories
4. Tests will run in parallel successfully

This is the **fastest path to 100% passing E2E tests**.
