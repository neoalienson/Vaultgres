# E2E Test Guide

## Running E2E Tests

E2E tests start actual VaultGres server instances and test them via psql. They must be run sequentially to avoid port conflicts.

### Run All E2E Tests

```bash
cargo test --test e2e_tests -- --test-threads=1
```

### Run Specific E2E Test

```bash
cargo test --test e2e_tests test_create_table -- --nocapture
```

### Run with Fail-Fast (Stop on First Failure)

To stop immediately when a test fails (useful for debugging):

```bash
# Using cargo directly
cargo test --package e2e --test scenarios -- --test-threads=1 --fail-fast

# Using cargo alias (recommended)
cargo test-e2e-ff
```

### Using Cargo Aliases

The project includes convenient aliases in `.cargo/config.toml`:

```bash
# Run E2E scenarios
cargo test-e2e

# Run E2E scenarios with fail-fast
cargo test-e2e-ff

# Run pet store tests
cargo test-pet-store

# Run pet store tests with fail-fast
cargo test-pet-store-ff
```

### Run Pet Store Comprehensive Test

```bash
# Standard run
cargo test --package e2e --test pet_store pet_store::test_pet_store_comprehensive -- --test-threads=1 --nocapture

# With fail-fast
cargo test --package e2e --test pet_store pet_store::test_pet_store_comprehensive -- --test-threads=1 --fail-fast --nocapture
```

### Prerequisites

- VaultGres must be built in release mode: `cargo build --release`
- PostgreSQL client (`psql`) must be installed
- Port 5432 must be available (VaultGres default)
- Docker must be running (for containerized E2E tests)

## Container Management

### Automatic Container Cleanup

When E2E tests start, the test framework will **automatically**:

1. **Clean up containers** from the current test project
2. **Remove orphan vaultgres containers** from crashed/previous tests
3. **Remove project networks** that may conflict
4. **Wait for port 5432** to become available

This happens automatically without user confirmation to support automated test runs.

Example output:
```
[TestEnv] Cleaning up any existing containers for project 'e2e-12345'...
[TestEnv] Removing any orphan vaultgres containers...
[TestEnv] Found 2 orphan vaultgres container(s), removing...
[TestEnv] Removing project networks...
[TestEnv] Waiting for cleanup to complete...
[TestEnv] Cleanup completed after 3 iterations
```

### Manual Cleanup

If you want to manually clean up containers before running tests:

```bash
# Stop and remove all vaultgres containers
docker ps -a --filter "name=vaultgres" --format "{{.Names}}" | xargs docker rm -f

# Or using docker compose
cd docker && docker compose down -v
```

## Debugging Test Failures

### Server Crashes

When a database query fails with connection errors, the test framework will **automatically fetch and display the last 50 lines of server logs**:

```
[DB] Executing: INSERT INTO orders VALUES (608, 'product8', 80)
[DB] Error: server closed the connection unexpectedly
        This probably means the server terminated abnormally
        before or while processing the request.

[!!!] SERVER CRASH DETECTED! Fetching server logs...
[Logs] Fetching logs for container on port 5432...

================================================================================
[Logs] === VAULTGRES SERVER LOGS (Last 50 lines) ===
================================================================================
STDOUT:
INFO: VaultGres starting on port 5432
ERROR: panic at 'attempt to divide by zero', src/executor/arithmetic.rs:42:5
...

================================================================================
[Logs] === END OF LOGS ===
```

### Manually Fetch Logs

You can also manually fetch server logs in your tests:

```rust
#[test]
fn test_with_debug() {
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();
    
    // Fetch logs at any point
    env.fetch_server_logs();
    
    // Continue with test...
}
```

### Increase Log Verbosity

To get more detailed logs, modify `docker/docker-compose.yml`:

```yaml
environment:
  - RUST_LOG=debug  # or trace for maximum detail
```

Then rebuild and re-run:

```bash
cd docker && docker compose build vaultgres
cargo test --package e2e --test pet_store -- --nocapture
```

### Common Crash Patterns

| Error Pattern | Likely Cause |
|---------------|--------------|
| `panic at 'attempt to divide by zero'` | Arithmetic expression bug |
| `panic at 'index out of bounds'` | Array/tuple access bug |
| `thread 'main' panicked at` | Unhandled error in executor |
| `connection refused` | Server didn't start or crashed during startup |

## Test Coverage

All 24 E2E tests pass:

- ✅ DDL operations (CREATE, DROP, DESCRIBE)
- ✅ DML operations (INSERT, SELECT, UPDATE, DELETE)
- ✅ WHERE clause with all comparison operators
- ✅ ORDER BY (ASC/DESC)
- ✅ LIMIT/OFFSET
- ✅ Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- ✅ Error handling (duplicate tables, wrong column counts, etc.)
- ✅ Complete CRUD workflows

## Why Sequential Execution?

E2E tests each start a server on port 5433. Running in parallel causes:
- Port conflicts
- Server startup race conditions
- Test interference

The `TEST_LOCK` mutex ensures only one test runs at a time.

## Test Structure

Each test:
1. Starts a VaultGres server
2. Executes SQL via psql
3. Validates results
4. Cleans up (server killed on drop)

## Troubleshooting

**Tests fail with "Failed to start server":**
- Ensure `cargo build --release` has been run
- Check port 5433 is not in use: `lsof -i :5433`

**Tests timeout:**
- Increase sleep duration in `TestServer::start()`
- Check server logs for startup errors

**psql not found:**
- Install PostgreSQL client: `sudo apt install postgresql-client`
