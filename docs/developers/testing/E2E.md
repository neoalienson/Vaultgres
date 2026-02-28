# E2E Test Guide

## Running E2E Tests

E2E tests start actual RustGres server instances and test them via psql. They must be run sequentially to avoid port conflicts.

### Run All E2E Tests

```bash
cargo test --test e2e_tests -- --test-threads=1
```

### Run Specific E2E Test

```bash
cargo test --test e2e_tests test_create_table -- --nocapture
```

### Prerequisites

- RustGres must be built in release mode: `cargo build --release`
- PostgreSQL client (`psql`) must be installed
- Port 5433 must be available

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
1. Starts a RustGres server
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
