# End-to-End Tests

## Overview
This directory contains end-to-end tests for RustGres using both shell scripts and Rust-based tests.

## Test Types

### 1. Rust-Based E2E Tests (Recommended)
Located in `tests/e2e_tests.rs`

**Advantages:**
- Type-safe and integrated with Cargo
- Automatic server lifecycle management
- Better error handling
- Runs with `cargo test`

**Run:**
```bash
cargo test --test e2e_tests
```

**Features:**
- Uses `postgres` crate for real PostgreSQL protocol connections
- Automatically starts/stops server
- Tests DDL operations (CREATE, DROP)
- Validates error conditions

### 2. Shell Script Tests
Located in `tests/e2e/*.sh`

**Advantages:**
- Quick manual testing
- Easy to read and modify
- Uses psql directly

**Run:**
```bash
cd tests/e2e
bash test_ddl_execution.sh
bash test_all_sql.sh
```

## Available Tests

### Rust Tests (`e2e_tests.rs`)
- `test_create_table` - CREATE TABLE and duplicate detection
- `test_drop_table` - DROP TABLE and error handling
- `test_drop_table_if_exists` - IF EXISTS clause
- `test_ddl_workflow` - Complete DDL lifecycle

### Shell Scripts
- `test_ddl_execution.sh` - DDL operations with error cases
- `test_all_sql.sh` - Comprehensive SQL statement testing
- `test_create.sh` - CREATE TABLE testing
- `test_describe.sh` - DESCRIBE statement testing
- `test_drop.sh` - DROP TABLE testing
- `test_e2e.sh` - Basic end-to-end connectivity
- `test_sql.sh` - SQL parsing tests

## Writing New Tests

### Rust E2E Test Template
```rust
#[test]
fn test_my_feature() {
    let server = TestServer::start();
    let mut client = server.connect().expect("Failed to connect");
    
    client.execute("YOUR SQL HERE", &[])
        .expect("Operation failed");
}
```

### Shell Script Template
```bash
#!/bin/bash
set -e

./target/release/rustgres > server.log 2>&1 &
SERVER_PID=$!
sleep 2

cleanup() {
    kill $SERVER_PID 2>/dev/null || true
}
trap cleanup EXIT

psql -h localhost -p 5433 -U postgres -d postgres << EOF
YOUR SQL HERE
EOF
```

## CI/CD Integration

Add to your CI pipeline:
```yaml
- name: Build
  run: cargo build --release

- name: Run E2E Tests
  run: cargo test --test e2e_tests
```

## Troubleshooting

**Server won't start:**
- Check if port 5433 is available
- Ensure binary is built: `cargo build --release`

**Connection refused:**
- Increase sleep duration in TestServer::start()
- Check server logs

**Tests hang:**
- Server process may not be killed properly
- Manually kill: `pkill rustgres`
