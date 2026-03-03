# VaultGres Test Organization

## Test Structure

```
tests/
├── unit_tests.rs              # Unit test runner
├── integration_tests.rs       # Integration test runner
├── unit/                      # Unit tests (fast, no I/O)
│   ├── mod.rs
│   ├── config_test.rs        # Config logic tests
│   ├── lexer_test.rs         # Lexer/tokenizer tests
│   └── page_test.rs          # Page structure tests
└── integration/               # Integration tests (real I/O)
    ├── mod.rs
    ├── directory_test.rs     # Directory creation & persistence
    ├── disk_test.rs          # Disk I/O operations
    ├── end_to_end_test.rs    # Full system tests
    ├── executor_test.rs      # Query execution tests
    ├── integration_test.rs   # Protocol message tests
    ├── parser_test.rs        # SQL parsing tests
    ├── protocol_test.rs      # Wire protocol tests
    ├── storage_test.rs       # Storage layer tests
    ├── transaction_test.rs   # Transaction tests
    └── wal_test.rs           # WAL tests
```

## Running Tests

### All Tests
```bash
cargo test
```

### Unit Tests Only (Fast)
```bash
cargo test --test unit_tests
```

### Integration Tests Only
```bash
cargo test --test integration_tests
```

### Specific Test File
```bash
cargo test --test unit_tests config_test
cargo test --test integration_tests disk_test
```

### Module Tests (in src/)
```bash
cargo test --lib
```

## Test Categories

### Unit Tests (19 tests)
- **Fast**: No I/O, pure logic
- **Isolated**: No external dependencies
- **Focused**: Test single units

**Examples:**
- Config struct validation
- Lexer tokenization
- Page structure logic

### Integration Tests (82 tests)
- **Real I/O**: Actual filesystem operations
- **End-to-end**: Full system behavior
- **Comprehensive**: Multiple components

**Examples:**
- Disk persistence
- WAL file creation
- Query execution pipeline
- Protocol message handling

### Module Tests (69 tests)
- **In-source**: Located in `src/` files
- **Component**: Test module internals
- **Quick**: Run with `cargo test --lib`

## Test Statistics

```
Total: 170 tests
├── Unit tests:        19 (11%)
├── Integration tests: 82 (48%)
└── Module tests:      69 (41%)
```

## Best Practices

### Unit Tests
- No I/O operations
- No network calls
- No external dependencies
- Fast execution (< 1ms each)
- Use mocks/stubs if needed

### Integration Tests
- Test real behavior
- Use `tempfile::TempDir` for isolation
- Clean up resources
- Slower execution acceptable
- Test error conditions

### When to Use Which

**Unit Test:**
- Testing algorithms
- Validating data structures
- Checking business logic
- Configuration parsing

**Integration Test:**
- File I/O operations
- Database operations
- Network protocols
- Multi-component workflows

## CI/CD

```bash
# Fast feedback (unit tests only)
cargo test --test unit_tests

# Full validation (all tests)
cargo test

# With coverage
cargo tarpaulin --test unit_tests
cargo tarpaulin --test integration_tests
```
