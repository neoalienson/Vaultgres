# VaultGres Project Context

## Project Overview

**VaultGres** is a high-performance, PostgreSQL-compatible relational database management system (RDBMS) written in Rust. It delivers ACID compliance, advanced query optimization, modern concurrency with MVCC (Multi-Version Concurrency Control), and enterprise-grade security features.

### Key Characteristics
- **PostgreSQL Wire Protocol Compatible**: Works with existing PostgreSQL clients (psql, psycopg2, node-postgres, etc.)
- **Written in Rust**: Memory safety guarantees, no buffer overflows or data races
- **Volcano Execution Model**: Iterator-based query execution
- **Pluggable Storage**: B+Tree and LSM-Tree implementations
- **Security by Design**: TDE, column-level encryption, audit logging, row-level security

## Project Structure

```
vaultgres/
├── src/
│   ├── main.rs                  # Entry point - server startup
│   ├── lib.rs                   # Library exports
│   ├── protocol/                # PostgreSQL wire protocol
│   ├── parser/                  # SQL parser (lexer, parser, AST)
│   ├── optimizer/               # Query optimizer (logical & physical planning)
│   ├── planner/                 # Query planner
│   ├── executor/                # Query execution engine
│   │   ├── volcano/             # Volcano-style executors (new model)
│   │   ├── operators/           # Core operator implementations
│   │   ├── parallel/            # Parallel execution infrastructure
│   │   └── builtin/             # Built-in functions
│   ├── catalog/                 # System catalog & metadata
│   ├── storage/                 # Storage engine (buffer pool, pages, indexes)
│   ├── transaction/             # Transaction manager & MVCC
│   ├── wal/                     # Write-ahead logging & recovery
│   ├── statistics/              # Table/column statistics
│   └── metrics/                 # Performance metrics
├── tests/
│   ├── integration/             # Integration tests
│   ├── e2e/                     # End-to-end scenario tests
│   └── integration_tests.rs     # Test harness
├── benches/                     # Performance benchmarks
├── docs/
│   ├── users/                   # User-facing documentation
│   ├── admins/                  # DBA guides (config, operations)
│   └── developers/              # Developer docs (architecture, contributing)
├── docker/                      # Docker build files
├── examples/                    # Usage examples
└── scripts/                     # Utility scripts
```

## Building and Running

### Prerequisites
- **Rust**: 1.82+ (2024 edition)
- **OS**: Linux, macOS, Windows

### Build Commands

```bash
# Build debug
cargo build

# Build release (optimized)
cargo build --release

# Run tests
cargo test

# Run with logging
RUST_LOG=debug cargo run

# Run benchmarks
cargo bench

# Generate documentation
cargo doc --no-deps --open

# Lint
cargo clippy

# Format
cargo fmt
```

### Running the Server

```bash
# Using the startup script
./start-server.sh

# Or directly
cargo run --release

# With custom config
VAULTGRES_CONFIG=config.dev.yaml cargo run --release
```

### Default Configuration
- **Host**: 127.0.0.1
- **Port**: 5433 (note: not default PostgreSQL 5432)
- **Data Directory**: `./data`
- **WAL Directory**: `./wal`
- **Buffer Pool**: 1000 pages

### Connecting

```bash
# Using psql
psql -h 127.0.0.1 -p 5433 -U postgres -d testdb

# Python (asyncpg/psycopg2)
# Node.js (pg)
# Any PostgreSQL-compatible client
```

## Configuration Files

| File | Purpose |
|------|---------|
| `config.yaml` | Default configuration |
| `config.dev.yaml` | Development environment settings |
| `config.prod.yaml` | Production environment settings |

Key configuration sections: `server`, `storage`, `logging`, `transaction`, `wal`, `performance`

## Development Conventions

### Code Style
- **Formatting**: `rustfmt` with 100 char max width, 4-space tabs
- **Edition**: Rust 2024
- **Clippy**: Configured in `.clippy.toml` with relaxed thresholds for complex types

### Testing Practices
- **Unit Tests**: Co-located with source in each module
- **Integration Tests**: In `tests/integration/` and `tests/e2e/`
- **Test Naming**: Descriptive names indicating scenario being tested
- **Assertions**: Use standard `assert!` macros

### Architecture Patterns
- **Volcano Model**: Executors implement `Executor` trait with `next()` method
- **Error Handling**: Custom error types per module (e.g., `ExecutorError`, `StorageError`)
- **Tuple Representation**: `Tuple = HashMap<String, Value>` with `Value` enum from catalog
- **MVCC**: Timestamp-based ordering with snapshot isolation

### Git Workflow
- Feature branches from `main`
- Descriptive commit messages
- CI/CD via GitHub Actions (see `.github/workflows/`)

## Key Documentation

| Document | Purpose |
|----------|---------|
| `docs/developers/ARCHITECTURE.md` | System design and layer overview |
| `docs/developers/CONTRIBUTING.md` | Contribution guidelines |
| `docs/developers/STORAGE.md` | Storage engine internals |
| `docs/developers/TRANSACTIONS.md` | MVCC and transaction handling |
| `docs/developers/OPTIMIZER.md` | Query optimization strategies |
| `docs/developers/ROADMAP.md` | Plan and Roadmap |

## Current Development Focus

`docs/developers/ROADMAP.md`

## Common Commands

```bash
# Quick test run
cargo test --lib

# Test specific module
cargo test --lib executor

# Test with output
cargo test -- --nocapture

# Check without building
cargo check

# Profile build
cargo build --release --features profiling

# E2E scenario test for pet store
cd /home/neo/projects/vaultgres/docker && docker compose down -v 2>/dev/null; docker compose build vaultgres 2>&1 
cd tests/e2e && cargo test --package e2e --test pet_store -- --test-threads=1  --nocapture
```

## Troubleshooting

- **Port conflicts**: Default port 5433 may conflict with existing PostgreSQL - change in `config.yaml`
- **Build errors**: Ensure Rust 1.82+ with `rustc --version`
- **Test failures**: Check data directory permissions in `./data` and `./wal`
