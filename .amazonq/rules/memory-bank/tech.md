# Technology Stack

## Programming Languages

### Rust
- **Version**: 1.75+ (2021 edition)
- **Primary Language**: All core components written in Rust
- **Key Features Used**:
  - Ownership and borrowing for memory safety
  - Zero-cost abstractions
  - Pattern matching and enums
  - Trait-based polymorphism
  - Async/await (planned for protocol layer)

### Shell Scripts
- **Usage**: End-to-end testing scripts in `tests/e2e/`
- **Purpose**: Integration testing and deployment automation

## Core Dependencies

### Runtime Dependencies
```toml
thiserror = "1.0"          # Error handling with derive macros
parking_lot = "0.12"       # High-performance synchronization primitives
dashmap = "5.5"            # Concurrent hash map
env_logger = "0.11"        # Logging implementation
log = "0.4"                # Logging facade
serde = "1.0"              # Serialization framework
serde_yaml = "0.9"         # YAML configuration parsing
ctrlc = "3.4"              # Signal handling for graceful shutdown
```

### Development Dependencies
```toml
criterion = "0.5"          # Benchmarking framework
proptest = "1.4"           # Property-based testing
tempfile = "3.8"           # Temporary file/directory creation for tests
```

## Build System

### Cargo
- **Build Tool**: Cargo (Rust's package manager and build system)
- **Edition**: 2021
- **Package Name**: vaultgres
- **Version**: 0.1.0

### Build Profiles
- **Debug**: Default development build with debug symbols
- **Release**: Optimized production build

## Development Commands

### Building
```bash
# Debug build (fast compilation, slower runtime)
cargo build

# Release build (optimized for performance)
cargo build --release

# Check code without building
cargo check

# Build documentation
cargo doc --no-deps --open
```

### Testing
```bash
# Run all tests
cargo test

# Run specific test suite
cargo test --test unit_tests
cargo test --test integration_tests
cargo test --test e2e_tests

# Run tests with output
cargo test -- --nocapture

# Run tests in specific module
cargo test storage::

# Run end-to-end shell tests
cd tests/e2e && ./test_all_sql.sh
```

### Benchmarking
```bash
# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench --bench storage_bench
```

### Code Quality
```bash
# Format code
cargo fmt

# Check formatting
cargo fmt -- --check

# Run linter
cargo clippy --all-targets --all-features

# Or use the lint script
./lint.sh

# Fix warnings automatically
cargo clippy --fix --all-targets --all-features

# Run linter with strict mode (fail on warnings)
cargo clippy --all-targets --all-features -- -D warnings
```

### Running
```bash
# Run debug build
cargo run

# Run release build
cargo run --release

# Run with specific config
cargo run -- --config config.dev.yaml

# Initialize database
cargo run -- init -D /var/lib/vaultgres/data

# Start server
cargo run -- start -D /var/lib/vaultgres/data -p 5432
```

### Cleaning
```bash
# Remove build artifacts
cargo clean

# Remove target directory
rm -rf target/
```

## Development Environment

### Required Tools
- **Rust Toolchain**: rustc, cargo (install via rustup)
- **Git**: Version control
- **Shell**: bash/zsh for running test scripts

### Recommended Tools
- **rust-analyzer**: IDE language server
- **cargo-watch**: Auto-rebuild on file changes
- **cargo-expand**: Macro expansion viewer
- **cargo-flamegraph**: Performance profiling

### IDE Setup
- **VS Code**: rust-analyzer extension
- **IntelliJ IDEA**: Rust plugin
- **Vim/Neovim**: rust.vim + coc-rust-analyzer

## System Requirements

### Development
- **OS**: Linux, macOS, Windows
- **Memory**: 4GB+ RAM for compilation
- **Disk**: 2GB+ for source and build artifacts
- **CPU**: Multi-core recommended for parallel compilation

### Runtime
- **OS**: Linux (primary), macOS, Windows
- **Memory**: 512MB minimum, 4GB+ recommended
- **Disk**: SSD recommended for production
- **Network**: TCP/IP for client connections

## Configuration Management

### Configuration Files
- `config.yaml` - Default configuration
- `config.dev.yaml` - Development overrides
- `config.prod.yaml` - Production settings

### Configuration Format
- **Format**: YAML
- **Library**: serde_yaml
- **Location**: Project root or specified via `--config` flag

### Key Configuration Sections
```yaml
# Connection settings
listen_addresses: '*'
port: 5432
max_connections: 100

# Memory settings
shared_buffers: 256MB
work_mem: 4MB

# WAL settings
wal_level: replica
max_wal_size: 1GB

# Logging
log_level: info
log_destination: stderr
```

## Logging

### Logging Framework
- **Facade**: `log` crate for logging API
- **Implementation**: `env_logger` for output
- **Configuration**: Via `RUST_LOG` environment variable

### Log Levels
```bash
# Set log level
export RUST_LOG=debug
export RUST_LOG=vaultgres=debug,storage=trace

# Run with logging
RUST_LOG=info cargo run
```

### Log Output
- **Default**: stderr
- **Format**: Timestamp, level, module, message
- **File**: Configurable via `server.log`

## Testing Infrastructure

### Test Organization
- **Unit Tests**: Inline with source code (`#[cfg(test)]`)
- **Integration Tests**: `tests/integration/` directory
- **E2E Tests**: `tests/e2e/` shell scripts
- **Edge Tests**: `*_edge_tests.rs` files for edge cases

### Test Utilities
- `tempfile` - Temporary directories for disk tests
- `proptest` - Property-based testing
- Custom test harness in `tests/test_harness.rs`

## Performance Tools

### Benchmarking
- **Framework**: Criterion.rs
- **Location**: `benches/` directory
- **Output**: HTML reports in `target/criterion/`

### Profiling
- **Tools**: cargo-flamegraph, perf, valgrind
- **Usage**: Profile release builds for accurate results

## Deployment

### Binary Distribution
```bash
# Build release binary
cargo build --release

# Binary location
target/release/vaultgres

# Install system-wide
sudo cp target/release/vaultgres /usr/local/bin/
```

### Docker (Planned)
- Dockerfile for containerized deployment
- Multi-stage build for minimal image size

## Version Control

### Git Workflow
- **Main Branch**: `main` - stable releases
- **Development**: Feature branches
- **Tags**: Version tags (v0.1.0, etc.)

### Commit Conventions
- Conventional commits format
- Clear, descriptive commit messages
- Reference issue numbers where applicable
