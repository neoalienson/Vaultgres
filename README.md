# VaultGres

[![CI](https://github.com/neoalienson/vaultgres/workflows/CI/badge.svg)](https://github.com/neoalienson/vaultgres/actions/workflows/ci.yml)
[![Integration Tests](https://github.com/neoalienson/vaultgres/workflows/Integration%20Tests/badge.svg)](https://github.com/neoalienson/vaultgres/actions/workflows/integration.yml)
[![codecov](https://codecov.io/gh/neoalienson/Vaultgres/branch/main/graph/badge.svg)](https://codecov.io/gh/neoalienson/Vaultgres)
[![License](https://img.shields.io/badge/license-Apache%202.0%2FMIT-blue.svg)](LICENSE)
[![Rust Version](https://img.shields.io/badge/rust-1.82%2B-orange.svg)](https://www.rust-lang.org)

A **high-performance, PostgreSQL-compatible relational database management system** written in Rust - delivering ACID compliance, advanced query optimization, modern concurrency with memory safety guarantees, and enterprise-grade security by design.

## Purpose

VaultGres is a fully-featured RDBMS built from the ground up in Rust, providing:

- **PostgreSQL Wire Protocol Compatibility**: Drop-in replacement for existing PostgreSQL clients
- **PostgreSQL SQL Compatibility**: Full SQL standard support with PostgreSQL extensions
- **ACID Transactions**: Full transactional support with MVCC (Multi-Version Concurrency Control)
- **Advanced Query Engine**: Cost-based optimizer with parallel execution
- **Memory Safety**: Zero-cost abstractions with Rust's ownership model
- **Security by Design**: Enterprise-grade security features built into the core

**Key Benefits (Target State):**
- 🔒 **Memory Safe**: No buffer overflows, use-after-free, or data races
- 🛡️ **Security First**: TDE, column-level encryption, audit logging, data masking
- 🔄 **Full ACID**: Serializable isolation with optimistic concurrency control
- 🔌 **Compatible**: Works with existing PostgreSQL tools and drivers
- 📊 **Modern Architecture**: Async runtime, vectorized execution, columnar storage
- 🛠️ **Easy to Deploy**: Single binary, no external dependencies

## Quick Start

### Installation

**From Binary:**
```bash
# Download latest release
curl -L https://github.com/vaultgres/vaultgres/releases/latest/download/vaultgres-linux-x64.tar.gz | tar xz
sudo mv vaultgres /usr/local/bin/
```

**From Source:**
```bash
git clone https://github.com/vaultgres/vaultgres.git
cd vaultgres
cargo build --release
sudo cp target/release/vaultgres /usr/local/bin/
```

**Using Docker:**
```bash
# Pull and run
docker run -d -p 5432:5432 --name vaultgres vaultgres:latest

# Or build locally
docker build -f docker/Dockerfile -t vaultgres:latest .
docker run -d -p 5432:5432 vaultgres:latest

# With persistent data
docker run -d -p 5432:5432 \
  -v vaultgres-data:/var/lib/vaultgres/data \
  vaultgres:latest
```

### Initialize Database

```bash
# Initialize data directory
vaultgres init -D /var/lib/vaultgres/data

# Start server
vaultgres start -D /var/lib/vaultgres/data -p 5432

# Create database
vaultgres createdb mydb
```

### Connect

```bash
# Using psql (PostgreSQL client)
psql -h localhost -p 5432 -U postgres -d mydb

# Using any PostgreSQL-compatible client
# Python: psycopg2, asyncpg
# Node.js: pg, node-postgres
# Go: lib/pq
# Rust: tokio-postgres, sqlx
```

### Basic Usage

```sql
-- Create table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert data
INSERT INTO users (email) VALUES ('user@example.com');

-- Query with index
CREATE INDEX idx_users_email ON users(email);
SELECT * FROM users WHERE email = 'user@example.com';

-- Transaction
BEGIN;
UPDATE users SET email = 'new@example.com' WHERE id = 1;
COMMIT;
```

## Documentation

### For Users
- **[Quick Start Tutorial](docs/users/QUICKSTART.md)** - First steps with VaultGres
- **[SQL Reference](docs/users/SQL.md)** - Supported SQL syntax and features

### For Database Administrators
- **[Installation Guide](docs/admins/INSTALLATION.md)** - Build, install, and configure VaultGres
- **[Configuration Guide](docs/admins/CONFIGURATION.md)** - Server configuration and tuning
- **[Server Operations](docs/admins/SERVER.md)** - Database administration tasks
- **[Logging](docs/admins/LOGGING.md)** - Logging configuration and best practices

### For Developers
- **[Architecture Overview](docs/developers/ARCHITECTURE.md)** - System design and components
- **[Contributing Guide](docs/developers/CONTRIBUTING.md)** - How to contribute to VaultGres
- **[Coding Standards](docs/developers/STANDARDS.md)** - Development guidelines and conventions
- **[Storage Engine](docs/developers/STORAGE.md)** - Buffer pool, indexes, WAL, recovery
- **[Transaction Manager](docs/developers/TRANSACTIONS.md)** - MVCC, isolation levels, concurrency
- **[Query Optimizer](docs/developers/OPTIMIZER.md)** - Cost model, statistics, plan generation
- **[Testing Guide](docs/developers/testing/TESTING.md)** - Test organization and running instructions
- **[Roadmap](docs/developers/ROADMAP.md)** - Future features and milestones

## Building from Source

```bash
# Clone repository
git clone https://github.com/vaultgres/vaultgres.git
cd vaultgres

# Build release binary
cargo build --release

# Run tests
cargo test

# Run benchmarks
cargo bench

# Build documentation
cargo doc --no-deps --open
```

## Configuration

Basic `vaultgres.conf`:

```ini
# Connection settings
listen_addresses = '*'
port = 5432
max_connections = 100

# Memory settings
shared_buffers = 256MB
work_mem = 4MB
maintenance_work_mem = 64MB

# WAL settings
wal_level = replica
max_wal_size = 1GB
checkpoint_timeout = 5min

# Security settings
tde_enabled = true
tde_key_rotation_days = 90
audit_log_enabled = true
audit_log_level = all
ssl_enabled = true
ssl_min_version = TLSv1.3

# Query tuning
effective_cache_size = 4GB
random_page_cost = 1.1
```

See [Configuration Guide](docs/admins/CONFIGURATION.md) for all options.

## Contributing

We welcome contributions! See [Contributing Guide](docs/developers/CONTRIBUTING.md) for:
- Code of conduct
- Development workflow
- Testing requirements
- Pull request process

## License

VaultGres is licensed under the Apache License 2.0 or MIT License, at your option.

## Acknowledgments

VaultGres builds on ideas from:
- **PostgreSQL**: Query optimizer and MVCC design
- **SQLite**: Testing methodology and SQL parser
- **DuckDB**: Vectorized execution engine
- **CockroachDB**: Distributed transaction protocols
- **DataFusion**: Query execution framework (Apache Arrow)

## Related Projects

- **[pgwire](https://github.com/sunng87/pgwire)** - PostgreSQL wire protocol implementation
- **[sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs)** - SQL parser library
- **[Apache Arrow](https://arrow.apache.org/)** - Columnar data format
- **[sled](https://github.com/spacejam/sled)** - Embedded database engine
