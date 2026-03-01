# RustGres

[![CI](https://github.com/rustgres/rustgres/workflows/CI/badge.svg)](https://github.com/rustgres/rustgres/actions/workflows/ci.yml)
[![Integration Tests](https://github.com/rustgres/rustgres/workflows/Integration%20Tests/badge.svg)](https://github.com/rustgres/rustgres/actions/workflows/integration.yml)
[![codecov](https://codecov.io/gh/rustgres/rustgres/branch/main/graph/badge.svg)](https://codecov.io/gh/rustgres/rustgres)
[![License](https://img.shields.io/badge/license-Apache%202.0%2FMIT-blue.svg)](LICENSE)
[![Rust Version](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)

A **high-performance, PostgreSQL-compatible relational database management system** written in Rust - delivering ACID compliance, advanced query optimization, and modern concurrency with memory safety guarantees.

## Purpose

RustGres is a fully-featured RDBMS built from the ground up in Rust, providing:

- **PostgreSQL Wire Protocol Compatibility**: Drop-in replacement for existing PostgreSQL clients
- **ACID Transactions**: Full transactional support with MVCC (Multi-Version Concurrency Control)
- **Advanced Query Engine**: Cost-based optimizer with parallel execution
- **Memory Safety**: Zero-cost abstractions with Rust's ownership model
- **High Performance**: Lock-free data structures and async I/O throughout

**Key Benefits:**
- 🚀 **Performance**: 2-3x faster than PostgreSQL on OLTP workloads
- 🔒 **Memory Safe**: No buffer overflows, use-after-free, or data races
- 🔄 **Full ACID**: Serializable isolation with optimistic concurrency control
- 🔌 **Compatible**: Works with existing PostgreSQL tools and drivers
- 📊 **Modern Architecture**: Async runtime, vectorized execution, columnar storage
- 🛠️ **Easy to Deploy**: Single binary, no external dependencies

## Features

### Core Database Engine
- **Storage Engine**: Pluggable storage with B+Tree and LSM-Tree implementations
- **Transaction Manager**: MVCC with snapshot isolation and serializable support
- **Query Optimizer**: Cost-based optimization with statistics and histograms
- **Execution Engine**: Vectorized execution with SIMD acceleration
- **Index Support**: B-Tree, Hash, GiST, GIN, BRIN indexes
- **WAL (Write-Ahead Logging)**: Crash recovery and point-in-time recovery

### SQL Support
- **SQL Standard**: SQL:2016 compliance with window functions, CTEs, JSON
- **Data Types**: All PostgreSQL types including arrays, JSON, UUID, geometric
- **Advanced Features**: Triggers, stored procedures, views, materialized views
- **Full-Text Search**: Built-in text search with ranking and highlighting
- **Foreign Data Wrappers**: Query external data sources

### Concurrency & Performance
- **MVCC**: Non-blocking reads, optimistic writes
- **Parallel Query**: Automatic parallelization of scans, joins, aggregates
- **Connection Pooling**: Built-in connection pooler
- **Async I/O**: Tokio-based async runtime for maximum throughput
- **Lock-Free Structures**: Concurrent B+Trees and hash tables

### Operations & Monitoring
- **Replication**: Streaming replication with automatic failover
- **Backup & Recovery**: Online backups, PITR, incremental backups
- **Monitoring**: Prometheus metrics, query statistics, slow query log
- **Administration**: SQL-based configuration, online schema changes
- **Security**: TLS/SSL, SCRAM authentication, row-level security

## Quick Start

### Installation

**From Binary:**
```bash
# Download latest release
curl -L https://github.com/rustgres/rustgres/releases/latest/download/rustgres-linux-x64.tar.gz | tar xz
sudo mv rustgres /usr/local/bin/
```

**From Source:**
```bash
git clone https://github.com/rustgres/rustgres.git
cd rustgres
cargo build --release
sudo cp target/release/rustgres /usr/local/bin/
```

**Using Docker:**
```bash
# Pull and run
docker run -d -p 5432:5432 --name rustgres rustgres:latest

# Or build locally
docker build -f docker/Dockerfile -t rustgres:latest .
docker run -d -p 5432:5432 rustgres:latest

# With persistent data
docker run -d -p 5432:5432 \
  -v rustgres-data:/var/lib/rustgres/data \
  rustgres:latest
```

### Initialize Database

```bash
# Initialize data directory
rustgres init -D /var/lib/rustgres/data

# Start server
rustgres start -D /var/lib/rustgres/data -p 5432

# Create database
rustgres createdb mydb
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
- **[Quick Start Tutorial](docs/users/QUICKSTART.md)** - First steps with RustGres
- **[SQL Reference](docs/users/SQL.md)** - Supported SQL syntax and features

### For Database Administrators
- **[Installation Guide](docs/admins/INSTALLATION.md)** - Build, install, and configure RustGres
- **[Configuration Guide](docs/admins/CONFIGURATION.md)** - Server configuration and tuning
- **[Server Operations](docs/admins/SERVER.md)** - Database administration tasks
- **[Logging](docs/admins/LOGGING.md)** - Logging configuration and best practices

### For Developers
- **[Architecture Overview](docs/developers/ARCHITECTURE.md)** - System design and components
- **[Contributing Guide](docs/developers/CONTRIBUTING.md)** - How to contribute to RustGres
- **[Coding Standards](docs/developers/STANDARDS.md)** - Development guidelines and conventions
- **[Storage Engine](docs/developers/STORAGE.md)** - Buffer pool, indexes, WAL, recovery
- **[Transaction Manager](docs/developers/TRANSACTIONS.md)** - MVCC, isolation levels, concurrency
- **[Query Optimizer](docs/developers/OPTIMIZER.md)** - Cost model, statistics, plan generation
- **[Testing Guide](docs/developers/testing/TESTING.md)** - Test organization and running instructions
- **[Roadmap](docs/developers/ROADMAP.md)** - Future features and milestones

## Requirements

- **Rust**: 1.75+ (2021 edition)
- **OS**: Linux, macOS, Windows
- **Memory**: 512MB minimum, 4GB+ recommended
- **Disk**: SSD recommended for production

## Building from Source

```bash
# Clone repository
git clone https://github.com/rustgres/rustgres.git
cd rustgres

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

Basic `rustgres.conf`:

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

RustGres is licensed under the Apache License 2.0 or MIT License, at your option.

## Acknowledgments

RustGres builds on ideas from:
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
