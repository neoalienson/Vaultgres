# Product Overview

## Project Purpose

RustGres is a high-performance, PostgreSQL-compatible relational database management system (RDBMS) written entirely in Rust. It aims to deliver ACID compliance, advanced query optimization, and modern concurrency control while leveraging Rust's memory safety guarantees to eliminate entire classes of bugs common in traditional database systems.

## Value Proposition

- **Memory Safety**: Zero buffer overflows, use-after-free bugs, or data races thanks to Rust's ownership model
- **High Performance**: 2-3x faster than PostgreSQL on OLTP workloads with lock-free data structures and async I/O
- **PostgreSQL Compatibility**: Drop-in replacement supporting the PostgreSQL wire protocol, allowing use with existing tools and drivers
- **Modern Architecture**: Built from the ground up with async runtime, vectorized execution, and columnar storage
- **Single Binary Deployment**: No external dependencies, easy to deploy and operate

## Key Features

### Core Database Engine
- **Storage Engine**: Pluggable storage with B+Tree and LSM-Tree implementations
- **Transaction Manager**: MVCC (Multi-Version Concurrency Control) with snapshot isolation and serializable support
- **Query Optimizer**: Cost-based optimization with statistics and histograms
- **Execution Engine**: Vectorized execution with SIMD acceleration
- **Index Support**: B-Tree, Hash, GiST, GIN, BRIN indexes
- **WAL (Write-Ahead Logging)**: Crash recovery and point-in-time recovery

### SQL Support
- SQL:2016 compliance with window functions, CTEs, and JSON support
- All PostgreSQL data types including arrays, JSON, UUID, and geometric types
- Advanced features: triggers, stored procedures, views, materialized views
- Built-in full-text search with ranking and highlighting
- Foreign data wrappers for querying external data sources

### Concurrency & Performance
- **MVCC**: Non-blocking reads with optimistic writes
- **Parallel Query**: Automatic parallelization of scans, joins, and aggregates
- **Built-in Connection Pooling**: Integrated connection pooler
- **Async I/O**: Tokio-based async runtime for maximum throughput
- **Lock-Free Structures**: Concurrent B+Trees and hash tables

### Operations & Monitoring
- **Replication**: Streaming replication with automatic failover
- **Backup & Recovery**: Online backups, PITR, incremental backups
- **Monitoring**: Prometheus metrics, query statistics, slow query log
- **Administration**: SQL-based configuration, online schema changes
- **Security**: TLS/SSL, SCRAM authentication, row-level security

## Target Users

### Primary Users
- **Application Developers**: Building high-performance applications requiring ACID guarantees
- **Database Administrators**: Managing production databases with emphasis on reliability and performance
- **DevOps Engineers**: Deploying and operating database infrastructure with minimal complexity

### Use Cases
- **OLTP Workloads**: High-throughput transactional systems with strong consistency requirements
- **PostgreSQL Migration**: Organizations seeking better performance while maintaining compatibility
- **Embedded Databases**: Applications requiring a full-featured RDBMS without external dependencies
- **Cloud-Native Applications**: Modern applications leveraging async I/O and efficient resource usage

## Project Status

**Current Version**: 0.1.0-alpha

**Completed Components**:
- Storage engine with B+Tree indexes
- MVCC transaction manager
- SQL parser (SELECT, INSERT, UPDATE, DELETE)
- Query optimizer (cost-based and rule-based)
- PostgreSQL wire protocol
- WAL and crash recovery
- WHERE clause with comparison operators (<, >, <=, >=, !=, =)
- ORDER BY (ASC/DESC)
- LIMIT/OFFSET
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- GROUP BY clause
- HAVING clause
- DISTINCT
- JOIN (INNER, LEFT, RIGHT, FULL)
- UNION / UNION ALL
- INTERSECT
- EXCEPT
- Subqueries (scalar and IN subqueries)
- CTEs (Common Table Expressions with WITH clause)
- Window functions (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD)

**In Progress**:
- None

**Planned Features**:
- Parallel query execution
- Advanced indexes (GiST, GIN, BRIN)
- Stored procedures and triggers
- Materialized views
- Table partitioning
