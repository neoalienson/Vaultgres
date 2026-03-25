# Roadmap

VaultGres development roadmap with planned features and milestones.

## Completed Features ✅

**Core Database Engine**
- ✅ Page-based storage with buffer pool
- ✅ B+Tree indexes with concurrent access
- ✅ MVCC transaction manager with snapshot isolation
- ✅ WAL and crash recovery (ARIES protocol)
- ✅ PostgreSQL wire protocol with result set serialization
- ✅ Comprehensive SQL parser (DDL, DML, queries)
- ✅ Cost-based query optimizer with statistics
- ✅ Volcano-style execution engine

**Query Optimization**
- ✅ Cost-based optimizer with histogram statistics
- ✅ Join ordering optimization (dynamic programming)
- ✅ Predicate pushdown and projection pruning
- ✅ Selectivity estimation with histograms
- ✅ ANALYZE command for statistics collection
- ✅ Index selection optimization with cost-based selection

**Execution Engine**
- ✅ Sequential scan with predicate evaluation
- ✅ Index scan with B+Tree
- ✅ Nested loop join
- ✅ Hash join implementation
- ✅ Merge join with sorted inputs
- ✅ Hash aggregation with GROUP BY
- ✅ Sort operator with external merge sort
- ✅ Filter and projection operators
- ✅ LIMIT and OFFSET operators

**SQL Features**
- ✅ DDL: CREATE TABLE, DROP TABLE, CREATE INDEX
- ✅ DML: INSERT, UPDATE, DELETE
- ✅ Queries: SELECT with WHERE, ORDER BY, GROUP BY, HAVING
- ✅ Subqueries: Scalar and IN subqueries with caching
- ✅ Correlated subqueries: EXISTS, NOT EXISTS, IN, NOT IN, Scalar
- ✅ Common Table Expressions (CTEs) with WITH clause
- ✅ Window functions: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD
- ✅ Set operations: UNION, UNION ALL, INTERSECT, EXCEPT
- ✅ All JOIN types: INNER, LEFT, RIGHT, FULL OUTER
- ✅ Aggregation: COUNT, SUM, AVG, MIN, MAX with DISTINCT
- ✅ CASE expressions
- ✅ PRIMARY KEY and FOREIGN KEY constraints
- ✅ CHECK constraints with expression validation
- ✅ UNIQUE constraints (column and table-level)
- ✅ DEFAULT values for columns
- ✅ AUTO_INCREMENT/SERIAL columns
- ✅ NOT NULL enforcement
- ✅ Foreign key actions: ON DELETE/ON UPDATE (CASCADE, SET NULL, RESTRICT)

**Transaction Management**
- ✅ MVCC with snapshot isolation
- ✅ BEGIN, COMMIT, ROLLBACK
- ✅ Multi-statement transactions with full ACID support
- ✅ Savepoints: SAVEPOINT, ROLLBACK TO, RELEASE SAVEPOINT
- ✅ Transaction isolation levels: READ COMMITTED, REPEATABLE READ, SERIALIZABLE
- ✅ Deadlock detection and resolution with wait-for graph
- ✅ Lock timeout configuration with customizable duration

**Testing Infrastructure**
- ✅ Comprehensive tests (100% pass rate)
- ✅ Unit tests across all modules
- ✅ Integration tests
- ✅ Docker-based E2E testing framework
- ✅ Performance comparison tests (VaultGres vs PostgreSQL)
- ✅ Load and soak testing infrastructure
- ✅ Monitoring stack (Prometheus, Grafana, cAdvisor)

**Development Tools**
- ✅ Pre-commit hooks (secret scanning, formatting, linting)
- ✅ CI/CD pipeline with automated testing
- ✅ Code coverage tracking
- ✅ Comprehensive documentation

## Executor Migration Status

**Phase 0: Preparation & Infrastructure** ✅ COMPLETED
- ✅ Adapter utilities for type conversion (SimpleTuple ↔ Tuple, bytes ↔ Value)
- ✅ Enhanced test infrastructure with MockExecutor and TupleBuilder
- ✅ Feature flag infrastructure (executor-migration)

**Phase 1: Leaf Node Executors** ✅ COMPLETED
- ✅ SeqScanExecutor (volcano module)
- ✅ FilterExecutor (operators module)
- ✅ ProjectExecutor (operators module)
- ✅ TableFunctionExecutor with generate_series() and unnest() (volcano module)

**Phase 2: Transform Executors** ✅ COMPLETED
- ✅ SortExecutor (volcano module)
- ✅ LimitExecutor (volcano module)
- ✅ DistinctExecutor (volcano module)
- ✅ HashAggExecutor (volcano module)
- ✅ HavingExecutor (volcano module)

**Phase 3: Join Executors** ✅ COMPLETED
- ✅ NestedLoopJoinExecutor (volcano module)
- ✅ HashJoinExecutor (volcano module)
- ✅ MergeJoinExecutor (volcano module)
- ✅ JoinExecutor with JoinType enum (volcano module)

**Phase 4: Set Operation Executors** ✅ COMPLETED
- ✅ UnionExecutor with UnionType enum (volcano module)
- ✅ IntersectExecutor (volcano module)
- ✅ ExceptExecutor (volcano module)

**Phase 5: Advanced Executors** ⚠️ PARTIALLY COMPLETED
- ✅ AggregateExecutor (volcano module)
- ✅ SubqueryExecutor (volcano module)
- ✅ CaseExecutor (volcano module)
- ✅ GroupByExecutor (volcano module - sort-based grouping, alternative to HashAggExecutor)
- ✅ CTEExecutor with WITH/WITH RECURSIVE support (volcano module, planner integration)
- ✅ WindowExecutor (volcano module - supports ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTILE, NTH_VALUE, PERCENT_RANK, CUME_DIST)

**Phase 6: Parallel Execution Infrastructure** ✅ COMPLETED
- ✅ Work-stealing scheduler migrated to Tuple format
- ✅ Worker pool migrated to Tuple format
- ✅ Morsel type migrated to Tuple format
- ✅ ParallelCoordinator migrated to Tuple format
- ✅ ParallelSeqScan migrated to Tuple format
- ✅ ParallelSort migrated to Tuple format
- ✅ ParallelHashJoin (infrastructure complete, join logic complete with Tuple key extraction, all join types supported)
- ✅ ParallelHashAgg (infrastructure complete, aggregation logic complete with Tuple-based grouping, all aggregate functions supported)


## Version 0.2.0 (Current - Alpha)

**Completed**
- ✅ Query plan caching with LRU eviction
- ✅ CHECK constraints with expression validation
- ✅ UNIQUE constraints with column and table-level support
- ✅ DEFAULT values for columns
- ✅ AUTO_INCREMENT/SERIAL for auto-incrementing integer columns
- ✅ Transactions: BEGIN, COMMIT, ROLLBACK
- ✅ Savepoints: SAVEPOINT, ROLLBACK TO, RELEASE SAVEPOINT
- ✅ Multi-statement transactions with full ACID support
- ✅ Transaction isolation levels: READ COMMITTED, REPEATABLE READ, SERIALIZABLE
- ✅ Deadlock detection and resolution
- ✅ Lock timeout configuration
- ✅ NOT NULL enforcement
- ✅ Foreign key actions: ON DELETE/ON UPDATE (CASCADE, SET NULL, RESTRICT)
- ✅ Recursive CTEs (WITH RECURSIVE)
- ✅ Lateral joins (LATERAL)
- ✅ Table aliases and column aliases
- ✅ Prepared statements
- ✅ Bind parameter support


## Version 0.3.0 (Beta)

**Advanced SQL**
- ✅ Recursive CTEs (WITH RECURSIVE)
- ✅ Lateral joins (LATERAL)
- ✅ Table aliases and column aliases
- ✅ Qualified column references (table.column syntax)
- ✅ JOIN execution integration
  - ✅ Hash join, merge join, nested loop join executors implemented
  - ✅ Protocol layer integration implemented for JOIN queries
  - ✅ Multi-table schema resolution for qualified columns in SELECT
  - ✅ Parser support for table aliases in FROM clause (with and without AS)
  - ✅ Parser support for JOIN clauses (INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN)

**Query Optimization**
- ✅ Prepared statements
- ✅ Bind parameter support


## Version 0.4.0 (Beta)

**Parallel Execution**
- ✅ Parallel sequential scan with worker threads
- ✅ Parallel hash join (all join types supported)
- ✅ Parallel aggregation (all aggregate functions supported)
- ✅ Parallel sort with merge (heap-based k-way merge, multi-phase cascading merge, LIMIT optimization)
- ✅ Work-stealing scheduler
- ✅ Configurable parallelism (max_parallel_workers)

**Advanced Indexes**
- ✅ Hash indexes for equality lookups
- ✅ Partial indexes with WHERE clause
- ✅ Expression indexes (functional indexes)
- ✅ Multi-column indexes
- ✅ Index-only scans
- ✅ Covering indexes

**SQL Features**
- ✅ Views (CREATE VIEW, DROP VIEW)
- ✅ Materialized views with REFRESH
- ✅ User-defined functions (SQL functions) - supports scalar and set-returning functions with parameter binding
- ✅ Aggregate functions (custom aggregates) - CREATE AGGREGATE and DROP AGGREGATE parser, catalog storage, protocol handling, and executor integration (sfunc/finalfunc) implemented
- ✅ Table partitioning (RANGE, LIST, HASH)
  - ✅ `CREATE TABLE ... PARTITION BY {RANGE|LIST|HASH} (columns)`
  - ✅ `CREATE TABLE name PARTITION OF parent FOR VALUES ...`
  - ✅ `ALTER TABLE ... ATTACH PARTITION ...`
  - ✅ `ALTER TABLE ... DETACH PARTITION ...`
  - ✅ Partition bound specifications (FROM/TO, IN, WITH (MODULUS/REMAINDER))
  - ✅ Default partition support
- ✅ String functions (CONCAT, SUBSTRING, UPPER, LOWER)
- ✅ Date/time functions (NOW, DATE_TRUNC, EXTRACT)
- ✅ Subquery with AVG aggregate function


**Data Types**
- ✅ BOOLEAN type
- ✅ DATE, TIME, TIMESTAMP types
- ✅ DECIMAL/NUMERIC with precision
- ✅ VARCHAR with length limits
- ✅ TEXT type
- ✅ BLOB/BYTEA for binary data

## Version 0.5.0 (Beta)

**Replication & High Availability**
- Streaming replication (async)
- Logical replication with publications/subscriptions
- Replication slots
- Automatic failover with health checks
- Read replicas with load balancing
- Cascading replication

**Backup & Recovery**
- Online backups (pg_basebackup compatible)
- Point-in-time recovery (PITR)
- Incremental backups
- Backup compression (gzip, zstd)
- Backup verification
- Restore testing automation

**Monitoring & Observability**
- ✅ Prometheus metrics exporter
- ✅ Query statistics (pg_stat_statements compatible)
- ✅ Slow query log with configurable threshold
- ✅ Lock monitoring and wait events
- ✅ Buffer pool statistics
- ✅ Connection pool metrics
- ✅ Disk I/O statistics

## Version 0.6.0 (Beta)

**Storage Enhancements**
- ✅ Table partitioning (range, hash, list)
  - ✅ Parser support for `CREATE TABLE ... PARTITION BY {RANGE|LIST|HASH}`
  - ✅ Parser support for `CREATE TABLE ... PARTITION OF parent`
  - ✅ Parser support for `ALTER TABLE ... ATTACH/DETACH PARTITION`
  - ✅ AST types: `PartitionMethod`, `PartitionKey`, `PartitionBoundSpec`, `PartitionDef`
  - ✅ Lexer tokens: `RANGE`, `LIST`, `HASH`, `MODULUS`, `REMAINDER`, `ATTACH`, `DETACH`
  - ✅ Catalog methods: `create_partitioned_table`, `create_partition`, `attach_partition`, `detach_partition`
  - ✅ Persistence layer: `save_partitions` and `load_partitions` (JSON-based)
  - ✅ Unit tests (12 parser tests)
  - ✅ Integration tests (15 catalog tests)
- 🚧 Partition pruning in query optimizer
- Compression (LZ4, Zstd) for tables and indexes
- TOAST (The Oversized-Attribute Storage Technique)
- Vacuum improvements (parallel vacuum)
- Autovacuum with configurable thresholds
- Dead tuple cleanup optimization

**Performance Optimizations**
- Vectorized execution with SIMD
- JIT compilation for expressions (LLVM)
- Adaptive query execution with runtime statistics
- Query result caching
- Prepared statement caching
- Connection pooling (built-in PgBouncer-like)
- ✅ Batch insert optimization

**Advanced SQL**
- Full-text search with tsvector/tsquery
- ✅ JSON/JSONB types with operators
  - ✅ DataType::Json and DataType::Jsonb
  - ✅ JSON operators: ->, ->>, #>, #>>, ? (exists), ?| (exists any), ?& (exists all)
  - ✅ INSERT validation and type checking
  - ✅ Integration with catalog and persistence layers
  - ✅ Unit tests (16 tests) and integration tests (22 tests)
- ✅ Array types and operations
  - ✅ DataType::Array with nested type support
  - ✅ Value::Array implementation
  - ✅ Array literals with Expr::Array
  - ✅ Array operators: @>, <@, &&, ||, [] (indexing)
  - ✅ unnest() table function
  - ✅ ArraySubqueryExecutor for subquery execution
  - ✅ Unit tests and integration tests
- ✅ Range types (int4range, int8range, numrange, daterange, tsrange, tstzrange)
  - ✅ DataType::Int4Range, Int8Range, NumRange, DateRange, TsRange, TsTzRange
  - ✅ Value::Range implementation with RangeBound struct
  - ✅ Range literals with Expr::Range
  - ✅ Range operators: @> (contains), <@ (contained by), && (overlaps), << (left of), >> (right of), -|- (adjacent)
  - ✅ Range evaluation in executor
  - ✅ Display trait for Range
  - ✅ Hash trait for Range
  - ✅ Unit tests (19 tests in eval.rs, 25 tests in datatype_tests.rs)
  - ✅ Integration tests and E2E tests
- ✅ Composite types (user-defined types)
  - ✅ DataType::Composite(String) in AST
  - ✅ CompositeTypeDef struct with fields
  - ✅ Value::Composite(CompositeValue) for runtime values
  - ✅ CREATE TYPE ... AS (col1 type1, col2 type2, ...) parsing
  - ✅ DROP TYPE ... [CASCADE | RESTRICT]
  - ✅ Composite type registry in Catalog
  - ✅ Composite value validation on INSERT (ROW(...) syntax)
  - ✅ Nested composite types (composite containing another composite)
  - ✅ Composite types with enum fields
  - ✅ Unit tests (19 tests) and integration tests (14 tests)
  - ✅ E2E tests (8 tests in scenarios/composite_tests.rs)
- ✅ Enum types
  - ✅ DataType::Enum(String) in AST
  - ✅ CREATE TYPE ... AS ENUM parsing
  - ✅ ALTER TYPE ... ADD VALUE parsing
  - ✅ DROP TYPE ... [CASCADE | RESTRICT]
  - ✅ Enum type registry in Catalog
  - ✅ Enum value validation on INSERT
  - ✅ Unit tests (27 tests) and integration tests (18 tests)

## Version 0.7.0 (RC)

**Security**
- TLS/SSL support with certificate validation
- SCRAM-SHA-256 authentication
- Certificate-based authentication
- Row-level security (RLS) policies
- Column-level permissions
- Audit logging with configurable events
- Password policies and expiration
- Role-based access control (RBAC)

**Administration**
- Online schema changes (ALTER TABLE without locks)
- Configuration hot reload
- Dynamic memory allocation
- Tablespace management
- Database templates
- pg_dump/pg_restore compatibility
- Migration tools from PostgreSQL

**Compatibility**
- PostgreSQL 16 wire protocol compatibility
- Foreign data wrappers (FDW) framework
- Extensions API with dynamic loading
- System catalog compatibility
- Information schema views


## Version 1.0.0 (Stable)

**Production Readiness**
- ✅ Comprehensive unit testing (553 tests)
- ✅ Edge case testing (79 tests)
- ✅ Docker-based E2E testing
- 🚧 Fuzz testing (parser, optimizer, executor)
- 🚧 Performance benchmarks (TPC-C, TPC-H, TPC-DS)
- 🚧 Stress testing (1M+ QPS sustained)
- 🚧 Chaos engineering tests
- 🚧 Complete documentation suite
- 🚧 Migration tools from PostgreSQL
- 🚧 Production deployment guide
- 🚧 Disaster recovery procedures

**Stability & Reliability**
- ✅ Edge case handling across all modules
- 🚧 Memory leak detection and fixes (Valgrind, ASAN)
- 🚧 Performance regression testing
- 🚧 Long-running stability tests (7+ days)
- 🚧 Resource leak detection
- 🚧 Crash recovery testing
- 🚧 Data corruption detection

**Ecosystem & Tooling**
- 🚧 Client libraries (Rust, Python, Node.js, Go, Java)
- 🚧 ORM support (SQLAlchemy, Diesel, TypeORM)
- 🚧 pgAdmin compatibility
- 🚧 DBeaver support
- 🚧 Grafana dashboards
- 🚧 Kubernetes operator
- 🚧 Docker Compose templates
- 🚧 Terraform modules
- 🚧 Ansible playbooks

**Documentation**
- 🚧 Complete user guide
- 🚧 Administrator handbook
- 🚧 SQL reference manual
- 🚧 Performance tuning guide
- 🚧 Troubleshooting guide
- 🚧 Migration guide from PostgreSQL
- 🚧 Internals documentation
- 🚧 API documentation


## Version 1.1.0 (Post-Stable)

**Advanced Indexes**
- GiST (Generalized Search Tree) for spatial data
- GIN (Generalized Inverted Index) for full-text search
- BRIN (Block Range Index) for large tables
- Bloom filters for multi-column queries
- Adaptive radix tree (ART) indexes

**Triggers & Procedures**
- Triggers (BEFORE/AFTER, FOR EACH ROW/STATEMENT)
- Stored procedures (PL/pgSQL)
- Event triggers
- Trigger cascading
- Deferred constraint checking

**Advanced Features**
- Geometric types (point, line, polygon)
- Network address types (inet, cidr, macaddr)
- UUID generation functions
- XML type and functions
- HStore (key-value store)


## Version 1.2.0+ (Future) 🔮

**Distributed Database**
- Horizontal sharding with automatic routing
- Distributed transactions (2PC, Raft consensus)
- Cross-shard queries with distributed execution
- Automatic shard rebalancing
- Multi-region deployment with geo-replication
- Conflict resolution strategies
- Global secondary indexes

**Columnar Storage & OLAP**
- Columnar storage engine for analytics
- Vectorized execution with Apache Arrow
- Approximate query processing (sampling, sketches)
- Materialized view auto-refresh
- Time-series optimizations (compression, retention)
- Window function optimizations
- Parallel aggregation with SIMD

**Cloud Native Features**
- Kubernetes operator with CRDs
- Auto-scaling based on load
- Serverless mode (scale-to-zero)
- Multi-tenancy with resource isolation
- Cloud storage integration (S3, GCS, Azure Blob)
- Separation of compute and storage
- Snapshot-based backups to object storage

**Machine Learning Integration**
- SQL/ML for in-database ML
- Model training and inference
- Feature engineering functions
- Integration with TensorFlow/PyTorch
- Automated feature selection
- Model versioning and deployment

**Advanced Performance**
- NUMA-aware memory allocation
- GPU acceleration for analytics (CUDA)
- Persistent memory (PMEM) support
- Zero-copy networking (io_uring)
- RDMA support for replication
- Intelligent prefetching
- Adaptive indexing

## Compatibility Promise

**Before 1.0**:
- No compatibility guarantees
- Breaking changes allowed
- Migration guides provided

**After 1.0**:
- Wire protocol compatibility maintained
- SQL syntax backward compatible
- Storage format migrations supported
- Deprecation warnings before removal

## Performance Goals

**Version 1.0 Targets** (vs PostgreSQL 16):
- 2-3x faster OLTP throughput (TPC-C)
- 1.5-2x faster OLAP queries (TPC-H)
- 50% lower memory usage
- 30% lower CPU usage
- Sub-millisecond P99 latency for point queries


## Documentation Goals

**User Documentation**:
- Getting started guide
- SQL reference
- Administration guide
- Performance tuning guide
- Migration guide

**Developer Documentation**:
- Architecture overview
- Component design docs
- API documentation
- Contributing guide
- Internals guide

## Long-Term Vision

VaultGres aims to be:
1. **Fastest**: Best-in-class performance for mixed workloads
2. **Safest**: Memory-safe with zero crashes
3. **Simplest**: Easy to deploy and operate
4. **Compatible**: Drop-in PostgreSQL replacement
5. **Modern**: Cloud-native, distributed, scalable

## Research Areas

**Active Research**:
- Learned indexes (ML-based index structures)
- Adaptive execution (runtime plan adjustment)
- Approximate query processing (sampling, sketches)
- Hardware acceleration (GPU, FPGA)
- Persistent memory integration

**Experimental Features**:
- Automatic index recommendation
- Query optimization hints
- Workload-aware tuning
- Predictive prefetching
- Intelligent caching
