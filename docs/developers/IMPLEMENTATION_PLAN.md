# Implementation Plan

Detailed implementation plan for RustGres, organized by version milestones from the roadmap.

## Phase 1: Version 0.1.0 (Alpha) - Foundation ✅ COMPLETE

### 1.1 Storage Layer (Week 1-3) ✅
- [x] Page structure and layout (8KB pages)
- [x] Buffer pool with LRU eviction
- [~] Page I/O operations (in-memory only, disk I/O deferred to v0.2.0)
- [x] B+Tree index implementation (basic, leaf nodes only)
- [x] Heap file storage (in-memory)

**Files**: `src/storage/{page.rs, buffer_pool.rs, btree.rs, heap.rs, error.rs, mod.rs}`
**Tests**: 17 tests passing
**Status**: Complete with intentional simplifications

### 1.2 Transaction Manager (Week 4-6) ✅
- [x] Transaction ID generation
- [x] MVCC tuple visibility
- [x] Snapshot isolation
- [~] Transaction status (CLOG) (deferred to v0.2.0)
- [x] Lock manager basics

**Files**: `src/transaction/{manager.rs, mvcc.rs, snapshot.rs, lock.rs, error.rs, mod.rs}`
**Tests**: 39 tests passing
**Status**: Complete

### 1.3 WAL & Recovery (Week 7-8) ✅
- [x] WAL record format
- [x] WAL writing and flushing (in-memory buffer)
- [x] ARIES recovery (Analysis, Redo, Undo)
- [x] Checkpoint mechanism

**Files**: `src/wal/{writer.rs, recovery.rs, checkpoint.rs, error.rs, mod.rs}`
**Tests**: 57 tests passing
**Status**: Complete (WAL buffered in-memory, disk persistence in v0.2.0)

### 1.4 SQL Parser (Week 9-10) ✅
- [x] Lexer/tokenizer
- [x] Parser for SELECT, INSERT, UPDATE, DELETE
- [x] AST nodes
- [x] Basic semantic analysis
- [x] Semicolon handling
- [x] SELECT without FROM clause

**Files**: `src/parser/{lexer.rs, parser.rs, ast.rs, error.rs, mod.rs}`
**Tests**: 80 tests passing
**Status**: Complete

### 1.5 Query Execution (Week 11-12) ✅
- [x] Volcano iterator model
- [x] SeqScan operator
- [x] Filter operator
- [x] Project operator
- [x] NestedLoop join

**Files**: `src/executor/{executor.rs, seq_scan.rs, filter.rs, project.rs, nested_loop.rs, mod.rs}`
**Tests**: 91 tests passing
**Status**: Complete

### 1.6 Protocol Layer (Week 13-14) ✅
- [x] PostgreSQL wire protocol parser
- [x] Connection handling
- [x] SSL negotiation (reject with 'N')
- [~] Authentication (MD5) (no password auth, accepts all connections)
- [x] Result serialization
- [x] Error response formatting

**Files**: `src/protocol/{message.rs, connection.rs, server.rs, mod.rs}`, `src/main.rs`
**Tests**: 110 tests passing
**Status**: Complete

**v0.1.0 Summary**:
- ✅ All 6 phases complete
- ✅ 110 tests passing (62 unit + 48 integration)
- ✅ ~2,500 lines of code
- ✅ Fully functional TCP server
- ✅ In-memory only (disk I/O deferred to v0.2.0)
- ✅ PostgreSQL client compatible

**Intentional Simplifications for v0.1.0**:
- No disk I/O (all in-memory)
- No password authentication
- No SSL/TLS support
- Basic B+Tree (leaf nodes only)
- No query planner (direct execution)
- No prepared statements

---

## Phase 2: Version 0.2.0 (Alpha) - Optimization 📋 PLANNED

**Note**: Disk I/O added as first priority for data persistence.

### 2.1 Disk I/O & Persistence (Week 1-3) ✅ COMPLETE
```rust
// src/storage/disk.rs
struct DiskManager {
    data_dir: PathBuf,
    file_handles: HashMap<PageId, File>,
}

impl DiskManager {
    fn read_page(&mut self, page_id: PageId) -> Result<Page>;
    fn write_page(&mut self, page_id: PageId, page: &Page) -> Result<()>;
    fn sync(&mut self) -> Result<()>;
}

// src/wal/disk.rs
struct WALDiskWriter {
    wal_file: File,
    current_segment: u64,
}
```

**Tasks**:
- [x] Disk manager for page I/O
- [x] WAL file management
- [x] Buffer pool eviction to disk
- [x] Page replacement policy
- [x] Crash recovery from disk

**Files**: `src/storage/disk.rs, src/wal/disk.rs, src/storage/buffer_pool.rs, src/wal/writer.rs`
**Tests**: `tests/integration/{buffer_pool_disk_test.rs, wal_disk_test.rs}` (8 new tests)
**Duration**: Complete
**Status**: ✅ Integrated with buffer pool and WAL writer

### 2.2 Statistics Collection (Week 4-5) ✅ COMPLETE
```rust
// src/statistics/collector.rs
struct TableStats {
    row_count: u64,
    page_count: u64,
    avg_row_size: u32,
}

struct ColumnStats {
    n_distinct: f64,
    null_frac: f64,
    most_common_vals: Vec<Value>,
    histogram: Histogram,
}

impl Analyzer {
    fn analyze_table(&mut self, table: &Table) -> TableStats;
    fn analyze_column(&mut self, column: &Column) -> ColumnStats;
    fn build_histogram(&self, values: Vec<Value>) -> Histogram;
}
```

**Tasks**:
- [x] Implement ANALYZE command
- [x] Sample rows for statistics
- [x] Build histograms (equi-depth)
- [x] Store statistics in catalog
- [x] Auto-analyze on threshold

**Files**: `src/statistics/{collector.rs, histogram.rs, error.rs, mod.rs}`
**Tests**: `tests/integration/statistics_test.rs` (9 tests)
**Duration**: Complete
**Status**: ✅ Basic statistics collection implemented

### 2.3 Cost-Based Optimizer (Week 6-8) ✅ COMPLETE
```rust
// src/optimizer/cost.rs
struct CostModel {
    seq_page_cost: f64,
    random_page_cost: f64,
    cpu_tuple_cost: f64,
}

impl CostModel {
    fn estimate_scan(&self, table: &Table, filter: Option<&Expr>) -> Cost;
    fn estimate_index_scan(&self, index: &Index, filter: &Expr) -> Cost;
    fn estimate_join(&self, left: &Cost, right: &Cost) -> Cost;
    fn estimate_selectivity(&self, expr: &Expr, stats: &Stats) -> f64;
}
```

**Tasks**:
- [x] Cost estimation for scans
- [x] Cost estimation for joins
- [x] Selectivity estimation
- [x] Cardinality estimation
- [x] Index selection logic

**Files**: `src/optimizer/{cost.rs, selectivity.rs, error.rs, mod.rs}`
**Tests**: `tests/integration/optimizer_test.rs` (10 tests)
**Duration**: Complete
**Status**: ✅ Cost model with scan and join estimation

### 2.4 Join Ordering (Week 9-10) ✅ COMPLETE
```rust
// src/optimizer/join_order.rs
impl JoinOptimizer {
    fn optimize_dp(&self, relations: Vec<Relation>) -> LogicalPlan;
    fn optimize_greedy(&self, relations: Vec<Relation>) -> LogicalPlan;
    fn find_join_condition(&self, left: &Relation, right: &Relation) -> Expr;
}
```

**Tasks**:
- [x] Dynamic programming for ≤12 tables
- [x] Greedy algorithm for >12 tables
- [x] Join condition detection
- [x] Bushy vs left-deep trees

**Files**: `src/optimizer/join_order.rs`
**Tests**: `tests/integration/join_order_test.rs` (7 tests)
**Duration**: Complete
**Status**: ✅ DP for small joins, greedy for large joins

### 2.5 Rule-Based Optimization (Week 11-12) ✅ COMPLETE
```rust
// src/optimizer/rules.rs
trait OptimizationRule {
    fn apply(&self, plan: LogicalPlan) -> LogicalPlan;
}

struct PredicatePushdown;
struct ProjectionPruning;
struct ConstantFolding;
struct CommonSubexpressionElimination;
```

**Tasks**:
- [x] Predicate pushdown
- [x] Projection pruning
- [x] Constant folding
- [x] CSE elimination
- [x] Rule application framework

**Files**: `src/optimizer/{plan.rs, rules/mod.rs, rules/pushdown.rs, rules/pruning.rs, rules/folding.rs}`
**Tests**: `tests/integration/rules_test.rs` (8 tests)
**Duration**: Complete
**Status**: ✅ Rule framework with pushdown, pruning, folding

### 2.6 Advanced Join Algorithms (Week 13-15) ✅ COMPLETE
```rust
// src/executor/hash_join.rs
struct HashJoin {
    build_side: Box<dyn Executor>,
    probe_side: Box<dyn Executor>,
    hash_table: HashMap<Key, Vec<Tuple>>,
}

// src/executor/merge_join.rs
struct MergeJoin {
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    join_keys: Vec<Expr>,
}
```

**Tasks**:
- [x] Hash join (build + probe)
- [x] Merge join with sorted inputs
- [x] Hash aggregation
- [x] External merge sort
- [x] Spill to disk handling

**Files**: `src/executor/{hash_join.rs, sort.rs, hash_agg.rs, mock.rs}`
**Tests**: Module tests
**Duration**: Complete
**Status**: ✅ Hash join, sort, and hash aggregation implemented

### 2.7 Advanced SQL Features (Week 16-17)
```rust
// src/parser/subquery.rs
enum SubqueryType {
    Scalar,
    Exists,
    In,
    Any,
    All,
}

// src/executor/cte.rs
struct CTEExecutor {
    cte_name: String,
    cte_plan: Box<dyn Executor>,
    main_plan: Box<dyn Executor>,
}

// src/executor/window.rs
struct WindowFunction {
    func: AggFunc,
    partition_by: Vec<Expr>,
    order_by: Vec<SortExpr>,
}
```

**Tasks**:
- [ ] Subquery parsing and execution
- [ ] CTE implementation
- [ ] Window function framework
- [ ] CASE expressions

**Files**: `src/parser/subquery.rs, src/executor/{cte.rs, window.rs, case.rs}`
**Tests**: `tests/sql_advanced_test.rs`
**Duration**: 2 weeks

---

## Phase 3: Version 0.3.0 (Beta) - Parallelism 📋 PLANNED

### 3.1 Parallel Framework (Week 1-3)
```rust
// src/executor/parallel/mod.rs
struct ParallelContext {
    workers: Vec<Worker>,
    coordinator: Coordinator,
    shared_state: Arc<SharedState>,
}

struct Worker {
    id: usize,
    task_queue: WorkStealingQueue<Morsel>,
}

struct Morsel {
    start_page: PageId,
    end_page: PageId,
    tuples: Vec<Tuple>,
}
```

**Tasks**:
- [ ] Work-stealing scheduler
- [ ] Morsel-driven parallelism
- [ ] Worker thread pool
- [ ] Shared state management
- [ ] Barrier synchronization

**Files**: `src/executor/parallel/{context.rs, worker.rs, scheduler.rs}`
**Duration**: 3 weeks

### 3.2 Parallel Operators (Week 4-6)
```rust
// src/executor/parallel/scan.rs
struct ParallelSeqScan {
    table: Table,
    workers: usize,
    chunk_size: usize,
}

// src/executor/parallel/hash_join.rs
struct ParallelHashJoin {
    build_workers: usize,
    probe_workers: usize,
    partitions: usize,
}
```

**Tasks**:
- [ ] Parallel sequential scan
- [ ] Parallel hash join
- [ ] Parallel aggregation
- [ ] Parallel sort
- [ ] Exchange operator

**Files**: `src/executor/parallel/{scan.rs, join.rs, agg.rs, sort.rs}`
**Duration**: 3 weeks

### 3.3 Advanced Indexes (Week 7-10)
```rust
// src/storage/index/gist.rs
trait GiSTEntry {
    fn union(&self, other: &Self) -> Self;
    fn penalty(&self, entry: &Self) -> f64;
    fn consistent(&self, query: &Query) -> bool;
}

// src/storage/index/gin.rs
struct GINIndex {
    posting_tree: BTree<Key, PostingList>,
    pending_list: Vec<Entry>,
}

// src/storage/index/brin.rs
struct BRINIndex {
    ranges: Vec<BlockRange>,
    summaries: Vec<Summary>,
}
```

**Tasks**:
- [ ] GiST framework and R-Tree
- [ ] GIN for full-text search
- [ ] BRIN for large tables
- [ ] Hash indexes
- [ ] Partial/expression indexes

**Files**: `src/storage/index/{gist.rs, gin.rs, brin.rs, hash.rs}`
**Duration**: 4 weeks

### 3.4 Triggers & Procedures (Week 11-12)
```rust
// src/catalog/trigger.rs
struct Trigger {
    name: String,
    timing: TriggerTiming,  // BEFORE/AFTER
    events: Vec<TriggerEvent>,  // INSERT/UPDATE/DELETE
    function: FunctionId,
}

// src/plpgsql/interpreter.rs
struct PLpgSQLInterpreter {
    variables: HashMap<String, Value>,
    statements: Vec<Statement>,
}
```

**Tasks**:
- [ ] Trigger framework
- [ ] PL/pgSQL parser
- [ ] PL/pgSQL interpreter
- [ ] Views and materialized views

**Files**: `src/catalog/trigger.rs, src/plpgsql/{parser.rs, interpreter.rs}`
**Duration**: 2 weeks

---

## Phase 4: Version 0.4.0 (Beta) - Replication 📋 PLANNED

### 4.1 Streaming Replication (Week 1-4)
```rust
// src/replication/sender.rs
struct WALSender {
    standby_conn: Connection,
    start_lsn: LSN,
    current_lsn: LSN,
}

// src/replication/receiver.rs
struct WALReceiver {
    primary_conn: Connection,
    replay_lsn: LSN,
}
```

**Tasks**:
- [ ] WAL sender process
- [ ] WAL receiver process
- [ ] Replication slots
- [ ] Streaming protocol
- [ ] Async replication

**Files**: `src/replication/{sender.rs, receiver.rs, slot.rs}`
**Duration**: 4 weeks

### 4.2 Backup & Recovery (Week 5-8)
```rust
// src/backup/basebackup.rs
struct BaseBackup {
    backup_dir: PathBuf,
    start_lsn: LSN,
    end_lsn: LSN,
}

// src/backup/pitr.rs
struct PointInTimeRecovery {
    target_time: SystemTime,
    target_lsn: LSN,
}
```

**Tasks**:
- [ ] Base backup (pg_basebackup)
- [ ] WAL archiving
- [ ] Point-in-time recovery
- [ ] Incremental backups
- [ ] Backup compression

**Files**: `src/backup/{basebackup.rs, archive.rs, pitr.rs, incremental.rs}`
**Duration**: 4 weeks

### 4.3 Monitoring (Week 9-12)
```rust
// src/monitoring/metrics.rs
struct Metrics {
    queries_total: Counter,
    query_duration: Histogram,
    buffer_hit_ratio: Gauge,
    active_connections: Gauge,
}

// src/monitoring/prometheus.rs
struct PrometheusExporter {
    registry: Registry,
    endpoint: String,
}
```

**Tasks**:
- [ ] Prometheus metrics
- [ ] pg_stat_statements
- [ ] Slow query log
- [ ] Lock monitoring
- [ ] Statistics views

**Files**: `src/monitoring/{metrics.rs, prometheus.rs, stats.rs}`
**Duration**: 4 weeks

---

## Phase 5: Version 0.5.0 (Beta) - Performance (Week 1-12)

### 5.1 LSM-Tree Storage (Week 1-3)
```rust
// src/storage/lsm/memtable.rs
struct MemTable {
    data: SkipList<Key, Value>,
    size: AtomicUsize,
}

// src/storage/lsm/sstable.rs
struct SSTable {
    data_blocks: Vec<Block>,
    index_block: IndexBlock,
    bloom_filter: BloomFilter,
}

// src/storage/lsm/compaction.rs
enum CompactionStrategy {
    SizeTiered,
    Leveled,
}
```

**Tasks**:
- [ ] MemTable with skip list
- [ ] SSTable format
- [ ] Compaction strategies
- [ ] Bloom filters
- [ ] Range queries

**Files**: `src/storage/lsm/{memtable.rs, sstable.rs, compaction.rs}`
**Duration**: 3 weeks

### 5.2 Vectorized Execution (Week 4-6)
```rust
// src/executor/vectorized/batch.rs
struct RecordBatch {
    schema: Schema,
    columns: Vec<ArrayRef>,
    num_rows: usize,
}

// src/executor/vectorized/operators.rs
trait VectorizedOperator {
    fn execute(&mut self, batch: RecordBatch) -> Result<RecordBatch>;
}
```

**Tasks**:
- [ ] Arrow record batches
- [ ] Vectorized operators
- [ ] SIMD primitives
- [ ] Columnar format
- [ ] Batch processing

**Files**: `src/executor/vectorized/{batch.rs, operators.rs, simd.rs}`
**Duration**: 3 weeks

### 5.3 JIT Compilation (Week 7-9)
```rust
// src/jit/compiler.rs
struct JITCompiler {
    context: LLVMContext,
    module: Module,
}

impl JITCompiler {
    fn compile_expression(&mut self, expr: &Expr) -> CompiledExpr;
    fn compile_filter(&mut self, filter: &Expr) -> CompiledFilter;
}
```

**Tasks**:
- [ ] LLVM integration
- [ ] Expression compilation
- [ ] Filter compilation
- [ ] Code generation
- [ ] Runtime optimization

**Files**: `src/jit/{compiler.rs, codegen.rs, runtime.rs}`
**Duration**: 3 weeks

### 5.4 Advanced Data Types (Week 10-12)
```rust
// src/types/json.rs
struct JsonValue {
    data: Vec<u8>,
    offsets: Vec<u32>,
}

// src/types/array.rs
struct ArrayValue {
    element_type: DataType,
    dimensions: Vec<usize>,
    data: Vec<u8>,
}
```

**Tasks**:
- [ ] JSON/JSONB implementation
- [ ] Array operations
- [ ] Range types
- [ ] Full-text search
- [ ] Geometric types

**Files**: `src/types/{json.rs, array.rs, range.rs, fulltext.rs, geometric.rs}`
**Duration**: 3 weeks

---

## Phase 6: Version 0.6.0 (RC) - Production Ready (Week 1-12)

### 6.1 Security (Week 1-4)
```rust
// src/security/tls.rs
struct TLSConfig {
    cert_file: PathBuf,
    key_file: PathBuf,
    ca_file: Option<PathBuf>,
}

// src/security/auth.rs
enum AuthMethod {
    SCRAM_SHA_256,
    Certificate,
    LDAP,
}

// src/security/rls.rs
struct RowLevelSecurity {
    policy_name: String,
    using_expr: Expr,
    check_expr: Option<Expr>,
}
```

**Tasks**:
- [ ] TLS/SSL support
- [ ] SCRAM-SHA-256 auth
- [ ] Certificate auth
- [ ] Row-level security
- [ ] Audit logging

**Files**: `src/security/{tls.rs, auth.rs, rls.rs, audit.rs}`
**Duration**: 4 weeks

### 6.2 Administration (Week 5-8)
```rust
// src/admin/schema_change.rs
struct OnlineSchemaChange {
    table: Table,
    operation: DDLOperation,
    progress: AtomicU64,
}

// src/admin/vacuum.rs
struct ParallelVacuum {
    workers: usize,
    tables: Vec<Table>,
}
```

**Tasks**:
- [ ] Online schema changes
- [ ] Parallel vacuum
- [ ] Connection pooler
- [ ] Configuration reload
- [ ] Maintenance commands

**Files**: `src/admin/{schema_change.rs, vacuum.rs, pooler.rs, config.rs}`
**Duration**: 4 weeks

### 6.3 Testing & Benchmarking (Week 9-12)
```rust
// tests/integration/tpcc.rs
struct TPCCBenchmark {
    warehouses: usize,
    terminals: usize,
}

// tests/fuzz/parser_fuzz.rs
#[cfg(fuzzing)]
fn fuzz_parser(data: &[u8]) {
    let _ = parse_sql(data);
}
```

**Tasks**:
- [ ] TPC-C benchmark
- [ ] TPC-H benchmark
- [ ] Fuzz testing
- [ ] Compatibility tests
- [ ] Performance regression tests

**Files**: `tests/{integration/, fuzz/, compat/, perf/}`
**Duration**: 4 weeks

---

## Implementation Guidelines

### Code Organization
```
src/
├── storage/          # Storage layer
├── transaction/      # Transaction management
├── wal/             # Write-ahead logging
├── parser/          # SQL parser
├── optimizer/       # Query optimizer
├── executor/        # Query execution
├── protocol/        # Wire protocol
├── catalog/         # System catalog
├── types/           # Data types
├── replication/     # Replication
├── backup/          # Backup/recovery
├── monitoring/      # Metrics
├── security/        # Security
└── admin/           # Administration
```

### Testing Strategy
- Unit tests: 90%+ coverage
- Integration tests: All features
- Fuzz tests: Parser, optimizer
- Benchmark tests: Performance tracking
- Compatibility tests: PostgreSQL suite

### Performance Targets
- OLTP: 100K+ TPS (TPC-C)
- OLAP: 10x faster than PostgreSQL (TPC-H)
- Latency: <1ms P99 for point queries
- Memory: 50% less than PostgreSQL
- Startup: <100ms cold start

### Dependencies
```toml
[dependencies]
tokio = "1.35"           # Async runtime
serde = "1.0"            # Serialization
thiserror = "1.0"        # Error handling
tracing = "0.1"          # Logging
dashmap = "5.5"          # Concurrent maps
parking_lot = "0.12"     # Fast locks
crossbeam = "0.8"        # Concurrency
arrow = "50.0"           # Columnar format
sqlparser = "0.43"       # SQL parsing (optional)
```

### Milestones
- **v0.2.0**: Q3 2024 (3 months)
- **v0.3.0**: Q4 2024 (3 months)
- **v0.4.0**: Q1 2025 (3 months)
- **v0.5.0**: Q2 2025 (3 months)
- **v0.6.0**: Q3 2025 (3 months)
- **v1.0.0**: Q4 2025 (3 months)

### Team Structure
- **Core Team**: 3-5 developers
- **Contributors**: 20-50 active
- **Reviewers**: 2-3 maintainers
- **Documentation**: 1-2 writers

### Success Metrics
- Code coverage: >90%
- Build time: <5 minutes
- Test time: <10 minutes
- Documentation: 100% public APIs
- Performance: Meet targets above
