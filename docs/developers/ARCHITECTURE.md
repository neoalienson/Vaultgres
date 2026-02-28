# Architecture Overview

RustGres is designed as a modular, layered database system with clear separation of concerns and modern Rust idioms throughout.

## System Layers

### 1. Protocol Layer
**Responsibility**: Client communication and session management

**Components**:
- `PostgresProtocol`: Wire protocol parser/serializer
- `ConnectionManager`: Connection pooling and lifecycle
- `AuthManager`: SCRAM-SHA-256, MD5, certificate authentication
- `SessionState`: Per-connection state and temporary objects

**Key Design**:
- Async I/O with Tokio for high concurrency
- Zero-copy parsing where possible
- Streaming result sets to minimize memory

### 2. SQL Layer
**Responsibility**: Parse and validate SQL statements

**Components**:
- `Lexer`: Tokenization with keyword recognition
- `Parser`: Recursive descent parser producing AST
- `Analyzer`: Semantic analysis, type checking, name resolution
- `Validator`: Constraint validation and permission checks

**Key Design**:
- Hand-written parser for PostgreSQL dialect
- Rich error messages with source location
- AST nodes implement `Clone` for optimizer transformations

### 3. Optimizer Layer
**Responsibility**: Transform logical plans into efficient physical plans

**Components**:
- `LogicalPlanner`: Convert AST to logical plan
- `RuleOptimizer`: Apply transformation rules (predicate pushdown, projection pruning)
- `CostOptimizer`: Cost-based join ordering and access path selection
- `PhysicalPlanner`: Generate executable physical plan
- `Statistics`: Table/column statistics and histograms

**Key Design**:
- Volcano-style optimizer with memo structure
- Dynamic programming for join enumeration
- Cardinality estimation with histograms
- Adaptive optimization based on runtime statistics

### 4. Execution Layer
**Responsibility**: Execute physical plans and return results

**Components**:
- `Executor`: Volcano-style iterator model
- `VectorizedExecutor`: Batch-oriented execution with SIMD
- `ParallelExecutor`: Work-stealing parallel execution
- `ExpressionEvaluator`: JIT compilation for expressions
- `Operators`: Scan, Join, Aggregate, Sort, etc.

**Key Design**:
- Vectorized execution on Arrow record batches
- Morsel-driven parallelism for intra-query parallelism
- Code generation for hot paths
- Adaptive execution with runtime re-optimization

### 5. Transaction Layer
**Responsibility**: ACID guarantees and concurrency control

**Components**:
- `TransactionManager`: Transaction lifecycle and coordination
- `MVCCManager`: Multi-version concurrency control
- `LockManager`: Deadlock detection and resolution
- `SnapshotManager`: Snapshot isolation implementation
- `TwoPhaseCommit`: Distributed transaction support

**Key Design**:
- MVCC with timestamp-based ordering
- Optimistic concurrency control (OCC)
- Serializable snapshot isolation (SSI)
- Lock-free read operations
- Garbage collection for old versions

### 6. Storage Layer
**Responsibility**: Persistent data storage and retrieval

**Components**:
- `BufferPool`: Page cache with LRU/Clock eviction
- `PageManager`: Page allocation and layout
- `BTreeIndex`: B+Tree implementation for primary/secondary indexes
- `LSMTree`: Log-structured merge tree for write-heavy workloads
- `WAL`: Write-ahead logging for durability
- `Recovery`: Crash recovery with ARIES protocol

**Key Design**:
- Pluggable storage engines
- Copy-on-write B+Trees for MVCC
- Asynchronous I/O with io_uring (Linux)
- Direct I/O for WAL
- Incremental checkpointing

## Data Flow

### Query Execution Flow

```
Client Request
    ↓
[Protocol Layer] Parse wire protocol message
    ↓
[SQL Layer] Tokenize → Parse → Analyze
    ↓
[Optimizer] Logical Plan → Optimize → Physical Plan
    ↓
[Transaction] Begin/Get Snapshot
    ↓
[Execution] Execute operators → Fetch data
    ↓
[Storage] Read pages → Apply MVCC visibility
    ↓
[Execution] Return results
    ↓
[Protocol Layer] Serialize response
    ↓
Client Response
```

### Write Path

```
INSERT/UPDATE/DELETE
    ↓
[Transaction] Acquire write lock
    ↓
[Execution] Evaluate expressions
    ↓
[Storage] Allocate new page/slot
    ↓
[WAL] Write log record (fsync)
    ↓
[Storage] Write data page (async)
    ↓
[Transaction] Commit (update visibility)
    ↓
Response to client
```

## Concurrency Model

### MVCC Implementation

**Tuple Structure**:
```rust
struct Tuple {
    xmin: TransactionId,  // Creating transaction
    xmax: TransactionId,  // Deleting transaction (0 if active)
    cmin: CommandId,      // Creating command within transaction
    cmax: CommandId,      // Deleting command within transaction
    data: Vec<u8>,        // Actual tuple data
}
```

**Visibility Rules**:
- Tuple visible if `xmin` committed before snapshot and `xmax` not committed
- Handles in-progress transactions via transaction status cache
- Vacuum removes tuples invisible to all active transactions

**Snapshot Isolation**:
```rust
struct Snapshot {
    xmin: TransactionId,           // Oldest active transaction
    xmax: TransactionId,           // Next transaction ID
    active: Vec<TransactionId>,    // In-progress transactions
}
```

### Lock-Free Structures

**Concurrent B+Tree**:
- Optimistic lock coupling for traversal
- Copy-on-write for modifications
- Epoch-based memory reclamation

**Lock Manager**:
- Lock-free hash table for lock tracking
- Wait-die deadlock prevention
- Timeout-based deadlock detection

## Memory Management

### Buffer Pool

**Design**:
- Fixed-size pages (8KB default)
- LRU-K eviction policy (K=2)
- Pin/unpin reference counting
- Dirty page tracking for checkpointing

**Optimization**:
- NUMA-aware allocation
- Huge pages support
- Prefetching for sequential scans
- Adaptive replacement cache (ARC)

### Memory Contexts

**Hierarchy**:
```
TopMemoryContext (process lifetime)
├── CacheMemoryContext (catalog cache)
├── TransactionContext (per transaction)
│   └── ExecutorContext (per query)
│       └── ExpressionContext (per expression)
└── ErrorContext (error handling)
```

**Benefits**:
- Bulk deallocation on context drop
- Memory leak prevention
- Clear ownership semantics

## Storage Format

### Page Layout

```
┌─────────────────────────────────────────┐
│ PageHeader (24 bytes)                   │
├─────────────────────────────────────────┤
│ ItemId Array (4 bytes each)             │
├─────────────────────────────────────────┤
│ Free Space                               │
├─────────────────────────────────────────┤
│ Tuples (bottom-up)                       │
└─────────────────────────────────────────┘
```

**PageHeader**:
```rust
struct PageHeader {
    lsn: LSN,              // Last WAL record
    checksum: u32,         // Page checksum
    flags: u16,            // Page flags
    lower: u16,            // End of item array
    upper: u16,            // Start of free space
    special: u16,          // Special space offset
}
```

### Index Structure

**B+Tree Node**:
```
Internal Node:
┌──────────────────────────────────────┐
│ [Key1|Ptr1] [Key2|Ptr2] ... [KeyN]  │
└──────────────────────────────────────┘

Leaf Node:
┌──────────────────────────────────────┐
│ [Key1|TID1] [Key2|TID2] ... [Next]  │
└──────────────────────────────────────┘
```

**Features**:
- Prefix compression for keys
- Suffix truncation in internal nodes
- Fast path for unique indexes
- Deduplication for non-unique indexes

## Write-Ahead Logging

### WAL Record Format

```rust
struct WALRecord {
    header: WALHeader,
    data: Vec<u8>,
    crc: u32,
}

struct WALHeader {
    xid: TransactionId,
    prev_lsn: LSN,
    record_type: RecordType,
    length: u32,
}
```

### Record Types

- `INSERT`: Full tuple data
- `UPDATE`: Old/new tuple or delta
- `DELETE`: Tuple identifier
- `COMMIT`: Transaction commit
- `CHECKPOINT`: Checkpoint record
- `FULL_PAGE`: Full page image after checkpoint

### Recovery Process

**ARIES Protocol**:
1. **Analysis**: Scan WAL to identify dirty pages and active transactions
2. **Redo**: Replay all operations from last checkpoint
3. **Undo**: Roll back uncommitted transactions

## Query Optimization

### Cost Model

**Cost Components**:
```rust
struct Cost {
    startup_cost: f64,    // Cost before first tuple
    total_cost: f64,      // Cost for all tuples
    rows: f64,            // Estimated rows
}
```

**Cost Factors**:
- `seq_page_cost`: Sequential page read (1.0)
- `random_page_cost`: Random page read (4.0 HDD, 1.1 SSD)
- `cpu_tuple_cost`: Process one tuple (0.01)
- `cpu_operator_cost`: Execute one operator (0.0025)

### Join Algorithms

**Nested Loop**:
- Best for small inner relation
- Supports all join types
- O(N*M) complexity

**Hash Join**:
- Best for equi-joins
- Build hash table on smaller relation
- O(N+M) complexity

**Merge Join**:
- Requires sorted inputs
- Best for large sorted relations
- O(N+M) complexity

**Selection**:
- Cost-based selection using cardinality estimates
- Considers available indexes
- Adaptive join based on runtime statistics

## Parallelism

### Parallel Query Execution

**Morsel-Driven Parallelism**:
- Split data into morsels (1000-10000 rows)
- Work-stealing scheduler
- Pipeline parallelism between operators
- Adaptive degree of parallelism

**Parallel Operators**:
- Parallel Sequential Scan
- Parallel Hash Join
- Parallel Aggregation
- Parallel Sort

**Coordination**:
```rust
struct ParallelContext {
    workers: Vec<Worker>,
    coordinator: Coordinator,
    shared_state: Arc<SharedState>,
}
```

## Performance Characteristics

### Time Complexity

| Operation | Best | Average | Worst |
|-----------|------|---------|-------|
| Point Query (indexed) | O(log n) | O(log n) | O(log n) |
| Range Scan | O(log n + k) | O(log n + k) | O(n) |
| Insert | O(log n) | O(log n) | O(n) |
| Update | O(log n) | O(log n) | O(n) |
| Delete | O(log n) | O(log n) | O(n) |
| Join (hash) | O(n + m) | O(n + m) | O(n*m) |
| Sort | O(n log n) | O(n log n) | O(n log n) |

### Space Complexity

- **Buffer Pool**: Configurable (default 25% of RAM)
- **WAL**: Configurable (default 1GB)
- **Indexes**: ~1.5x table size for B+Tree
- **MVCC Overhead**: ~20% for active workloads

## Scalability

### Vertical Scaling
- Linear scaling up to 64 cores
- NUMA-aware memory allocation
- Lock-free data structures minimize contention

### Horizontal Scaling (Future)
- Shared-nothing architecture
- Range-based sharding
- Distributed transactions with 2PC
- Consensus-based replication (Raft)

## Reliability

### Durability Guarantees
- WAL with fsync before commit
- Checksums on all pages
- ARIES recovery protocol
- Point-in-time recovery

### High Availability
- Streaming replication
- Automatic failover
- Read replicas
- Backup and restore

## Monitoring

### Metrics Exposed
- Query latency (p50, p95, p99)
- Throughput (queries/sec)
- Buffer pool hit ratio
- WAL write rate
- Transaction commit rate
- Lock wait time
- Index usage statistics

### Integration
- Prometheus exporter
- OpenTelemetry tracing
- Structured logging (JSON)
- Query explain plans
