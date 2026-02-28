# Storage Engine

The RustGres storage engine provides durable, efficient data storage with MVCC support, crash recovery, and pluggable storage backends.

## Overview

**Key Features**:
- Page-based storage with 8KB pages
- Buffer pool with intelligent caching
- B+Tree and LSM-Tree storage engines
- Write-ahead logging (WAL)
- MVCC with garbage collection
- Online backup and PITR

## Buffer Pool

### Design

The buffer pool is a fixed-size cache of database pages in memory.

```rust
pub struct BufferPool {
    pages: Vec<Arc<RwLock<Page>>>,
    page_table: DashMap<PageId, FrameId>,
    replacer: ClockReplacer,
    free_list: Mutex<Vec<FrameId>>,
}
```

**Components**:
- `pages`: Array of page frames
- `page_table`: Maps page IDs to frame IDs
- `replacer`: Eviction policy (Clock/LRU-K)
- `free_list`: Available frames

### Page Lifecycle

```
Request Page
    ↓
In buffer pool? ──Yes──→ Pin page → Return
    ↓ No
    ↓
Free frame available? ──Yes──→ Load page → Pin → Return
    ↓ No
    ↓
Evict page (if dirty, flush) → Load page → Pin → Return
```

### Eviction Policies

**Clock (Second Chance)**:
- Single reference bit per page
- O(1) eviction time
- Good for uniform access patterns

**LRU-K (K=2)**:
- Tracks last K access times
- Resists sequential scan pollution
- Better for mixed workloads

**Configuration**:
```ini
shared_buffers = 256MB        # Buffer pool size
effective_cache_size = 4GB    # OS + buffer pool
```

## Page Format

### Page Structure

```
┌─────────────────────────────────────────┐ 0
│ PageHeader (24 bytes)                   │
├─────────────────────────────────────────┤ 24
│ ItemId Array (4 bytes each)             │
│   [offset, length, flags]               │
├─────────────────────────────────────────┤ lower
│                                          │
│         Free Space                       │
│                                          │
├─────────────────────────────────────────┤ upper
│ Tuples (growing upward)                  │
│   [Header | Data]                        │
├─────────────────────────────────────────┤ special
│ Special Space (index-specific)           │
└─────────────────────────────────────────┘ 8192
```

### PageHeader

```rust
#[repr(C)]
pub struct PageHeader {
    pub lsn: LSN,              // Last WAL record (8 bytes)
    pub checksum: u32,         // CRC32 checksum
    pub flags: u16,            // Page flags
    pub lower: u16,            // End of item array
    pub upper: u16,            // Start of free space
    pub special: u16,          // Special space offset
    pub page_size: u16,        // Page size
    pub version: u16,          // Page format version
}
```

**Flags**:
- `HAS_FREE_LINES`: Has deleted items
- `ALL_VISIBLE`: All tuples visible to all
- `FULL`: No free space

### ItemId

```rust
#[repr(C)]
pub struct ItemId {
    pub offset: u16,    // Offset to tuple
    pub length: u16,    // Tuple length
    pub flags: u8,      // Item flags
}
```

**Flags**:
- `USED`: Item is in use
- `DEAD`: Item is deleted
- `REDIRECT`: Points to another item

## Tuple Format

### Heap Tuple

```rust
#[repr(C)]
pub struct HeapTupleHeader {
    pub xmin: TransactionId,    // Insert transaction
    pub xmax: TransactionId,    // Delete transaction
    pub cmin: CommandId,        // Insert command
    pub cmax: CommandId,        // Delete command
    pub ctid: ItemPointer,      // Current or new tuple ID
    pub infomask: u16,          // Flags
    pub natts: u16,             // Number of attributes
    pub hoff: u8,               // Header offset
    pub bits: [u8],             // NULL bitmap (variable)
}
```

**Infomask Flags**:
- `XMIN_COMMITTED`: xmin is committed
- `XMIN_INVALID`: xmin is aborted
- `XMAX_COMMITTED`: xmax is committed
- `XMAX_INVALID`: xmax is aborted
- `UPDATED`: Tuple was updated
- `HAS_NULLS`: Has NULL values

### Tuple Data

```
┌──────────────────────────────────────┐
│ HeapTupleHeader (23+ bytes)          │
├──────────────────────────────────────┤
│ NULL Bitmap (if HAS_NULLS)           │
├──────────────────────────────────────┤
│ Attribute 1 (fixed-length)           │
├──────────────────────────────────────┤
│ Attribute 2 (fixed-length)           │
├──────────────────────────────────────┤
│ ...                                   │
├──────────────────────────────────────┤
│ Attribute N (variable-length)        │
│   [length | data]                    │
└──────────────────────────────────────┘
```

**Alignment**:
- 1-byte types: No alignment
- 2-byte types: 2-byte aligned
- 4-byte types: 4-byte aligned
- 8-byte types: 8-byte aligned

## B+Tree Index

### Node Structure

**Internal Node**:
```rust
pub struct BTreeInternalNode {
    pub header: NodeHeader,
    pub keys: Vec<Key>,
    pub children: Vec<PageId>,
}
```

**Leaf Node**:
```rust
pub struct BTreeLeafNode {
    pub header: NodeHeader,
    pub keys: Vec<Key>,
    pub values: Vec<ItemPointer>,
    pub next: Option<PageId>,
}
```

### Operations

**Search**:
```rust
fn search(&self, key: &Key) -> Option<ItemPointer> {
    let mut node = self.root;
    loop {
        if node.is_leaf() {
            return node.find(key);
        }
        node = node.find_child(key);
    }
}
```

**Insert**:
1. Search for leaf node
2. Insert key-value pair
3. If node full, split and propagate
4. Update parent pointers

**Delete**:
1. Search for leaf node
2. Remove key-value pair
3. If underflow, merge or redistribute
4. Update parent pointers

### Optimizations

**Prefix Compression**:
- Store common prefix once per node
- Reduces space by 30-50%

**Suffix Truncation**:
- Internal nodes only store separator keys
- Reduces internal node size

**Bulk Loading**:
- Sort data first
- Build tree bottom-up
- 2-3x faster than individual inserts

## LSM-Tree Storage

### Architecture

```
┌─────────────────────────────────────┐
│ MemTable (in-memory)                │
│   Skip List / B+Tree                │
└──────────────┬──────────────────────┘
               │ Flush when full
               ↓
┌─────────────────────────────────────┐
│ Level 0 (unsorted SSTables)         │
│   [SST1] [SST2] [SST3]              │
└──────────────┬──────────────────────┘
               │ Compact
               ↓
┌─────────────────────────────────────┐
│ Level 1 (sorted, non-overlapping)   │
│   [SST1] [SST2] [SST3]              │
└──────────────┬──────────────────────┘
               │ Compact
               ↓
┌─────────────────────────────────────┐
│ Level N (10x larger than N-1)       │
└─────────────────────────────────────┘
```

### SSTable Format

```
┌─────────────────────────────────────┐
│ Data Blocks                          │
│   [Block1] [Block2] ... [BlockN]    │
├─────────────────────────────────────┤
│ Index Block                          │
│   [Key1→Offset1] ... [KeyN→OffsetN] │
├─────────────────────────────────────┤
│ Bloom Filter                         │
├─────────────────────────────────────┤
│ Footer                               │
│   [Index Offset] [Bloom Offset]     │
└─────────────────────────────────────┘
```

### Compaction

**Size-Tiered**:
- Merge SSTables of similar size
- Good for write-heavy workloads
- Higher space amplification

**Leveled**:
- Merge into non-overlapping levels
- Better read performance
- Lower space amplification

**Configuration**:
```ini
lsm_memtable_size = 64MB
lsm_level0_files = 4
lsm_compaction_style = leveled
```

## Write-Ahead Logging

### WAL Structure

```rust
pub struct WALRecord {
    pub header: WALHeader,
    pub data: Vec<u8>,
    pub crc: u32,
}

pub struct WALHeader {
    pub xid: TransactionId,
    pub prev_lsn: LSN,
    pub record_type: RecordType,
    pub length: u32,
}
```

### Record Types

**Data Modification**:
- `INSERT`: Full tuple
- `UPDATE`: Old TID + new tuple or delta
- `DELETE`: TID only
- `TRUNCATE`: Relation OID

**Transaction Control**:
- `BEGIN`: Transaction start
- `COMMIT`: Transaction commit
- `ABORT`: Transaction rollback
- `PREPARE`: Two-phase commit prepare

**System**:
- `CHECKPOINT`: Checkpoint record
- `FULL_PAGE`: Full page image
- `SWITCH`: WAL segment switch

### WAL Writing

```rust
pub fn write_wal(&mut self, record: WALRecord) -> Result<LSN> {
    // 1. Serialize record
    let bytes = record.serialize();
    
    // 2. Append to WAL buffer
    let lsn = self.append_buffer(bytes);
    
    // 3. Flush if buffer full or commit
    if self.should_flush() {
        self.flush()?;
    }
    
    Ok(lsn)
}
```

**Flush Triggers**:
- Transaction commit
- Buffer full (default 16MB)
- Timeout (default 200ms)
- Checkpoint

### Configuration

```ini
wal_level = replica              # minimal, replica, logical
wal_buffers = 16MB               # WAL buffer size
wal_writer_delay = 200ms         # Flush interval
fsync = on                       # Fsync before commit
synchronous_commit = on          # Wait for fsync
wal_compression = on             # Compress WAL records
```

## Crash Recovery

### ARIES Protocol

**Phase 1: Analysis**
```rust
fn analysis_phase(&mut self) -> RecoveryState {
    let mut state = RecoveryState::new();
    
    for record in self.scan_wal_from_checkpoint() {
        match record.record_type {
            RecordType::Commit => state.committed.insert(record.xid),
            RecordType::Abort => state.aborted.insert(record.xid),
            _ => state.dirty_pages.insert(record.page_id),
        };
    }
    
    state
}
```

**Phase 2: Redo**
```rust
fn redo_phase(&mut self, state: &RecoveryState) {
    for record in self.scan_wal_from_checkpoint() {
        let page = self.buffer_pool.fetch(record.page_id);
        
        if page.lsn < record.lsn {
            self.apply_record(page, record);
            page.lsn = record.lsn;
        }
    }
}
```

**Phase 3: Undo**
```rust
fn undo_phase(&mut self, state: &RecoveryState) {
    for xid in state.active_transactions() {
        for record in self.scan_wal_backwards(xid) {
            self.undo_record(record);
        }
    }
}
```

## Checkpointing

### Checkpoint Process

1. **Begin Checkpoint**: Write checkpoint start record
2. **Flush Dirty Pages**: Write all dirty pages to disk
3. **Write Checkpoint Record**: Record LSN and active transactions
4. **Update Control File**: Point to new checkpoint

**Types**:
- **Full Checkpoint**: Flush all dirty pages (blocks writes)
- **Incremental Checkpoint**: Flush subset of pages (non-blocking)

**Configuration**:
```ini
checkpoint_timeout = 5min        # Time between checkpoints
checkpoint_completion_target = 0.9  # Spread I/O over 90% of interval
max_wal_size = 1GB               # Trigger checkpoint if exceeded
```

## Vacuum and Garbage Collection

### Dead Tuple Removal

```rust
pub fn vacuum_table(&mut self, table: &Table) {
    let oldest_xmin = self.get_oldest_xmin();
    
    for page in table.pages() {
        let mut dead_items = Vec::new();
        
        for (i, tuple) in page.tuples() {
            if tuple.xmax < oldest_xmin && tuple.xmax_committed() {
                dead_items.push(i);
            }
        }
        
        page.remove_items(dead_items);
        page.compact();
    }
}
```

**Vacuum Types**:
- **VACUUM**: Remove dead tuples, update statistics
- **VACUUM FULL**: Rewrite table, reclaim space
- **AUTOVACUUM**: Automatic background vacuum

**Configuration**:
```ini
autovacuum = on
autovacuum_naptime = 1min
autovacuum_vacuum_threshold = 50
autovacuum_vacuum_scale_factor = 0.2
```

## Storage Backends

### Heap Storage (Default)

**Characteristics**:
- Unordered tuple storage
- Fast inserts
- Sequential scans
- Requires indexes for fast lookups

**Use Cases**:
- OLTP workloads
- Mixed read/write
- General purpose

### Columnar Storage

**Characteristics**:
- Column-oriented layout
- High compression ratios
- Vectorized scans
- Slower updates

**Use Cases**:
- OLAP workloads
- Analytics queries
- Read-heavy workloads

### Configuration

```sql
-- Create table with storage engine
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email TEXT
) WITH (storage_engine = 'heap');

-- Or columnar for analytics
CREATE TABLE events (
    timestamp TIMESTAMP,
    user_id INT,
    event_type TEXT
) WITH (storage_engine = 'columnar');
```

## Performance Tuning

### Buffer Pool Sizing

**Rule of Thumb**:
- OLTP: 25% of RAM
- OLAP: 50-75% of RAM
- Mixed: 40% of RAM

**Monitoring**:
```sql
SELECT * FROM pg_stat_bgwriter;
SELECT * FROM pg_buffercache;
```

### I/O Optimization

**Direct I/O**:
```ini
wal_sync_method = fdatasync    # Linux
wal_sync_method = open_sync    # macOS
```

**Async I/O** (Linux):
```ini
backend_io_engine = io_uring
max_io_requests = 1000
```

### Page Compression

```ini
page_compression = on
page_compression_algorithm = lz4  # lz4, zstd, snappy
```

## Monitoring

### Key Metrics

```sql
-- Buffer pool hit ratio (target: >99%)
SELECT 
    sum(heap_blks_hit) / (sum(heap_blks_hit) + sum(heap_blks_read)) AS hit_ratio
FROM pg_statio_user_tables;

-- WAL write rate
SELECT * FROM pg_stat_wal;

-- Vacuum progress
SELECT * FROM pg_stat_progress_vacuum;
```

### Storage Statistics

```sql
-- Table sizes
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Index usage
SELECT * FROM pg_stat_user_indexes;
```
