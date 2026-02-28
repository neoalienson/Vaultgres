# Transaction Management

RustGres implements ACID transactions with Multi-Version Concurrency Control (MVCC) for high concurrency and isolation.

## ACID Properties

### Atomicity
- All-or-nothing execution via WAL
- Rollback on error or explicit abort
- Two-phase commit for distributed transactions

### Consistency
- Constraint enforcement (PK, FK, CHECK, UNIQUE)
- Trigger execution
- Deferred constraint checking

### Isolation
- Snapshot isolation (default)
- Serializable snapshot isolation (SSI)
- Read committed
- Repeatable read

### Durability
- Write-ahead logging with fsync
- Crash recovery via ARIES
- Point-in-time recovery

## MVCC Implementation

### Core Concepts

**Multi-Version Concurrency Control** allows:
- Non-blocking reads (readers don't block writers)
- Non-blocking writes (writers don't block readers)
- Snapshot isolation without locking

**Key Idea**: Each transaction sees a consistent snapshot of the database as it existed at transaction start.

### Transaction IDs

```rust
pub type TransactionId = u64;

pub struct TransactionManager {
    next_xid: AtomicU64,
    active_xids: DashMap<TransactionId, TransactionState>,
    snapshot_cache: LruCache<TransactionId, Snapshot>,
}
```

**XID Assignment**:
- Monotonically increasing counter
- Assigned at first write operation
- Read-only transactions may not get XID

**Special XIDs**:
- `0`: Invalid/bootstrap transaction
- `1`: Frozen transaction (always visible)
- `2`: First normal transaction

### Snapshots

```rust
pub struct Snapshot {
    pub xmin: TransactionId,           // Oldest active XID
    pub xmax: TransactionId,           // Next XID to assign
    pub active: Vec<TransactionId>,    // In-progress XIDs
    pub subxids: Vec<TransactionId>,   // Subtransaction XIDs
}
```

**Snapshot Creation**:
```rust
impl TransactionManager {
    pub fn get_snapshot(&self) -> Snapshot {
        let xmax = self.next_xid.load(Ordering::SeqCst);
        let active: Vec<_> = self.active_xids.iter()
            .map(|entry| *entry.key())
            .filter(|&xid| xid < xmax)
            .collect();
        let xmin = active.iter().min().copied().unwrap_or(xmax);
        
        Snapshot { xmin, xmax, active, subxids: vec![] }
    }
}
```

### Visibility Rules

```rust
pub fn tuple_visible(&self, tuple: &HeapTuple, snapshot: &Snapshot) -> bool {
    let xmin = tuple.xmin;
    let xmax = tuple.xmax;
    
    // Check if tuple was created by our transaction
    if xmin == snapshot.xmax - 1 {
        return xmax == 0 || xmax >= snapshot.xmax;
    }
    
    // Check if creating transaction committed before snapshot
    if xmin >= snapshot.xmax {
        return false;  // Created after snapshot
    }
    
    if snapshot.active.contains(&xmin) {
        return false;  // Creating transaction still active
    }
    
    if !self.xid_committed(xmin) {
        return false;  // Creating transaction aborted
    }
    
    // Tuple was created, check if deleted
    if xmax == 0 {
        return true;  // Not deleted
    }
    
    if xmax >= snapshot.xmax {
        return true;  // Deleted after snapshot
    }
    
    if snapshot.active.contains(&xmax) {
        return true;  // Deleting transaction still active
    }
    
    !self.xid_committed(xmax)  // Visible if delete aborted
}
```

### Transaction Status

```rust
pub enum TransactionStatus {
    InProgress,
    Committed,
    Aborted,
    SubCommitted,  // Subtransaction committed
}

pub struct TransactionStatusCache {
    clog: Vec<AtomicU8>,  // 2 bits per transaction
    cache: LruCache<TransactionId, TransactionStatus>,
}
```

**CLOG (Commit Log)**:
- 2 bits per transaction
- Stored on disk in pg_xact/
- Cached in memory for performance

## Isolation Levels

### Read Committed

**Behavior**:
- Each statement sees committed data as of statement start
- New snapshot per statement
- Prevents dirty reads

**Implementation**:
```rust
pub fn execute_statement(&mut self, stmt: Statement) -> Result<()> {
    // Get fresh snapshot for each statement
    let snapshot = self.txn_manager.get_snapshot();
    self.executor.execute(stmt, snapshot)
}
```

**Anomalies Prevented**:
- ✅ Dirty reads
- ❌ Non-repeatable reads
- ❌ Phantom reads

### Repeatable Read

**Behavior**:
- Single snapshot for entire transaction
- Sees consistent view throughout
- Prevents non-repeatable reads

**Implementation**:
```rust
pub fn begin_transaction(&mut self) -> Result<Transaction> {
    let xid = self.next_xid.fetch_add(1, Ordering::SeqCst);
    let snapshot = self.get_snapshot();
    
    Ok(Transaction {
        xid,
        snapshot,  // Reused for all statements
        state: TransactionState::InProgress,
    })
}
```

**Anomalies Prevented**:
- ✅ Dirty reads
- ✅ Non-repeatable reads
- ❌ Phantom reads (in some cases)

### Serializable

**Behavior**:
- Guarantees serializable execution
- Detects read-write conflicts
- May abort transactions to prevent anomalies

**Implementation (SSI)**:
```rust
pub struct SerializableTransaction {
    xid: TransactionId,
    snapshot: Snapshot,
    read_set: HashSet<ItemPointer>,
    write_set: HashSet<ItemPointer>,
    in_conflicts: Vec<TransactionId>,
    out_conflicts: Vec<TransactionId>,
}
```

**Conflict Detection**:
```rust
pub fn check_serializable(&self, txn: &SerializableTransaction) -> Result<()> {
    // Detect dangerous structures (rw-antidependency cycles)
    for conflict_txn in &txn.in_conflicts {
        if self.has_out_conflict(conflict_txn, &txn.out_conflicts) {
            return Err(Error::SerializationFailure);
        }
    }
    Ok(())
}
```

**Anomalies Prevented**:
- ✅ Dirty reads
- ✅ Non-repeatable reads
- ✅ Phantom reads
- ✅ Serialization anomalies

## Locking

### Lock Types

```rust
pub enum LockMode {
    AccessShare,       // SELECT
    RowShare,          // SELECT FOR UPDATE
    RowExclusive,      // INSERT, UPDATE, DELETE
    ShareUpdateExclusive,  // VACUUM, CREATE INDEX CONCURRENTLY
    Share,             // CREATE INDEX
    ShareRowExclusive, // Rare
    Exclusive,         // Rare
    AccessExclusive,   // DROP TABLE, TRUNCATE, VACUUM FULL
}
```

**Compatibility Matrix**:
```
                AS  RS  RE  SUE  S  SRE  E  AE
AccessShare     ✓   ✓   ✓   ✓   ✓   ✓   ✓   ✗
RowShare        ✓   ✓   ✓   ✓   ✓   ✓   ✗   ✗
RowExclusive    ✓   ✓   ✓   ✓   ✗   ✗   ✗   ✗
ShareUpdExcl    ✓   ✓   ✓   ✗   ✗   ✗   ✗   ✗
Share           ✓   ✓   ✗   ✗   ✓   ✗   ✗   ✗
ShareRowExcl    ✓   ✓   ✗   ✗   ✗   ✗   ✗   ✗
Exclusive       ✓   ✗   ✗   ✗   ✗   ✗   ✗   ✗
AccessExcl      ✗   ✗   ✗   ✗   ✗   ✗   ✗   ✗
```

### Lock Manager

```rust
pub struct LockManager {
    locks: DashMap<LockKey, LockEntry>,
    wait_graph: Mutex<WaitGraph>,
}

pub struct LockEntry {
    granted: Vec<(TransactionId, LockMode)>,
    waiting: VecDeque<(TransactionId, LockMode)>,
}
```

**Lock Acquisition**:
```rust
pub fn acquire_lock(&self, xid: TransactionId, key: LockKey, mode: LockMode) 
    -> Result<()> 
{
    let mut entry = self.locks.entry(key).or_default();
    
    // Check if compatible with granted locks
    if self.is_compatible(&entry.granted, mode) {
        entry.granted.push((xid, mode));
        return Ok(());
    }
    
    // Add to wait queue
    entry.waiting.push_back((xid, mode));
    
    // Check for deadlock
    if self.has_deadlock(xid) {
        return Err(Error::Deadlock);
    }
    
    // Wait for lock
    self.wait_for_lock(xid, key, mode)
}
```

### Deadlock Detection

**Wait-For Graph**:
```rust
pub struct WaitGraph {
    edges: HashMap<TransactionId, Vec<TransactionId>>,
}

impl WaitGraph {
    pub fn has_cycle(&self, start: TransactionId) -> bool {
        let mut visited = HashSet::new();
        let mut stack = vec![start];
        
        while let Some(node) = stack.pop() {
            if !visited.insert(node) {
                return true;  // Cycle detected
            }
            
            if let Some(neighbors) = self.edges.get(&node) {
                stack.extend(neighbors);
            }
        }
        
        false
    }
}
```

**Deadlock Resolution**:
- Detect cycles in wait-for graph
- Abort youngest transaction
- Retry with exponential backoff

## Savepoints

### Implementation

```rust
pub struct Savepoint {
    name: String,
    xid: TransactionId,
    command_id: CommandId,
    undo_log_position: usize,
}

impl Transaction {
    pub fn savepoint(&mut self, name: String) -> Savepoint {
        Savepoint {
            name,
            xid: self.xid,
            command_id: self.command_id,
            undo_log_position: self.undo_log.len(),
        }
    }
    
    pub fn rollback_to(&mut self, savepoint: &Savepoint) -> Result<()> {
        // Undo operations after savepoint
        while self.undo_log.len() > savepoint.undo_log_position {
            let op = self.undo_log.pop().unwrap();
            self.undo_operation(op)?;
        }
        
        self.command_id = savepoint.command_id;
        Ok(())
    }
}
```

### Usage

```sql
BEGIN;
INSERT INTO users (email) VALUES ('user1@example.com');

SAVEPOINT sp1;
INSERT INTO users (email) VALUES ('user2@example.com');

ROLLBACK TO sp1;  -- Undo second insert
COMMIT;           -- Commit first insert
```

## Two-Phase Commit

### Protocol

**Phase 1: Prepare**
```rust
pub fn prepare(&mut self, gid: &str) -> Result<()> {
    // Write prepare record to WAL
    self.wal.write(WALRecord::Prepare {
        xid: self.xid,
        gid: gid.to_string(),
    })?;
    
    // Flush WAL
    self.wal.flush()?;
    
    self.state = TransactionState::Prepared;
    Ok(())
}
```

**Phase 2: Commit/Abort**
```rust
pub fn commit_prepared(&mut self, gid: &str) -> Result<()> {
    let xid = self.prepared_xacts.get(gid)
        .ok_or(Error::UnknownPreparedTransaction)?;
    
    // Write commit record
    self.wal.write(WALRecord::CommitPrepared {
        xid: *xid,
        gid: gid.to_string(),
    })?;
    
    // Update transaction status
    self.clog.set_committed(*xid);
    
    Ok(())
}
```

### Usage

```sql
-- Coordinator
BEGIN;
INSERT INTO accounts (balance) VALUES (100);
PREPARE TRANSACTION 'txn_001';

-- Later (after all participants prepared)
COMMIT PREPARED 'txn_001';

-- Or rollback
ROLLBACK PREPARED 'txn_001';
```

## Subtransactions

### Implementation

```rust
pub struct Subtransaction {
    parent_xid: TransactionId,
    subxid: TransactionId,
    command_id: CommandId,
    state: TransactionState,
}

impl Transaction {
    pub fn begin_subtransaction(&mut self) -> Subtransaction {
        let subxid = self.txn_manager.next_xid();
        
        Subtransaction {
            parent_xid: self.xid,
            subxid,
            command_id: self.command_id,
            state: TransactionState::InProgress,
        }
    }
}
```

### Usage

```sql
BEGIN;
INSERT INTO users (email) VALUES ('user1@example.com');

-- Implicit subtransaction in PL/pgSQL
DO $$
BEGIN
    INSERT INTO users (email) VALUES ('user2@example.com');
EXCEPTION
    WHEN unique_violation THEN
        -- Subtransaction rolled back, main transaction continues
        RAISE NOTICE 'Duplicate email';
END $$;

COMMIT;
```

## Transaction Lifecycle

### State Machine

```
                    BEGIN
                      ↓
    ┌──────────→ InProgress ←──────────┐
    │                ↓                  │
    │         (first write)             │
    │                ↓                  │
    │            Active                 │
    │                ↓                  │
    │         PREPARE (2PC)             │
    │                ↓                  │
    │            Prepared               │
    │                ↓                  │
ROLLBACK      COMMIT/ABORT         SAVEPOINT
    │                ↓                  │
    │         Committing                │
    │                ↓                  │
    └──────────→ Committed              │
                     ↓                  │
                 Aborted ←──────────────┘
```

### Transaction Context

```rust
pub struct TransactionContext {
    pub xid: TransactionId,
    pub snapshot: Snapshot,
    pub command_id: CommandId,
    pub isolation_level: IsolationLevel,
    pub read_only: bool,
    pub deferrable: bool,
    pub locks: Vec<LockKey>,
    pub undo_log: Vec<UndoRecord>,
}
```

## Performance Optimization

### Transaction ID Wraparound

**Problem**: 32-bit XIDs wrap around after 4 billion transactions

**Solution**: Vacuum freezing
```rust
pub fn freeze_tuple(&mut self, tuple: &mut HeapTuple) {
    if tuple.xmin < self.freeze_limit {
        tuple.xmin = FROZEN_XID;
        tuple.infomask |= XMIN_FROZEN;
    }
}
```

**Configuration**:
```ini
vacuum_freeze_min_age = 50000000
vacuum_freeze_table_age = 150000000
autovacuum_freeze_max_age = 200000000
```

### Commit Timestamp

**Track commit time for each transaction**:
```rust
pub struct CommitTimestamp {
    timestamps: Vec<AtomicU64>,
}

impl CommitTimestamp {
    pub fn set(&self, xid: TransactionId, ts: u64) {
        self.timestamps[xid as usize].store(ts, Ordering::Release);
    }
    
    pub fn get(&self, xid: TransactionId) -> Option<u64> {
        let ts = self.timestamps[xid as usize].load(Ordering::Acquire);
        if ts > 0 { Some(ts) } else { None }
    }
}
```

**Use Cases**:
- Logical replication
- Conflict resolution
- Audit logging

## Monitoring

### Transaction Statistics

```sql
-- Active transactions
SELECT * FROM pg_stat_activity WHERE state = 'active';

-- Long-running transactions
SELECT 
    pid,
    now() - xact_start AS duration,
    query
FROM pg_stat_activity
WHERE xact_start IS NOT NULL
ORDER BY duration DESC;

-- Transaction ID usage
SELECT 
    age(datfrozenxid) AS xid_age,
    datname
FROM pg_database
ORDER BY xid_age DESC;
```

### Lock Monitoring

```sql
-- Current locks
SELECT * FROM pg_locks;

-- Blocking queries
SELECT 
    blocked.pid AS blocked_pid,
    blocked.query AS blocked_query,
    blocking.pid AS blocking_pid,
    blocking.query AS blocking_query
FROM pg_stat_activity blocked
JOIN pg_locks blocked_locks ON blocked.pid = blocked_locks.pid
JOIN pg_locks blocking_locks ON blocked_locks.locktype = blocking_locks.locktype
JOIN pg_stat_activity blocking ON blocking_locks.pid = blocking.pid
WHERE NOT blocked_locks.granted AND blocking_locks.granted;
```

## Best Practices

### Transaction Design

1. **Keep transactions short**: Minimize lock hold time
2. **Avoid long-running transactions**: Prevents vacuum, increases bloat
3. **Use appropriate isolation level**: Balance consistency vs performance
4. **Handle serialization failures**: Retry on conflict

### Error Handling

```rust
pub fn execute_with_retry<F, T>(&self, mut f: F) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    let mut retries = 0;
    loop {
        match f() {
            Ok(result) => return Ok(result),
            Err(Error::SerializationFailure) if retries < 3 => {
                retries += 1;
                std::thread::sleep(Duration::from_millis(100 * retries));
            }
            Err(e) => return Err(e),
        }
    }
}
```

### Configuration

```ini
# Transaction settings
default_transaction_isolation = 'read committed'
default_transaction_read_only = off
default_transaction_deferrable = off

# Lock settings
deadlock_timeout = 1s
max_locks_per_transaction = 64

# Statement timeout
statement_timeout = 0  # disabled
lock_timeout = 0       # disabled
idle_in_transaction_session_timeout = 0  # disabled
```
