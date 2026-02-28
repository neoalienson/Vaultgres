# Implementation Standards

Engineering standards and best practices for RustGres development.

## Code Style

### Rust Conventions

Follow [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/):

```rust
// Use snake_case for functions and variables
fn calculate_buffer_size(page_count: usize) -> usize { }

// Use CamelCase for types
struct BufferPool { }
enum TransactionState { }

// Use SCREAMING_SNAKE_CASE for constants
const MAX_CONNECTIONS: usize = 1000;
const DEFAULT_PAGE_SIZE: usize = 8192;

// Prefer explicit types for public APIs
pub fn execute_query(sql: &str) -> Result<ResultSet, Error> { }

// Use descriptive names, avoid abbreviations
let transaction_manager = TransactionManager::new();  // Good
let txn_mgr = TxnMgr::new();  // Bad
```

### Formatting

```bash
# Format all code before commit
cargo fmt

# Check formatting in CI
cargo fmt -- --check
```

**Configuration** (`.rustfmt.toml`):
```toml
max_width = 100
tab_spaces = 4
edition = "2021"
use_small_heuristics = "Max"
```

### Linting

```bash
# Run clippy with strict settings
cargo clippy -- -D warnings

# Check all targets
cargo clippy --all-targets --all-features -- -D warnings
```

**Allowed lints** (when justified):
```rust
#[allow(clippy::too_many_arguments)]  // For builder patterns
#[allow(clippy::large_enum_variant)]  // When performance matters
```

## Architecture Principles

### SOLID Principles

**Single Responsibility**:
```rust
// Good: Each struct has one responsibility
struct BufferPool {
    fn fetch_page(&mut self, page_id: PageId) -> &Page;
    fn evict_page(&mut self) -> PageId;
}

struct PageManager {
    fn allocate_page(&mut self) -> PageId;
    fn free_page(&mut self, page_id: PageId);
}

// Bad: Mixed responsibilities
struct BufferPoolManager {
    fn fetch_page(&mut self, page_id: PageId) -> &Page;
    fn allocate_page(&mut self) -> PageId;
    fn execute_query(&mut self, sql: &str) -> Result;  // Wrong layer!
}
```

**Open/Closed**:
```rust
// Open for extension via traits
trait StorageEngine {
    fn read(&self, key: &[u8]) -> Result<Vec<u8>>;
    fn write(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
}

struct BTreeStorage { }
struct LSMStorage { }

impl StorageEngine for BTreeStorage { }
impl StorageEngine for LSMStorage { }
```

**Liskov Substitution**:
```rust
// All executors must be interchangeable
trait Executor {
    fn next(&mut self) -> Result<Option<Tuple>>;
    fn schema(&self) -> &Schema;
}

// Any Executor can be used anywhere
fn execute_plan(executor: Box<dyn Executor>) -> Result<Vec<Tuple>> {
    let mut results = Vec::new();
    while let Some(tuple) = executor.next()? {
        results.push(tuple);
    }
    Ok(results)
}
```

**Interface Segregation**:
```rust
// Split large interfaces into focused traits
trait Readable {
    fn read(&self, offset: usize, buf: &mut [u8]) -> Result<usize>;
}

trait Writable {
    fn write(&mut self, offset: usize, buf: &[u8]) -> Result<usize>;
}

trait Seekable {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64>;
}

// Implement only what's needed
impl Readable for ReadOnlyFile { }
impl Readable + Writable + Seekable for File { }
```

**Dependency Inversion**:
```rust
// Depend on abstractions, not concrete types
struct QueryExecutor<S: StorageEngine> {
    storage: S,
}

impl<S: StorageEngine> QueryExecutor<S> {
    fn execute(&mut self, plan: Plan) -> Result<ResultSet> {
        // Works with any StorageEngine implementation
        let data = self.storage.read(&plan.table)?;
        // ...
    }
}
```

### DRY (Don't Repeat Yourself)

```rust
// Bad: Repeated logic
impl BTreeIndex {
    fn insert(&mut self, key: Key, value: Value) -> Result<()> {
        if self.is_full() {
            self.split();
        }
        // insert logic
    }
}

impl HashIndex {
    fn insert(&mut self, key: Key, value: Value) -> Result<()> {
        if self.is_full() {
            self.split();
        }
        // insert logic
    }
}

// Good: Extract common behavior
trait Index {
    fn is_full(&self) -> bool;
    fn split(&mut self);
    fn insert_internal(&mut self, key: Key, value: Value) -> Result<()>;
    
    fn insert(&mut self, key: Key, value: Value) -> Result<()> {
        if self.is_full() {
            self.split();
        }
        self.insert_internal(key, value)
    }
}
```

### Composition Over Inheritance

```rust
// Use composition with traits instead of inheritance
struct QueryOptimizer {
    cost_model: CostModel,
    statistics: Statistics,
    rules: Vec<Box<dyn OptimizationRule>>,
}

impl QueryOptimizer {
    fn optimize(&self, plan: LogicalPlan) -> PhysicalPlan {
        let mut optimized = plan;
        for rule in &self.rules {
            optimized = rule.apply(optimized);
        }
        self.cost_model.select_best_plan(optimized, &self.statistics)
    }
}
```

### Separation of Concerns

```rust
// Clear layer separation
mod storage {
    // Only storage concerns: pages, files, I/O
}

mod transaction {
    // Only transaction concerns: MVCC, locking
}

mod executor {
    // Only execution concerns: operators, pipelines
}

// No cross-layer dependencies (use interfaces)
```

## Error Handling

### Use thiserror for Errors

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("page {0} not found")]
    PageNotFound(PageId),
    
    #[error("buffer pool full")]
    BufferPoolFull,
    
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("corruption detected at offset {offset}: {details}")]
    Corruption { offset: u64, details: String },
}

// Use Result everywhere
pub fn read_page(id: PageId) -> Result<Page, StorageError> {
    // ...
}
```

### Error Context

```rust
use anyhow::{Context, Result};

fn load_config(path: &Path) -> Result<Config> {
    let content = std::fs::read_to_string(path)
        .context(format!("Failed to read config from {}", path.display()))?;
    
    let config: Config = toml::from_str(&content)
        .context("Failed to parse config file")?;
    
    Ok(config)
}
```

### Never Panic in Library Code

```rust
// Bad
pub fn get_page(&self, id: PageId) -> &Page {
    self.pages.get(&id).unwrap()  // Can panic!
}

// Good
pub fn get_page(&self, id: PageId) -> Result<&Page, StorageError> {
    self.pages.get(&id).ok_or(StorageError::PageNotFound(id))
}
```

## Testing Standards

### Unit Tests

**Coverage**: 90%+ for all modules

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool_fetch() {
        // Arrange
        let mut pool = BufferPool::new(10);
        let page_id = PageId(1);
        
        // Act
        let page = pool.fetch(page_id).unwrap();
        
        // Assert
        assert_eq!(page.id(), page_id);
        assert_eq!(pool.size(), 1);
    }
    
    #[test]
    fn test_buffer_pool_eviction() {
        let mut pool = BufferPool::new(2);
        
        // Fill buffer pool
        pool.fetch(PageId(1)).unwrap();
        pool.fetch(PageId(2)).unwrap();
        
        // This should trigger eviction
        pool.fetch(PageId(3)).unwrap();
        
        assert_eq!(pool.size(), 2);
    }
    
    #[test]
    #[should_panic(expected = "buffer pool full")]
    fn test_buffer_pool_full() {
        let mut pool = BufferPool::new(1);
        pool.fetch(PageId(1)).unwrap();
        pool.fetch(PageId(2)).unwrap();  // Should panic
    }
}
```

### Property-Based Testing

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_btree_insert_retrieve(key in 0u64..1000, value in 0u64..1000) {
        let mut tree = BTree::new();
        tree.insert(key, value).unwrap();
        assert_eq!(tree.get(key), Some(value));
    }
    
    #[test]
    fn test_btree_ordering(keys in prop::collection::vec(0u64..1000, 10..100)) {
        let mut tree = BTree::new();
        for (i, &key) in keys.iter().enumerate() {
            tree.insert(key, i as u64).unwrap();
        }
        
        // Verify sorted iteration
        let mut prev = None;
        for (key, _) in tree.iter() {
            if let Some(p) = prev {
                assert!(key >= p);
            }
            prev = Some(key);
        }
    }
}
```

### Integration Tests

**Location**: `tests/` directory

```rust
// tests/integration_test.rs
use rustgres::*;

#[test]
fn test_end_to_end_query() {
    let db = Database::new_temp().unwrap();
    
    // Create table
    db.execute("CREATE TABLE users (id INT, name TEXT)").unwrap();
    
    // Insert data
    db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob')").unwrap();
    
    // Query
    let result = db.execute("SELECT * FROM users WHERE id = 1").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get::<i32>(0), 1);
    assert_eq!(result.rows[0].get::<String>(1), "Alice");
}

#[test]
fn test_transaction_rollback() {
    let db = Database::new_temp().unwrap();
    db.execute("CREATE TABLE accounts (id INT, balance INT)").unwrap();
    db.execute("INSERT INTO accounts VALUES (1, 100)").unwrap();
    
    // Start transaction
    db.execute("BEGIN").unwrap();
    db.execute("UPDATE accounts SET balance = 50 WHERE id = 1").unwrap();
    db.execute("ROLLBACK").unwrap();
    
    // Verify rollback
    let result = db.execute("SELECT balance FROM accounts WHERE id = 1").unwrap();
    assert_eq!(result.rows[0].get::<i32>(0), 100);
}
```

### Performance Tests

**Location**: `benches/` directory

```rust
// benches/btree_benchmark.rs
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use rustgres::storage::BTree;

fn bench_btree_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree_insert");
    
    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut tree = BTree::new();
                for i in 0..size {
                    tree.insert(black_box(i), black_box(i * 2)).unwrap();
                }
            });
        });
    }
    
    group.finish();
}

fn bench_btree_lookup(c: &mut Criterion) {
    let mut tree = BTree::new();
    for i in 0..10000 {
        tree.insert(i, i * 2).unwrap();
    }
    
    c.bench_function("btree_lookup", |b| {
        b.iter(|| {
            tree.get(black_box(5000))
        });
    });
}

criterion_group!(benches, bench_btree_insert, bench_btree_lookup);
criterion_main!(benches);
```

**Performance Targets**:
```rust
// Add performance assertions
#[test]
fn test_query_performance() {
    let db = setup_large_database();
    
    let start = Instant::now();
    db.execute("SELECT * FROM users WHERE id = 1").unwrap();
    let duration = start.elapsed();
    
    assert!(duration < Duration::from_millis(1), 
            "Query took {:?}, expected <1ms", duration);
}
```

### Test Organization

```
tests/
├── unit/              # Unit tests (also in src/)
├── integration/       # Integration tests
│   ├── sql_test.rs
│   ├── transaction_test.rs
│   └── replication_test.rs
├── performance/       # Performance tests
│   ├── tpcc.rs
│   └── tpch.rs
├── fuzz/             # Fuzz tests
│   ├── parser_fuzz.rs
│   └── executor_fuzz.rs
└── compat/           # PostgreSQL compatibility tests
    └── pg_regress.rs
```

## Documentation Standards

### Public API Documentation

```rust
/// Manages a pool of database pages in memory.
///
/// The buffer pool uses an LRU eviction policy to manage a fixed-size
/// cache of pages. Pages are pinned while in use to prevent eviction.
///
/// # Examples
///
/// ```
/// use rustgres::storage::BufferPool;
///
/// let mut pool = BufferPool::new(100);
/// let page = pool.fetch(PageId(1))?;
/// // Use page...
/// pool.unpin(PageId(1));
/// # Ok::<(), Error>(())
/// ```
///
/// # Thread Safety
///
/// BufferPool is thread-safe and can be shared across threads using Arc.
pub struct BufferPool {
    // ...
}

impl BufferPool {
    /// Creates a new buffer pool with the specified capacity.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Maximum number of pages to cache
    ///
    /// # Panics
    ///
    /// Panics if capacity is 0.
    ///
    /// # Examples
    ///
    /// ```
    /// let pool = BufferPool::new(100);
    /// ```
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be positive");
        // ...
    }
    
    /// Fetches a page from the buffer pool.
    ///
    /// If the page is not in the pool, it will be loaded from disk.
    /// The page is pinned and must be unpinned when done.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::PageNotFound` if the page doesn't exist.
    /// Returns `StorageError::BufferPoolFull` if no pages can be evicted.
    ///
    /// # Examples
    ///
    /// ```
    /// let page = pool.fetch(PageId(1))?;
    /// // Use page...
    /// pool.unpin(PageId(1));
    /// # Ok::<(), Error>(())
    /// ```
    pub fn fetch(&mut self, page_id: PageId) -> Result<&Page, StorageError> {
        // ...
    }
}
```

### Module Documentation

```rust
//! Storage layer implementation.
//!
//! This module provides the core storage abstractions including:
//! - Page-based storage with 8KB pages
//! - Buffer pool for caching pages in memory
//! - B+Tree indexes for fast lookups
//! - Write-ahead logging for durability
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────┐
//! │  Buffer Pool    │
//! └────────┬────────┘
//!          │
//! ┌────────▼────────┐
//! │  Page Manager   │
//! └────────┬────────┘
//!          │
//! ┌────────▼────────┐
//! │   File I/O      │
//! └─────────────────┘
//! ```
//!
//! # Examples
//!
//! ```
//! use rustgres::storage::{BufferPool, PageManager};
//!
//! let mut pool = BufferPool::new(100);
//! let page = pool.fetch(PageId(1))?;
//! # Ok::<(), Error>(())
//! ```
```

## Performance Standards

### Benchmarking Requirements

Every performance-critical component must have benchmarks:

```rust
// Required benchmarks for each component:
// - Best case performance
// - Average case performance
// - Worst case performance
// - Scalability (varying input sizes)

#[bench]
fn bench_buffer_pool_hit(b: &mut Bencher) {
    let mut pool = setup_warm_cache();
    b.iter(|| pool.fetch(black_box(PageId(1))));
}

#[bench]
fn bench_buffer_pool_miss(b: &mut Bencher) {
    let mut pool = BufferPool::new(10);
    b.iter(|| pool.fetch(black_box(PageId::random())));
}
```

### Performance Regression Tests

```rust
// tests/performance/regression.rs
#[test]
fn test_no_performance_regression() {
    let baseline = load_baseline_metrics();
    let current = run_benchmarks();
    
    for (name, current_time) in current {
        let baseline_time = baseline.get(name).unwrap();
        let regression = (current_time - baseline_time) / baseline_time;
        
        assert!(regression < 0.05, 
                "{} regressed by {:.1}%", name, regression * 100.0);
    }
}
```

### Profiling

```bash
# CPU profiling
cargo build --release
perf record --call-graph=dwarf ./target/release/rustgres
perf report

# Memory profiling
valgrind --tool=massif ./target/debug/rustgres
ms_print massif.out.*

# Flamegraph
cargo flamegraph --bench my_benchmark
```

## Code Review Standards

### Review Checklist

- [ ] Code follows style guidelines (rustfmt, clippy)
- [ ] All public APIs documented
- [ ] Unit tests added/updated (90%+ coverage)
- [ ] Integration tests for new features
- [ ] Performance tests for critical paths
- [ ] No unwrap() or panic!() in library code
- [ ] Error handling is comprehensive
- [ ] No unsafe code without justification
- [ ] Thread safety considered
- [ ] Memory leaks checked (valgrind)

### PR Requirements

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Performance improvement
- [ ] Refactoring
- [ ] Documentation

## Testing
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Performance tests pass
- [ ] Manual testing completed

## Performance Impact
- Benchmark results: [link]
- Memory usage: [before/after]
- No regression: [yes/no]

## Checklist
- [ ] Code formatted (cargo fmt)
- [ ] Lints pass (cargo clippy)
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
```

## Continuous Integration

### CI Pipeline

```yaml
# .github/workflows/ci.yml
name: CI

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
      
      - name: Format check
        run: cargo fmt -- --check
      
      - name: Clippy
        run: cargo clippy -- -D warnings
      
      - name: Unit tests
        run: cargo test --lib
      
      - name: Integration tests
        run: cargo test --test '*'
      
      - name: Doc tests
        run: cargo test --doc
      
      - name: Coverage
        run: |
          cargo install cargo-tarpaulin
          cargo tarpaulin --out Xml
          bash <(curl -s https://codecov.io/bash)
  
  bench:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Benchmark
        run: cargo bench --no-fail-fast
      
      - name: Check regression
        run: cargo run --bin check-regression
```

## Security Standards

### Unsafe Code

```rust
// Unsafe code requires:
// 1. Detailed safety comment
// 2. Justification for why unsafe is needed
// 3. Proof that invariants are maintained

/// # Safety
///
/// This function is safe because:
/// 1. The pointer is guaranteed to be valid (allocated by Box)
/// 2. The lifetime is bounded by the struct lifetime
/// 3. No other references exist during this operation
unsafe fn read_raw_page(ptr: *const u8, len: usize) -> &[u8] {
    std::slice::from_raw_parts(ptr, len)
}
```

### Input Validation

```rust
// Always validate external input
pub fn parse_sql(sql: &str) -> Result<Statement, ParseError> {
    // Limit input size
    if sql.len() > MAX_QUERY_SIZE {
        return Err(ParseError::QueryTooLarge);
    }
    
    // Validate UTF-8
    if !sql.is_ascii() {
        return Err(ParseError::InvalidEncoding);
    }
    
    // Parse...
}
```

## Summary

**Key Principles**:
1. **Correctness**: Comprehensive testing, no panics
2. **Performance**: Benchmarks, profiling, regression tests
3. **Maintainability**: Clear code, good docs, SOLID principles
4. **Safety**: Minimal unsafe, input validation, error handling
5. **Quality**: 90%+ coverage, CI/CD, code review
