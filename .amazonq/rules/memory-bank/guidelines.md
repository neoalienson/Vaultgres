# Development Guidelines

## Code Quality Standards

### Code Formatting
- **Indentation**: 4 spaces (enforced by rustfmt)
- **Line Length**: Soft limit at 100 characters, hard limit at 120
- **Brace Style**: Opening braces on same line (K&R style)
- **Imports**: Group by std, external crates, internal modules
- **Trailing Commas**: Used in multi-line collections and function arguments

### Structural Conventions
- **Module Organization**: One primary type per file, related types grouped together
- **Visibility**: Default to private, expose only necessary public APIs
- **File Naming**: Snake_case matching module names (e.g., `join_order.rs`)
- **Test Organization**: Tests in dedicated `#[cfg(test)] mod tests` blocks or separate `*_tests.rs` files

### Naming Standards
- **Types**: PascalCase (e.g., `BTree`, `JoinOptimizer`, `TupleId`)
- **Functions/Methods**: snake_case (e.g., `insert`, `optimize_dp`, `estimate_selectivity`)
- **Constants**: SCREAMING_SNAKE_CASE (e.g., `PORT_COUNTER`)
- **Type Aliases**: PascalCase (e.g., `Key`, `TupleId`)
- **Lifetimes**: Single lowercase letter or descriptive (e.g., `'a`, `'static`)

### Documentation Standards
- **Module-Level Docs**: Use `//!` for module documentation at file top
- **Item Documentation**: Use `///` for public types, functions, and methods
- **Doc Comments**: Describe purpose, parameters, return values, and examples
- **Edge Cases**: Document special behavior, panics, and error conditions
- **Examples**: Include code examples in doc comments for complex APIs

## Practices Followed Throughout Codebase

### Error Handling
- **Error Types**: Each module defines its own error type using `thiserror` crate
- **Result Types**: All fallible operations return `Result<T, E>`
- **Error Propagation**: Use `?` operator for error propagation
- **Error Messages**: Descriptive error messages with context
- **Example Pattern**:
```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Page {0} not found")]
    PageNotFound(PageId),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, StorageError>;
```

### Testing Strategy
- **Unit Tests**: In-module tests for individual functions and methods
- **Integration Tests**: Cross-module tests in `tests/integration/` directory
- **E2E Tests**: End-to-end tests using actual server process and psql client
- **Edge Tests**: Dedicated `*_edge_tests.rs` files for boundary conditions
- **Test Naming**: Descriptive names starting with `test_` prefix
- **Test Organization**: Group related tests in same file, one assertion focus per test

### Test Patterns
```rust
#[test]
fn test_btree_insert_and_get() {
    let mut tree = BTree::new();
    let key = vec![1, 2, 3];
    let value = TupleId { page_id: PageId(1), slot: 0 };
    
    tree.insert(key.clone(), value).unwrap();
    assert_eq!(tree.get(&key), Some(value));
}

#[test]
fn test_btree_get_nonexistent() {
    let tree = BTree::new();
    let key = vec![1, 2, 3];
    assert_eq!(tree.get(&key), None);
}
```

### Concurrency Patterns
- **Synchronization**: Use `parking_lot` crate for mutexes and RwLocks
- **Concurrent Collections**: Use `dashmap` for concurrent hash maps
- **Atomic Operations**: Use `std::sync::atomic` for lock-free counters
- **Example**:
```rust
use std::sync::atomic::{AtomicU16, Ordering};
use parking_lot::RwLock;
use dashmap::DashMap;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(15433);
let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
```

### Resource Management
- **RAII Pattern**: Use Drop trait for automatic cleanup
- **Temporary Resources**: Use `tempfile` crate for temporary directories in tests
- **Cleanup**: Implement Drop for resources requiring explicit cleanup
- **Example**:
```rust
impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        thread::sleep(Duration::from_millis(100));
        // TempDir automatically cleans up
    }
}
```

## Semantic Patterns Overview

### Builder Pattern for Complex Types
- Use builder methods for types with multiple configuration options
- Provide sensible defaults with `new()` and customization with `with_*()` methods
```rust
impl BTree {
    pub fn new() -> Self {
        Self::with_order(128)
    }
    
    pub fn with_order(order: usize) -> Self {
        Self { root: None, order }
    }
}
```

### Default Trait Implementation
- Implement `Default` trait for types with obvious default values
- Delegate to `new()` method when appropriate
```rust
impl Default for BTree {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for JoinOptimizer {
    fn default() -> Self {
        Self::new()
    }
}
```

### Iterator Pattern
- Provide iterators for collection-like types
- Define custom iterator structs with lifetime parameters
- Implement `Iterator` trait with appropriate `Item` type
```rust
pub struct BTreeIterator<'a> {
    node: Option<&'a Node>,
    index: usize,
}

impl<'a> Iterator for BTreeIterator<'a> {
    type Item = (&'a Key, TupleId);
    
    fn next(&mut self) -> Option<Self::Item> {
        // Iterator implementation
    }
}
```

### Enum-Based Polymorphism
- Use enums for types with distinct variants
- Pattern match on enum variants for type-specific behavior
- Keep enum variants private when implementation detail
```rust
enum Node {
    Internal(InternalNode),
    Leaf(LeafNode),
}

match node {
    Node::Leaf(leaf) => { /* handle leaf */ },
    Node::Internal(internal) => { /* handle internal */ },
}
```

### Type Aliases for Domain Concepts
- Define type aliases for domain-specific concepts
- Improves code readability and maintainability
```rust
pub type Key = Vec<u8>;
pub type Result<T> = std::result::Result<T, StorageError>;
```

### Struct-Based Configuration
- Use structs to group related configuration or data
- Derive common traits (Debug, Clone, Copy, PartialEq, Eq)
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TupleId {
    pub page_id: PageId,
    pub slot: u16,
}

#[derive(Debug, Clone)]
pub struct Relation {
    pub id: usize,
    pub name: String,
    pub row_count: u64,
}
```

## Internal API Usage and Patterns

### Storage Layer APIs
```rust
// B+Tree index operations
let mut tree = BTree::new();
tree.insert(key, value)?;
let result = tree.get(&key);
tree.delete(&key)?;

// Iterate over entries
for (key, value) in tree.iter() {
    // Process entries
}
```

### Optimizer APIs
```rust
// Cost model usage
let model = CostModel::new();
let seq_cost = model.estimate_seq_scan(&stats, selectivity)?;
let idx_cost = model.estimate_index_scan(&stats, selectivity)?;
let join_cost = model.estimate_nested_loop_join(&left, &right)?;

// Join optimization
let optimizer = JoinOptimizer::new();
let plan = optimizer.optimize(relations)?;
```

### Statistics APIs
```rust
// Histogram creation and usage
let mut hist = Histogram::new(10);
hist.build(values)?;
let selectivity = hist.estimate_selectivity(value);

// Selectivity estimation
let estimator = SelectivityEstimator::new();
let eq_sel = estimator.estimate_equality(&stats);
let and_sel = estimator.estimate_and(sel1, sel2);
let or_sel = estimator.estimate_or(sel1, sel2);
```

### Test Infrastructure APIs
```rust
// E2E test server setup
let server = TestServer::start();
let result = server.execute_sql("CREATE TABLE users (id INT, name TEXT)");
assert!(result.is_ok());

// Temporary directories for tests
let data_dir = TempDir::new()?;
let path = data_dir.path();
// Directory automatically cleaned up on drop
```

## Frequently Used Code Idioms

### Option and Result Handling
```rust
// Early return with ?
pub fn get(&self, key: &Key) -> Option<TupleId> {
    let root = self.root.as_ref()?;  // Return None if root is None
    // Continue processing
}

// Pattern matching on Result
match result {
    Ok(value) => { /* handle success */ },
    Err(e) => { /* handle error */ },
}

// Unwrap with expect for tests
tree.insert(key, value).expect("INSERT failed");
```

### Binary Search for Sorted Collections
```rust
// Find insertion position
let pos = leaf.keys.binary_search(&key).unwrap_or_else(|e| e);
leaf.keys.insert(pos, key);

// Check if key exists
if let Ok(idx) = leaf.keys.binary_search(key) {
    leaf.keys.remove(idx);
    return Ok(true);
}
```

### Cloning for Ownership Transfer
```rust
// Clone when moving into owned structure
tree.insert(key.clone(), value).unwrap();
assert_eq!(tree.get(&key), Some(value));

// Clone for recursive structures
best_plan = Some(JoinPlan {
    left: Some(Box::new(left_plan.clone())),
    right: Some(Box::new(right_plan.clone())),
    relation: None,
    cost: join_cost,
});
```

### Conditional Compilation for Tests
```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_something() {
        // Test code
    }
}

// Edge tests in separate files
#[cfg(test)]
mod config_edge_tests;
```

### Dynamic Programming with HashMap
```rust
let mut dp: HashMap<Vec<usize>, JoinPlan> = HashMap::new();

// Store subproblem solutions
dp.insert(vec![i], plan);

// Retrieve and use cached solutions
if let (Some(left_plan), Some(right_plan)) = (dp.get(&left_set), dp.get(&right_set)) {
    // Use cached plans
}
```

## Popular Annotations and Attributes

### Derive Macros
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]  // Common for simple types
#[derive(Debug, Clone)]                        // For types with heap data
#[derive(Error, Debug)]                        // For error types (thiserror)
#[derive(Default)]                             // For types with obvious defaults
```

### Test Attributes
```rust
#[test]                    // Mark function as test
#[cfg(test)]              // Conditional compilation for tests
#[should_panic]           // Test should panic (rarely used)
```

### Conditional Compilation
```rust
#[cfg(test)]              // Only compile in test builds
#[cfg(feature = "...")]   // Feature-gated code
```

### Visibility and Warnings
```rust
pub                       // Public visibility
pub(crate)               // Crate-visible (used sparingly)
#[allow(dead_code)]      // Suppress unused code warnings (rare)
```

## Code Review Checklist

### Before Submitting Code
- [ ] All tests pass (`cargo test`)
- [ ] Code is formatted (`cargo fmt`)
- [ ] No clippy warnings (`cargo clippy`)
- [ ] Public APIs have documentation
- [ ] Error cases are handled with Result types
- [ ] Resources are properly cleaned up (Drop trait)
- [ ] Edge cases have dedicated tests
- [ ] Integration tests added for cross-module features
- [ ] No unwrap() in production code (use ? or expect in tests)
- [ ] Concurrency primitives used correctly (parking_lot, dashmap)

### Performance Considerations
- [ ] Avoid unnecessary clones (use references when possible)
- [ ] Use binary search for sorted collections
- [ ] Consider cache locality for hot paths
- [ ] Profile before optimizing (use criterion benchmarks)
- [ ] Use appropriate data structures (BTree vs HashMap)

### Security Considerations
- [ ] Input validation for external data
- [ ] Bounds checking for array/vector access
- [ ] Integer overflow checks for arithmetic
- [ ] Resource limits enforced (connections, memory)
- [ ] No unsafe code without thorough review
