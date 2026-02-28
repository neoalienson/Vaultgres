# Project Structure

## Directory Organization

```
rustgres/
├── src/                    # Core source code
│   ├── catalog/           # Schema catalog and metadata management
│   │   ├── tests/         # Catalog-specific tests
│   │   ├── aggregation.rs
│   │   ├── catalog.rs
│   │   ├── persistence.rs
│   │   ├── predicate.rs
│   │   ├── schema.rs
│   │   ├── tuple.rs
│   │   └── value.rs
│   ├── executor/          # Query execution engine
│   │   ├── edge_tests.rs  # Edge case tests
│   │   ├── executor.rs
│   │   ├── filter.rs
│   │   ├── hash_agg.rs
│   │   ├── hash_join.rs
│   │   ├── nested_loop.rs
│   │   ├── project.rs
│   │   ├── seq_scan.rs
│   │   └── sort.rs
│   ├── optimizer/          # Query optimizer and cost model
│   │   ├── rules/         # Optimization rules
│   │   ├── cost.rs
│   │   ├── edge_tests.rs
│   │   ├── join_order.rs
│   │   ├── plan.rs
│   │   └── selectivity.rs
│   ├── parser/            # SQL parser and lexer
│   │   ├── parser/        # Parser submodule
│   │   ├── ast.rs
│   │   ├── lexer.rs
│   │   ├── parser.rs
│   │   └── parser_edge_tests.rs
│   ├── protocol/          # PostgreSQL wire protocol
│   │   ├── connection.rs
│   │   ├── edge_tests.rs
│   │   ├── message.rs
│   │   └── server.rs
│   ├── statistics/        # Statistics collection
│   │   ├── collector.rs
│   │   ├── edge_tests.rs
│   │   ├── histogram.rs
│   │   └── mod.rs
│   ├── storage/           # Storage engine
│   │   ├── btree.rs
│   │   ├── buffer_pool.rs
│   │   ├── disk.rs
│   │   ├── edge_tests.rs
│   │   ├── filesystem.rs
│   │   ├── heap.rs
│   │   └── page.rs
│   ├── transaction/       # Transaction manager and MVCC
│   │   ├── edge_tests.rs
│   │   ├── lock.rs
│   │   ├── manager.rs
│   │   ├── mvcc.rs
│   │   └── snapshot.rs
│   ├── wal/               # Write-ahead logging
│   │   ├── checkpoint.rs
│   │   ├── disk.rs
│   │   ├── edge_tests.rs
│   │   ├── recovery.rs
│   │   └── writer.rs
│   ├── config.rs          # Configuration management
│   ├── config_edge_tests.rs
│   ├── lib.rs             # Library entry point
│   └── main.rs            # Binary entry point
├── tests/                 # Test suites
│   ├── e2e/              # End-to-end shell scripts
│   │   ├── README.md     # E2E test guide
│   │   └── *.sh          # Shell test scripts
│   ├── integration/      # Integration tests
│   │   ├── buffer_pool_disk_test.rs
│   │   ├── catalog_test.rs
│   │   ├── executor_test.rs
│   │   ├── optimizer_test.rs
│   │   ├── parser_test.rs
│   │   ├── protocol_test.rs
│   │   ├── storage_test.rs
│   │   ├── transaction_test.rs
│   │   └── wal_test.rs
│   ├── unit/             # Unit tests
│   │   ├── config_test.rs
│   │   ├── lexer_test.rs
│   │   ├── page_test.rs
│   │   └── parser_test.rs
│   ├── README.md         # Test organization guide
│   ├── e2e_tests.rs      # E2E test runner
│   ├── integration_tests.rs
│   └── unit_tests.rs
├── benches/              # Performance benchmarks
│   └── storage_bench.rs
├── docs/                 # Comprehensive documentation
│   ├── ARCHITECTURE.md   # System architecture
│   ├── CONFIGURATION.md  # Configuration guide
│   ├── CONTRIBUTING.md   # Contribution guidelines
│   ├── INSTALLATION.md   # Installation instructions
│   ├── LOGGING.md        # Logging configuration
│   ├── OPTIMIZER.md      # Query optimizer details
│   ├── QUICKSTART.md     # Quick start guide
│   ├── ROADMAP.md        # Project roadmap
│   ├── SERVER.md         # Server operations
│   ├── SQL.md            # SQL reference
│   ├── STANDARDS.md      # Coding standards
│   ├── STORAGE.md        # Storage engine details
│   └── TRANSACTIONS.md   # Transaction management
├── data/                 # Runtime data directory
├── wal/                  # Write-ahead log files
├── target/               # Build artifacts
├── .amazonq/             # Amazon Q configuration
│   └── rules/
│       └── memory-bank/  # Memory bank documentation
├── config.yaml           # Default server configuration
├── config.dev.yaml       # Development configuration
├── config.prod.yaml      # Production configuration
├── Cargo.toml            # Rust package manifest
├── .rustfmt.toml         # Code formatting rules
├── .gitignore            # Git ignore patterns
└── README.md             # Project overview

```

## Core Components

### Storage Layer (`src/storage/`)
The foundation of the database, managing persistent data storage.

**Key Files**:
- `btree.rs` - B+Tree index implementation for efficient key-value lookups
- `buffer_pool.rs` - In-memory page cache with LRU eviction
- `page.rs` - Fixed-size page abstraction (8KB pages)
- `heap.rs` - Heap file storage for table data
- `disk.rs` - Low-level disk I/O operations
- `filesystem.rs` - File system abstraction layer

**Responsibilities**:
- Page-based storage management
- Buffer pool caching
- B+Tree index operations
- Disk I/O and file management

### Transaction Layer (`src/transaction/`)
Implements MVCC for concurrent transaction processing.

**Key Files**:
- `manager.rs` - Transaction lifecycle management
- `mvcc.rs` - Multi-version concurrency control implementation
- `snapshot.rs` - Snapshot isolation for read consistency
- `lock.rs` - Lock management for write conflicts

**Responsibilities**:
- Transaction begin/commit/abort
- MVCC version management
- Snapshot isolation
- Deadlock detection

### WAL Layer (`src/wal/`)
Write-ahead logging for durability and crash recovery.

**Key Files**:
- `writer.rs` - WAL record writing
- `recovery.rs` - Crash recovery and replay
- `checkpoint.rs` - Checkpoint management
- `disk.rs` - WAL-specific disk operations

**Responsibilities**:
- Log record generation
- Crash recovery
- Checkpoint coordination
- Log file management

### Parser Layer (`src/parser/`)
SQL parsing and AST generation.

**Key Files**:
- `lexer.rs` - Tokenization of SQL text
- `parser.rs` - Recursive descent parser
- `ast.rs` - Abstract syntax tree definitions

**Responsibilities**:
- SQL tokenization
- Syntax parsing
- AST construction
- Error reporting

### Optimizer Layer (`src/optimizer/`)
Query optimization and plan generation.

**Key Files**:
- `plan.rs` - Logical and physical plan representations
- `cost.rs` - Cost model for plan selection
- `join_order.rs` - Join order optimization
- `selectivity.rs` - Selectivity estimation
- `rules/` - Optimization rules

**Responsibilities**:
- Logical plan optimization
- Cost-based plan selection
- Join order optimization
- Statistics-based estimation

### Executor Layer (`src/executor/`)
Query execution engine with operator implementations.

**Key Files**:
- `executor.rs` - Main execution coordinator
- `seq_scan.rs` - Sequential scan operator
- `filter.rs` - Filter/selection operator
- `project.rs` - Projection operator
- `nested_loop.rs` - Nested loop join
- `hash_join.rs` - Hash join operator
- `hash_agg.rs` - Hash aggregation
- `sort.rs` - Sort operator

**Responsibilities**:
- Operator execution
- Tuple processing
- Join algorithms
- Aggregation

### Protocol Layer (`src/protocol/`)
PostgreSQL wire protocol for client communication.

**Key Files**:
- `server.rs` - Server socket and connection handling
- `connection.rs` - Per-connection state management
- `message.rs` - Protocol message encoding/decoding

**Responsibilities**:
- Client connection handling
- Protocol message parsing
- Authentication
- Query/response flow

### Catalog Layer (`src/catalog/`)
Schema metadata and system catalog.

**Key Files**:
- `catalog.rs` - Catalog management
- `schema.rs` - Schema definitions
- `tuple.rs` - Tuple representation
- `value.rs` - Value types
- `persistence.rs` - Catalog persistence

**Responsibilities**:
- Schema storage
- Table/column metadata
- Type system
- Catalog persistence

### Statistics Layer (`src/statistics/`)
Statistics collection for query optimization.

**Key Files**:
- `collector.rs` - Statistics gathering
- `histogram.rs` - Histogram generation
- `mod.rs` - Statistics API

**Responsibilities**:
- Table statistics
- Column histograms
- Cardinality estimation

## Architectural Patterns

### Layered Architecture
The system follows a strict layered architecture where each layer depends only on layers below it:
```
Protocol → Parser → Optimizer → Executor → Transaction → Storage
                                              ↓
                                            WAL
```

### Error Handling
- Each module defines its own error type using `thiserror`
- Errors propagate up through `Result<T, E>` types
- Edge cases tested in dedicated `edge_tests.rs` files

### Concurrency Model
- Lock-free data structures using `parking_lot` and `dashmap`
- MVCC for transaction isolation
- Async I/O planned for protocol layer

### Testing Strategy
- **Unit Tests**: In-module tests for individual functions
- **Integration Tests**: Cross-module tests in `tests/integration/`
- **E2E Tests**: Shell-based end-to-end tests in `tests/e2e/`
- **Edge Tests**: Dedicated edge case tests (`*_edge_tests.rs`)
- **Benchmarks**: Performance tests in `benches/`

## Module Relationships

```
┌─────────────────────────────────────────────────────────┐
│                    Protocol Layer                        │
│         (Client connections, authentication)             │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                   Parser Layer                           │
│            (SQL parsing, AST generation)                 │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                 Optimizer Layer                          │
│     (Cost-based optimization, plan generation)           │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                 Executor Layer                           │
│         (Operator execution, tuple processing)           │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│              Transaction Layer                           │
│         (MVCC, snapshots, lock management)               │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                Storage Layer                             │
│    (Buffer pool, B+Tree, pages, disk I/O)               │
└─────────────────────────────────────────────────────────┘
         │
         └──────────────────────┐
                                │
┌───────────────────────────────▼─────────────────────────┐
│                    WAL Layer                             │
│         (Write-ahead logging, recovery)                  │
└─────────────────────────────────────────────────────────┘
```

## Configuration Files

- `Cargo.toml` - Rust package manifest with dependencies
- `config.yaml` - Default server configuration
- `config.dev.yaml` - Development environment config
- `config.prod.yaml` - Production environment config
- `.rustfmt.toml` - Code formatting rules
- `.gitignore` - Git ignore patterns

## Documentation Structure

### Root Documentation
- `README.md` - Project overview, features, quick start

### docs/ Directory Structure

#### docs/users/ - End User Documentation
- `QUICKSTART.md` - Quick start tutorial
- `SQL.md` - SQL syntax and feature reference

#### docs/admins/ - Database Administrator Documentation
- `INSTALLATION.md` - Installation and setup instructions
- `CONFIGURATION.md` - Configuration options and tuning
- `CONFIG.md` - Configuration file reference
- `SERVER.md` - Server operations and management
- `LOGGING.md` - Logging configuration and best practices

#### docs/developers/ - Developer Documentation
- `ARCHITECTURE.md` - Detailed system architecture and design
- `CONTRIBUTING.md` - Contribution guidelines
- `STANDARDS.md` - Coding standards and conventions
- `STORAGE.md` - Storage engine implementation details
- `TRANSACTIONS.md` - Transaction management and MVCC
- `OPTIMIZER.md` - Query optimizer internals
- `IMPLEMENTATION_PLAN.md` - Implementation roadmap
- `ROADMAP.md` - Project roadmap and future plans
- `testing/` - Testing documentation
  - `TESTING.md` - Test organization and running instructions
  - `E2E.md` - E2E test guide and prerequisites
  - `E2E_TEST_FRAMEWORKS.md` - E2E testing framework details
  - `TEST_COVERAGE.md` - Test coverage statistics
  - `EDGE_TEST_SUMMARY.md` - Edge case test summary


### docs/project-status/ Directory
Project status and development history:
- `PROJECT_STATUS.md` - Current implementation status
- `PERSISTENCE_STATUS.md` - Persistence implementation status
- `PHASE2.8.md`, `PHASE2.9.md` - Phase-specific documentation
- `SESSION_SUMMARY.md` - Development session summaries
