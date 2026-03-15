# Pet Store Test Refactoring

## Overview

The `pet_store.rs` test file has been split into modular components for better maintainability and readability.

## File Structure

```
tests/e2e/scenarios/
├── pet_store.rs          # Entry point (re-exports pet_store_tests module)
└── pet_store_tests/      # Test modules directory
    ├── mod.rs            # Module exports and main test function
    ├── setup.rs          # Schema creation and initial data setup
    ├── select_tests.rs   # SELECT edge case tests (20 tests)
    ├── insert_tests.rs   # INSERT edge case tests (12 tests)
    ├── update_tests.rs   # UPDATE tests (10 tests)
    ├── persistence_tests.rs  # Persistence/restart tests (4 scenarios)
    ├── edge_case_tests.rs    # Additional edge case tests (10 tests)
    ├── cleanup.rs        # Test cleanup
    └── README.md         # This file
```

## Test Categories

### Setup (`setup.rs`)
- Schema creation with indexes
- Initial data insertion
- Transaction and savepoint testing
- SCD Type 2 data setup
- View and materialized view creation

### SELECT Tests (`select_tests.rs`)
1. NULL value handling
2. DISTINCT
3. ORDER BY (ASC/DESC)
4. LIMIT and OFFSET
5. LIKE pattern matching
6. AND/OR conditions
7. BETWEEN
8. Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
9. GROUP BY
10. HAVING
11. CASE expressions
12. COALESCE
13. Nested subqueries
14. EXISTS
15. Multiple JOINs
16. Complex expressions
17. Column aliases
18. Table aliases

### INSERT Tests (`insert_tests.rs`)
1. Single row insertion
2. Multiple row insertion
3. NULL value handling
4. Special characters (quotes)
5. Large values
6. Negative values
7. INSERT with SELECT
8. Expressions in VALUES
9. CONCAT function
10. UPPER/LOWER functions
11. All columns insertion
12. Zero values

### UPDATE Tests (`update_tests.rs`)
1. Arithmetic operations (-, +, *)
2. Multiple column updates
3. Text WHERE conditions
4. Complex WHERE conditions
5. No matching rows
6. Inventory decrement
7. Loyalty points updates
8. CONCAT in SET clause

### Persistence Tests (`persistence_tests.rs`)
1. Graceful restart (SIGTERM)
2. Kill restart (SIGKILL - crash simulation)
3. Stop/Start restart
4. Multiple rapid restarts

### Edge Case Tests (`edge_case_tests.rs`)
1. UPDATE persistence after restart
2. NULL handling in UPDATE
3. CASE expressions in UPDATE
4. Subqueries in UPDATE
5. UPDATE all rows
6. Large values
7. Negative values
8. String concatenation
9. String functions (UPPER)
10. Complex multi-table verification

### Cleanup (`cleanup.rs`)
- Drops all test tables and views

## Main Test File

`pet_store.rs` - Main entry point that:
1. Initializes the test environment
2. Calls setup functions
3. Runs basic functionality tests
4. Runs all edge case test modules
5. Runs persistence tests
6. Performs final verification
7. Cleans up test data

## Benefits of Refactoring

1. **Maintainability**: Each test category is in its own file
2. **Readability**: Smaller, focused files are easier to understand
3. **Reusability**: Test functions can be called independently
4. **Parallel Development**: Multiple developers can work on different test categories
5. **Faster Compilation**: Only changed modules need recompilation
6. **Better Error Localization**: Easier to identify which test category has issues

## Running Tests

```bash
# Run the full pet_store test
cd tests/e2e
cargo test --test scenarios pet_store::test_pet_store_comprehensive

# Run with logging
RUST_LOG=debug cargo test --test scenarios pet_store::test_pet_store_comprehensive -- --nocapture
```

## Total Test Coverage

- **SELECT tests**: 20 scenarios
- **INSERT tests**: 12 scenarios
- **UPDATE tests**: 10 scenarios
- **Persistence tests**: 4 restart scenarios
- **Edge case tests**: 10 scenarios
- **Basic functionality**: 10+ scenarios

**Total: 66+ test scenarios** covering SQL operations, edge cases, and data persistence.
