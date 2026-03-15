# VaultGres Debugging Guide

This guide helps developers debug common issues in VaultGres, particularly around expression handling, column resolution, and planner-executor interaction.

## Quick Start

### Enable Debug Logging

```bash
# Run with debug logging
RUST_LOG=debug cargo run

# Run tests with debug output
RUST_LOG=debug cargo test -- --nocapture

# Filter specific modules
RUST_LOG=vaultgres::planner=debug,vaultgres::executor::volcano::project=debug cargo test
```

### Common Debug Scenarios

## 1. Column Name Issues

**Symptom**: `ColumnNotFound("QualifiedColumn { table: \"c\", column: \"name\" }")`

**Cause**: Expression was converted to column name using `Debug` format instead of proper extraction.

**How to Debug**:

1. Check `ProjectExecutor::new()` debug logs:
   ```
   ProjectExecutor::new with 3 columns
     [0] QualifiedColumn { table: "c", column: "name" } -> 'name'
     [1] QualifiedColumn { table: "o", column: "id" } -> 'id'
   ```

2. Look for warnings about suspicious names:
   ```
   WARN ProjectExecutor: Suspicious column name 'QualifiedColumn {...}' from expression ...
   ```

3. Check planner logs for expression conversion:
   ```
   Planner: Processing column expression: QualifiedColumn { table: "c", column: "name" }
   Planner: Converting QualifiedColumn to Column 'name'
   ```

**Prevention**: The integration tests in `planner_executor_integration_test.rs` catch this automatically.

## 2. Schema Mismatch Errors

**Symptom**: `InternalError("Projection column 'x' not found in schema. Available columns: [...]")`

**Cause**: Planner is trying to project a column that doesn't exist in the input schema.

**How to Debug**:

1. Check the validation error message - it lists available columns
2. Trace the schema through the planner:
   - What tables are in the FROM clause?
   - What columns does each table have?
   - Are JOINs correctly merging schemas?

**Prevention**: `validate_projection_schema()` catches this before executor creation.

## 3. Expression Handling Bugs

**Symptom**: Unexpected column names, type mismatches, or execution failures.

**How to Debug**:

1. Run the expression coverage test:
   ```bash
   cargo test test_column_name_invariants_for_all_expr_types
   ```

2. Check what `get_column_name()` produces for each expression type:
   - `Column("name")` → `"name"`
   - `QualifiedColumn { table: "t", column: "c" }` → `"c"`
   - `Alias { alias: "a", .. }` → `"a"`
   - `FunctionCall { name: "UPPER", .. }` → `"upper"`
   - `BinaryOp { .. }` → Generated name (not Debug format!)

**Key Invariants**:
- Column names should NEVER contain `{`, `}`, or type names like `QualifiedColumn`
- Column names should NEVER contain `Expr::` prefix
- Column names should be simple identifiers

## 4. JOIN and View Issues

**Symptom**: Views with JOINs fail with column not found errors.

**How to Debug**:

1. Check if qualified columns are being converted:
   ```
   Planner: Converting QualifiedColumn to Column 'name'
   ```

2. Verify the combined schema has all columns from joined tables

3. Run the view-specific integration test:
   ```bash
   cargo test test_view_with_qualified_columns_planning
   ```

**Prevention**: The planner automatically converts `QualifiedColumn` to `Column` for projections.

## Testing Tools

### Integration Tests

```bash
# Run all planner-executor integration tests
cargo test --test integration_tests planner_executor_integration

# Run specific test
cargo test --test integration_tests test_plan_qualified_columns_no_malformed_names
```

### Unit Tests

```bash
# Test expression handling
cargo test --lib executor::volcano::project::tests

# Test planner schema derivation
cargo test --lib planner::planner::tests
```

### Debug Helper

Use `ExecutionDebugger` (when implemented) to validate tuple structures:

```rust
let debugger = ExecutionDebugger::new("ProjectExecutor", &input, &output);
debugger.validate().expect("Execution validation failed");
```

## Logging Reference

| Module | Log Level | What It Shows |
|--------|-----------|---------------|
| `vaultgres::planner` | debug | Expression processing, column conversion |
| `vaultgres::executor::volcano::project` | debug | Column name mappings |
| `vaultgres::protocol::connection` | debug | Projection operations |

## Common Pitfalls

### 1. Using Debug Format for Column Names

❌ **Wrong**:
```rust
let col_name = format!("{:?}", expr);  // Produces "QualifiedColumn {...}"
```

✅ **Correct**:
```rust
let col_name = match expr {
    Expr::Column(name) => name.clone(),
    Expr::QualifiedColumn { column, .. } => column.clone(),
    // ...
};
```

### 2. Not Handling Qualified Columns

❌ **Wrong**:
```rust
match expr {
    Expr::Column(name) => { /* handle */ }
    // Missing QualifiedColumn case!
}
```

✅ **Correct**:
```rust
match expr {
    Expr::Column(name) => { /* handle */ }
    Expr::QualifiedColumn { column, .. } => { /* handle */ }
    // ...
}
```

### 3. Schema Not Updated After JOIN

❌ **Wrong**:
```rust
// After JOIN, schema still only has left table columns
let plan = ProjectExecutor::new(plan, columns);
```

✅ **Correct**:
```rust
// After JOIN, merge schemas
current_schema.columns.extend(right_schema.columns.clone());
let plan = ProjectExecutor::new(plan, columns);
```

## Checklist for New Executors

When adding a new executor that processes expressions:

- [ ] Handle all `Expr` variants (or document which are unsupported)
- [ ] Never use `format!("{:?}", expr)` for column names
- [ ] Add debug logging in constructor
- [ ] Add validation for column name invariants
- [ ] Write unit tests for each expression type
- [ ] Add integration test with planner

## Related Documentation

- [Architecture Overview](ARCHITECTURE.md)
- [Planner Design](PLANNER.md)
- [Executor Design](EXECUTOR.md)
- [Debugging Improvements Plan](DEBUGGING_IMPROVEMENTS_PLAN.md)

---

*Last updated: 2026-03-15*
