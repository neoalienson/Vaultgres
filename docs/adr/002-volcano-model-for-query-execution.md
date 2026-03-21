# ADR 002: Use Volcano Model for Query Execution

## Status

Accepted

## Context

VaultGres requires a query execution engine that can:
- Execute SQL queries efficiently using an iterator-based model
- Support a wide variety of relational operators (scan, join, aggregate, sort, etc.)
- Enable query optimization with interchangeable operator implementations
- Provide a clean, composable architecture for adding new operators
- Support both row-based and vectorized execution paths

We need an execution model that balances simplicity, extensibility, and performance.

## Decision

We will implement the **Volcano Model** (also known as the Iterator Model) for query execution in VaultGres.

## Rationale

### Simple Iterator Interface

The Volcano model uses a uniform interface for all operators:
```rust
pub trait Executor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError>;
}
```

Each operator:
- Requests tuples from child operators via `next()`
- Processes tuples and produces output tuples
- Returns `None` when exhausted

This simplicity makes the model easy to understand, implement, and debug.

### Composable Operator Tree

Queries are represented as a tree of operators:
```
Projection
  └── HashJoin
      ├── TableScan (left)
      └── Filter
          └── IndexScan (right)
```

Benefits:
- **Modularity**: Each operator is independent and testable
- **Flexibility**: Operators can be swapped by the optimizer
- **Clarity**: Execution plan is explicit and inspectable

### Pull-Based Execution

The Volcano model uses pull-based (demand-driven) execution:
- Root operator pulls tuples on demand
- Each operator pulls from children as needed
- Natural backpressure prevents memory explosion

This contrasts with push-based models where operators push tuples to parents, requiring explicit buffering.

### Optimizer Integration

The Volcano model integrates naturally with query optimization:
- **Logical operators**: Represent relational algebra (Scan, Join, Project)
- **Physical operators**: Implement execution strategies (HashJoin, NestedLoopJoin, MergeJoin)
- **Cost-based selection**: Optimizer chooses physical operators based on statistics

Example optimization:
```
Logical Join
  ├── Cost: HashJoin (large table, equality condition)
  ├── Cost: NestedLoopJoin (small table)
  └── Cost: MergeJoin (sorted inputs)
```

### Extensibility

New operators can be added without modifying existing code:
- Implement the `Executor` trait
- Integrate with the planner
- No changes to other operators required

This open/closed design supports:
- Window functions
- Recursive CTEs
- Custom aggregation functions
- Specialized join algorithms

### Testing and Debugging

Each operator can be tested in isolation:
```rust
#[test]
fn test_hash_join_executor() {
    let build_side = MockExecutor::new(vec![tuple1, tuple2]);
    let probe_side = MockExecutor::new(vec![tuple3, tuple4]);
    let mut join = HashJoinExecutor::new(build_side, probe_side, condition);
    
    assert_eq!(join.next()?, Some(expected_tuple));
}
```

Benefits:
- Unit tests for individual operators
- Mock executors for testing
- Clear data flow for debugging

### Foundation for Advanced Features

The Volcano model provides a foundation for:
- **Parallel execution**: Operators can be parallelized with exchange operators
- **Vectorized execution**: Batch-based variant (Volcano-style but with vectors)
- **Adaptive execution**: Operators can switch strategies mid-query
- **Incremental execution**: Materialized view maintenance

## Consequences

### Positive

1. **Simplicity**: Easy to understand and implement
2. **Modularity**: Clean separation of concerns between operators
3. **Extensibility**: New operators added without modifying existing code
4. **Testability**: Each operator can be tested independently
5. **Optimizer-friendly**: Natural fit for cost-based optimization
6. **Debugging**: Clear execution flow with well-defined boundaries

### Negative

1. **Function call overhead**: Virtual function calls per tuple can impact performance
2. **Tuple-at-a-time**: Processing one tuple at a time limits optimization opportunities
3. **Cache inefficiency**: Poor CPU cache utilization compared to vectorized models
4. **Materialization**: Intermediate results may be materialized unnecessarily

### Mitigation Strategies

1. **Function call overhead**:
   - Use `#[inline]` hints for hot paths
   - Consider monomorphization with generics for critical operators
   - Profile and optimize hot paths

2. **Tuple-at-a-time limitations**:
   - Implement vectorized operators as an alternative execution path
   - Use batch-based `next_batch()` for columnar operations
   - Hybrid approach: Volcano for control, vectorized for compute

3. **Cache inefficiency**:
   - Implement prefetching in scan operators
   - Use columnar storage for analytical workloads
   - Consider tuple batching for hot paths

4. **Materialization**:
   - Implement pipelining where possible
   - Use late materialization for columnar access
   - Optimize operator fusion in the planner

## Alternatives Considered

### Vectorized Model (Columnar)

Process tuples in batches (vectors) rather than one at a time:
- **Pros**: Better CPU cache utilization, SIMD optimization, reduced function call overhead
- **Cons**: More complex implementation, buffering required, less flexible for row-based operations

**Decision**: Implement as an enhancement to Volcano model for analytical workloads.

### Push-Based Model

Operators push tuples to parent operators:
- **Pros**: Can be more efficient for certain operations (e.g., aggregations)
- **Cons**: Complex buffering, harder to reason about, less composable

**Decision**: Use pull-based Volcano as primary model; push-based can be used internally within specific operators.

### Dataflow Model

Operators connected by channels, processing streams:
- **Pros**: Natural parallelism, good for distributed execution
- **Cons**: Complex coordination, overhead for single-node execution

**Decision**: Use Volcano for single-node; consider dataflow for distributed extensions.

### Interpretation vs. Compilation

- **Interpretation (Volcano)**: Execute operator tree via virtual function calls
- **Compilation (JIT)**: Generate native code for query execution

**Decision**: Start with interpreted Volcano model; consider JIT compilation (e.g., via `cranelift`) for performance-critical workloads.

## Implementation Details

### Core Traits

```rust
// Executor trait - all operators implement this
pub trait Executor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError>;
}

// Operator trait - metadata and planning
pub trait Operator {
    fn schema(&self) -> &Schema;
    fn estimated_rows(&self) -> usize;
    fn statistics(&self) -> &OperatorStatistics;
}
```

### Executor Directory Structure

```
src/executor/
├── volcano/           # Volcano-style executors
│   ├── scan.rs        # TableScan, IndexScan
│   ├── join.rs        # HashJoin, NestedLoopJoin, MergeJoin
│   ├── aggregate.rs   # HashAggregate, StreamAggregate
│   ├── sort.rs        # Sort, TopN
│   ├── filter.rs      # Filter operator
│   ├── project.rs     # Projection operator
│   └── limit.rs       # Limit operator
├── operators/         # Core operator implementations
├── parallel/          # Parallel execution infrastructure
└── builtin/           # Built-in functions
```

### Example Operator Implementation

```rust
pub struct FilterExecutor {
    child: Box<dyn Executor>,
    predicate: Expression,
    schema: Schema,
}

impl FilterExecutor {
    pub fn new(child: Box<dyn Executor>, predicate: Expression) -> Self {
        let schema = child.schema().clone();
        Self { child, predicate, schema }
    }
}

impl Executor for FilterExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        while let Some(tuple) = self.child.next()? {
            if self.predicate.evaluate(&tuple)? {
                return Ok(Some(tuple));
            }
            // Tuple doesn't match, continue to next
        }
        Ok(None) // Child exhausted
    }
}
```

## Performance Considerations

### Hot Paths

The following paths require careful optimization:
1. `next()` calls in tight loops
2. Tuple construction and cloning
3. Expression evaluation
4. Hash table lookups in joins

### Optimization Techniques

- **Memory pooling**: Reuse tuple allocations
- **Expression compilation**: Pre-compile expressions where possible
- **Predicate pushdown**: Filter early to reduce tuple flow
- **Operator fusion**: Combine simple operators (Filter + Project)

### Benchmarking

Use Criterion for performance tracking:
```rust
criterion_group!(
    benches,
    bench_hash_join,
    bench_nested_loop_join,
    bench_aggregation,
);
```

## References

- Graefe, Goetz. "Implementing sorting in database systems." ACM Computing Surveys (2006)
- Graefe, Goetz. "Query evaluation techniques for large databases." ACM Computing Surveys (1993)
- `docs/developers/OPTIMIZER.md` - Query optimization strategies
- `docs/developers/ARCHITECTURE.md` - System architecture overview
- `src/executor/volcano/` - Volcano executor implementations

---

**Decision Date**: 2026-03-21  
**Last Updated**: 2026-03-21
