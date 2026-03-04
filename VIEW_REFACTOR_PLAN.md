# Plan to Refactor the Execution Engine for Compositional View Expansion

## 1. Introduction

The current execution engine is built around a monolithic `SelectExecutor` which handles all aspects of a `SELECT` query (filtering, projection, aggregation, etc.) in a single, large function. This architecture has several drawbacks:

- **Lack of Compositionality**: It is difficult to compose or nest query operations. This makes it extremely challenging to implement features like views, subqueries in the `FROM` clause, and complex query plans.
- **Maintainability**: The monolithic design is hard to understand, maintain, and extend. Adding new features or optimizing existing ones is a complex and error-prone process.
- **View Expansion Nightmare**: The immediate problem is the inability to support views. The current `catalog.get_table` logic cannot handle views, and there is no mechanism to "expand" a view into its underlying query.

The goal of this refactoring is to replace the monolithic `SelectExecutor` with a compositional, operator-based execution engine, commonly known as the Volcano model. This will not only solve the view expansion problem but also make the entire query execution engine more modular, maintainable, and extensible.

## 2. Proposed Architecture: A Compositional Executor (Volcano Model)

The core idea is to represent each logical operation in a SQL query as a physical operator that implements a common `Executor` trait. These operators can then be composed into a tree-like query plan.

### Executor Trait

We will define a new `Executor` trait that all query operators will implement.

```rust
pub trait Executor {
    /// Returns the next tuple from the operator's output.
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError>;
}
```

### Operator Implementations

Each SQL operation will be implemented as a struct that implements the `Executor` trait. Each operator will have one or more child executors from which it pulls tuples.

- **`SeqScanExecutor`**: Scans a table and produces tuples. This is a "leaf" operator with no children.
- **`FilterExecutor`**: Takes another `Executor` as input (its child) and filters tuples based on a predicate expression.
- **`ProjectExecutor`**: Takes another `Executor` as input and applies projection, transforming the input tuples into the desired output tuples.
- **`LimitExecutor`**: Takes another `Executor` as input and limits the number of tuples returned.
- **`SortExecutor`**: Takes another `Executor` as input and sorts the tuples based on a set of ordering expressions.
- **`HashJoinExecutor`**, **`NestedLoopJoinExecutor`**: Join operators that take two child executors as input and produce joined tuples.
- **`HashAggExecutor`**: An aggregation operator that takes a child executor and performs grouping and aggregation.

### Plan Tree

A query plan will be represented as a tree of these `Executor` structs. For example, the query `SELECT name FROM users WHERE age > 18` would be represented by the following plan tree:

```
ProjectExecutor(columns: [name])
  |
  +-- FilterExecutor(predicate: age > 18)
        |
        +-- SeqScanExecutor(table: users)
```

## 3. View Expansion Strategy

With the compositional Volcano model, view expansion becomes a natural part of query planning.

1.  **Public `get_view`**: The `catalog.get_view(view_name)` method will be made public so the planner can access it.
2.  **Planner/Optimizer Logic**: When the query planner encounters a view in the `FROM` clause of a query:
    a. It will call `catalog.get_view(view_name)` to retrieve the `SelectStmt` that defines the view.
    b. It will recursively call the planner on the view's `SelectStmt` to generate a sub-plan (a tree of `Executor`s).
    c. This sub-plan will be treated as a data source for the outer query. The planner will wrap the view's sub-plan in a "subquery scan" operator that will be a child of the outer query's operators.

For example, given a view `CREATE VIEW young_users AS SELECT * FROM users WHERE age < 30;` and a query `SELECT name FROM young_users;`, the planner will generate the following plan:

```
ProjectExecutor(columns: [name])
  |
  +-- SubqueryScanExecutor(sub_plan_for_young_users)
        |
        +-- [Sub-plan for young_users]
              |
              +-- FilterExecutor(predicate: age < 30)
                    |
                    +-- SeqScanExecutor(table: users)
```

## 4. `get_view` Accessibility

To enable the planner to expand views, the `get_view` method in `src/catalog/catalog.rs` must be made public.

```rust
// in src/catalog/catalog.rs
impl Catalog {
    // ...
    pub fn get_view(&self, name: &str) -> Option<SelectStmt> {
        self.views.read().unwrap().get(name).cloned()
    }
    // ...
}
```
This is a simple change that has a large impact on the system's ability to handle views.

## 5. Refactoring Steps

This is a significant architectural change and should be done in stages.

1.  **Step 1: Implement the `Executor` Trait and Basic Operators.**
    - Create a new `executor` module (e.g., `src/execution/volcano`).
    - Define the `Executor` trait.
    - Implement `SeqScanExecutor` and `ProjectExecutor` as the first basic operators. `ProjectExecutor` should be able to handle expressions using the `Eval` logic developed previously.

2.  **Step 2: Refactor `catalog.select` to Build and Execute a Plan Tree.**
    - Modify `catalog.select` to construct a simple plan tree using the new operators.
    - For a simple `SELECT a, b FROM t WHERE c > 10`, it will build a `ProjectExecutor(FilterExecutor(SeqScanExecutor(t)))`.
    - The `catalog.select` will then call `next()` on the root of the plan tree repeatedly to fetch all results.
    - The existing `SelectExecutor` will be marked as deprecated and gradually phased out.

3.  **Step 3: Integrate View Expansion into a Planner.**
    - Create a simple query planner (e.g., `src/planner/planner.rs`).
    - The planner will take a `SelectStmt` and build a plan tree.
    - Implement the view expansion logic in the planner as described in section 3.
    - `connection.rs` will call the planner to get a plan, and then execute it.

4.  **Step 4: Refactor Existing `SelectExecutor` Functionality into Individual Operators.**
    - Gradually migrate the functionality from the monolithic `SelectExecutor` into individual operator implementations.
    - Implement `FilterExecutor`, `LimitExecutor`, `SortExecutor`, `HashJoinExecutor`, `NestedLoopJoinExecutor`, `HashAggExecutor`, etc.
    - As each new operator is implemented, update the planner to use it.

## 6. Example: Before and After

**Query:**
```sql
CREATE VIEW active_users AS SELECT id, name FROM users WHERE status = 'active';
SELECT name FROM active_users WHERE id > 100;
```

**Before:**
- The query `SELECT name FROM active_users ...` fails.
- `catalog.select` is called with `table: "active_users"`.
- `catalog.get_table("active_users")` returns `None`, because `active_users` is a view, not a table.
- The query fails with "Table 'active_users' does not exist".

**After:**
1.  The planner is called with the `SelectStmt` for `SELECT name FROM active_users WHERE id > 100`.
2.  The planner sees `active_users` in the `FROM` clause and calls `catalog.get_view("active_users")`.
3.  The planner gets the view's `SelectStmt`: `SELECT id, name FROM users WHERE status = 'active'`.
4.  The planner recursively builds a sub-plan for the view:
    `ProjectExecutor([id, name], FilterExecutor(status='active', SeqScanExecutor(users)))`
5.  The planner builds the final plan for the outer query, using the view's sub-plan as a data source:
    ```
    ProjectExecutor(columns: [name])
      |
      +-- FilterExecutor(predicate: id > 100)
            |
            +-- SubqueryScanExecutor(sub_plan_for_active_users)
    ```
6.  The compositional executor executes this plan tree and returns the correct results.
