# Query Optimizer

RustGres uses a cost-based query optimizer with rule-based transformations and dynamic programming for join ordering.

## Overview

The optimizer transforms SQL queries into efficient execution plans through:

1. **Logical Planning**: Convert AST to logical plan
2. **Rule Optimization**: Apply transformation rules
3. **Cost Estimation**: Estimate plan costs using statistics
4. **Physical Planning**: Generate executable physical plan

## Logical Planning

### Logical Plan Nodes

```rust
pub enum LogicalPlan {
    Scan { table: String, filter: Option<Expr> },
    Filter { input: Box<LogicalPlan>, predicate: Expr },
    Project { input: Box<LogicalPlan>, exprs: Vec<Expr> },
    Join { left: Box<LogicalPlan>, right: Box<LogicalPlan>, on: Expr, join_type: JoinType },
    Aggregate { input: Box<LogicalPlan>, group_by: Vec<Expr>, aggs: Vec<AggExpr> },
    Sort { input: Box<LogicalPlan>, order_by: Vec<SortExpr> },
    Limit { input: Box<LogicalPlan>, limit: usize, offset: usize },
}
```

### AST to Logical Plan

```rust
impl LogicalPlanner {
    pub fn plan(&self, stmt: SelectStmt) -> Result<LogicalPlan> {
        // 1. Plan FROM clause
        let mut plan = self.plan_from(stmt.from)?;
        
        // 2. Apply WHERE clause
        if let Some(filter) = stmt.where_clause {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: filter,
            };
        }
        
        // 3. Apply GROUP BY
        if !stmt.group_by.is_empty() {
            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by: stmt.group_by,
                aggs: stmt.aggregates,
            };
        }
        
        // 4. Apply SELECT (projection)
        plan = LogicalPlan::Project {
            input: Box::new(plan),
            exprs: stmt.select_list,
        };
        
        // 5. Apply ORDER BY
        if !stmt.order_by.is_empty() {
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by: stmt.order_by,
            };
        }
        
        // 6. Apply LIMIT/OFFSET
        if stmt.limit.is_some() || stmt.offset.is_some() {
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                limit: stmt.limit.unwrap_or(usize::MAX),
                offset: stmt.offset.unwrap_or(0),
            };
        }
        
        Ok(plan)
    }
}
```

## Rule-Based Optimization

### Transformation Rules

**Predicate Pushdown**:
```rust
pub fn pushdown_filter(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            match *input {
                // Push filter below projection
                LogicalPlan::Project { input, exprs } => {
                    let pushed = pushdown_filter(LogicalPlan::Filter {
                        input,
                        predicate,
                    });
                    LogicalPlan::Project {
                        input: Box::new(pushed),
                        exprs,
                    }
                }
                
                // Push filter into join
                LogicalPlan::Join { left, right, on, join_type } => {
                    let (left_preds, right_preds, join_preds) = 
                        split_predicate(predicate, &left, &right);
                    
                    let left = if !left_preds.is_empty() {
                        Box::new(LogicalPlan::Filter {
                            input: left,
                            predicate: combine_predicates(left_preds),
                        })
                    } else {
                        left
                    };
                    
                    let right = if !right_preds.is_empty() {
                        Box::new(LogicalPlan::Filter {
                            input: right,
                            predicate: combine_predicates(right_preds),
                        })
                    } else {
                        right
                    };
                    
                    let mut plan = LogicalPlan::Join {
                        left,
                        right,
                        on,
                        join_type,
                    };
                    
                    if !join_preds.is_empty() {
                        plan = LogicalPlan::Filter {
                            input: Box::new(plan),
                            predicate: combine_predicates(join_preds),
                        };
                    }
                    
                    plan
                }
                
                _ => LogicalPlan::Filter { input, predicate },
            }
        }
        _ => plan,
    }
}
```

**Projection Pruning**:
```rust
pub fn prune_columns(plan: LogicalPlan, required: &[String]) -> LogicalPlan {
    match plan {
        LogicalPlan::Project { input, exprs } => {
            // Only keep required columns
            let pruned_exprs: Vec<_> = exprs.into_iter()
                .filter(|e| required.contains(&e.name()))
                .collect();
            
            // Determine columns needed by input
            let input_required = pruned_exprs.iter()
                .flat_map(|e| e.column_refs())
                .collect();
            
            LogicalPlan::Project {
                input: Box::new(prune_columns(*input, &input_required)),
                exprs: pruned_exprs,
            }
        }
        _ => plan,
    }
}
```

**Constant Folding**:
```rust
pub fn fold_constants(expr: Expr) -> Expr {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let left = fold_constants(*left);
            let right = fold_constants(*right);
            
            match (left, right) {
                (Expr::Literal(l), Expr::Literal(r)) => {
                    // Evaluate at compile time
                    Expr::Literal(eval_binary_op(l, op, r))
                }
                (left, right) => Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                },
            }
        }
        _ => expr,
    }
}
```

**Common Subexpression Elimination**:
```rust
pub fn eliminate_cse(plan: LogicalPlan) -> LogicalPlan {
    let mut expr_map: HashMap<Expr, String> = HashMap::new();
    let mut counter = 0;
    
    fn rewrite_expr(expr: Expr, map: &mut HashMap<Expr, String>, counter: &mut usize) -> Expr {
        if let Some(alias) = map.get(&expr) {
            return Expr::Column(alias.clone());
        }
        
        if expr.is_complex() {
            let alias = format!("_cse_{}", counter);
            *counter += 1;
            map.insert(expr.clone(), alias.clone());
            return Expr::Column(alias);
        }
        
        expr
    }
    
    // Rewrite plan with CSE
    rewrite_plan(plan, &mut expr_map, &mut counter)
}
```

### Rule Application

```rust
pub struct RuleOptimizer {
    rules: Vec<Box<dyn OptimizationRule>>,
}

impl RuleOptimizer {
    pub fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        let mut optimized = plan;
        let mut changed = true;
        let mut iterations = 0;
        
        // Apply rules until fixed point or max iterations
        while changed && iterations < 10 {
            changed = false;
            
            for rule in &self.rules {
                let new_plan = rule.apply(optimized);
                if new_plan != optimized {
                    changed = true;
                    optimized = new_plan;
                }
            }
            
            iterations += 1;
        }
        
        optimized
    }
}
```

## Cost-Based Optimization

### Cost Model

```rust
pub struct Cost {
    pub startup_cost: f64,    // Cost before first tuple
    pub total_cost: f64,      // Cost for all tuples
    pub rows: f64,            // Estimated rows
}

pub struct CostFactors {
    pub seq_page_cost: f64,      // 1.0
    pub random_page_cost: f64,   // 4.0 (HDD), 1.1 (SSD)
    pub cpu_tuple_cost: f64,     // 0.01
    pub cpu_operator_cost: f64,  // 0.0025
    pub cpu_index_tuple_cost: f64, // 0.005
}
```

### Cardinality Estimation

**Table Scan**:
```rust
pub fn estimate_scan(&self, table: &Table, filter: Option<&Expr>) -> Cost {
    let rows = table.row_count as f64;
    let pages = table.page_count as f64;
    
    let selectivity = filter.map(|f| self.estimate_selectivity(f, table))
        .unwrap_or(1.0);
    
    Cost {
        startup_cost: 0.0,
        total_cost: pages * self.factors.seq_page_cost 
                  + rows * self.factors.cpu_tuple_cost,
        rows: rows * selectivity,
    }
}
```

**Index Scan**:
```rust
pub fn estimate_index_scan(&self, index: &Index, filter: &Expr) -> Cost {
    let selectivity = self.estimate_selectivity(filter, index.table);
    let rows = index.table.row_count as f64 * selectivity;
    
    // Cost to traverse index
    let index_pages = (rows * index.height as f64).max(1.0);
    let index_cost = index_pages * self.factors.random_page_cost
                   + rows * self.factors.cpu_index_tuple_cost;
    
    // Cost to fetch heap tuples
    let heap_pages = rows * (1.0 - index.correlation);
    let heap_cost = heap_pages * self.factors.random_page_cost
                  + rows * self.factors.cpu_tuple_cost;
    
    Cost {
        startup_cost: index.height as f64 * self.factors.random_page_cost,
        total_cost: index_cost + heap_cost,
        rows,
    }
}
```

**Join**:
```rust
pub fn estimate_join(&self, left: &Cost, right: &Cost, join_type: JoinType) -> Cost {
    match join_type {
        JoinType::Inner => {
            let rows = (left.rows * right.rows) / self.join_selectivity();
            Cost {
                startup_cost: left.total_cost + right.startup_cost,
                total_cost: left.total_cost + right.total_cost 
                          + rows * self.factors.cpu_operator_cost,
                rows,
            }
        }
        JoinType::Left => {
            Cost {
                startup_cost: left.total_cost + right.startup_cost,
                total_cost: left.total_cost + right.total_cost
                          + left.rows * self.factors.cpu_operator_cost,
                rows: left.rows,
            }
        }
        _ => todo!(),
    }
}
```

### Selectivity Estimation

**Equality Predicate**:
```rust
pub fn estimate_equality(&self, column: &Column, value: &Value) -> f64 {
    if let Some(stats) = self.stats.get(column) {
        // Use histogram if available
        if let Some(hist) = &stats.histogram {
            return hist.estimate_equality(value);
        }
        
        // Use distinct count
        if stats.n_distinct > 0.0 {
            return 1.0 / stats.n_distinct;
        }
    }
    
    // Default selectivity
    0.01
}
```

**Range Predicate**:
```rust
pub fn estimate_range(&self, column: &Column, op: CompareOp, value: &Value) -> f64 {
    if let Some(stats) = self.stats.get(column) {
        if let Some(hist) = &stats.histogram {
            return hist.estimate_range(op, value);
        }
        
        // Linear interpolation between min and max
        let range = stats.max - stats.min;
        let offset = value - stats.min;
        
        return match op {
            CompareOp::Lt | CompareOp::Le => offset / range,
            CompareOp::Gt | CompareOp::Ge => 1.0 - offset / range,
            _ => 0.5,
        };
    }
    
    // Default selectivity
    0.33
}
```

**Conjunction/Disjunction**:
```rust
pub fn estimate_and(&self, left: f64, right: f64) -> f64 {
    left * right  // Assume independence
}

pub fn estimate_or(&self, left: f64, right: f64) -> f64 {
    left + right - (left * right)
}
```

## Join Optimization

### Join Ordering

**Dynamic Programming**:
```rust
pub fn optimize_join_order(&self, relations: Vec<Relation>) -> LogicalPlan {
    let n = relations.len();
    let mut dp: HashMap<BitSet, (LogicalPlan, Cost)> = HashMap::new();
    
    // Base case: single relations
    for (i, rel) in relations.iter().enumerate() {
        let set = BitSet::from_iter([i]);
        let plan = LogicalPlan::Scan { table: rel.name.clone(), filter: None };
        let cost = self.estimate_scan(rel, None);
        dp.insert(set, (plan, cost));
    }
    
    // Build up larger join trees
    for size in 2..=n {
        for subset in BitSet::subsets_of_size(n, size) {
            let mut best_cost = Cost::infinity();
            let mut best_plan = None;
            
            // Try all possible splits
            for left_set in subset.subsets() {
                let right_set = subset.difference(&left_set);
                
                if let (Some((left_plan, left_cost)), Some((right_plan, right_cost))) = 
                    (dp.get(&left_set), dp.get(&right_set))
                {
                    let join_plan = LogicalPlan::Join {
                        left: Box::new(left_plan.clone()),
                        right: Box::new(right_plan.clone()),
                        on: self.find_join_condition(&left_set, &right_set),
                        join_type: JoinType::Inner,
                    };
                    
                    let join_cost = self.estimate_join(left_cost, right_cost, JoinType::Inner);
                    
                    if join_cost.total_cost < best_cost.total_cost {
                        best_cost = join_cost;
                        best_plan = Some(join_plan);
                    }
                }
            }
            
            if let Some(plan) = best_plan {
                dp.insert(subset, (plan, best_cost));
            }
        }
    }
    
    dp.get(&BitSet::all(n)).unwrap().0.clone()
}
```

**Greedy Join Ordering** (for large queries):
```rust
pub fn greedy_join_order(&self, relations: Vec<Relation>) -> LogicalPlan {
    let mut remaining: HashSet<_> = (0..relations.len()).collect();
    let mut current = None;
    
    while !remaining.is_empty() {
        let mut best_next = None;
        let mut best_cost = Cost::infinity();
        
        for &i in &remaining {
            let candidate = if let Some(ref left) = current {
                let right = LogicalPlan::Scan { 
                    table: relations[i].name.clone(), 
                    filter: None 
                };
                LogicalPlan::Join {
                    left: Box::new(left.clone()),
                    right: Box::new(right),
                    on: self.find_join_condition_any(left, &relations[i]),
                    join_type: JoinType::Inner,
                }
            } else {
                LogicalPlan::Scan { 
                    table: relations[i].name.clone(), 
                    filter: None 
                }
            };
            
            let cost = self.estimate_cost(&candidate);
            if cost.total_cost < best_cost.total_cost {
                best_cost = cost;
                best_next = Some((i, candidate));
            }
        }
        
        if let Some((i, plan)) = best_next {
            remaining.remove(&i);
            current = Some(plan);
        }
    }
    
    current.unwrap()
}
```

### Join Algorithms

**Nested Loop Join**:
```rust
pub fn cost_nested_loop(&self, left: &Cost, right: &Cost) -> Cost {
    Cost {
        startup_cost: left.startup_cost + right.startup_cost,
        total_cost: left.total_cost 
                  + left.rows * right.total_cost
                  + left.rows * right.rows * self.factors.cpu_operator_cost,
        rows: left.rows * right.rows * self.join_selectivity(),
    }
}
```

**Hash Join**:
```rust
pub fn cost_hash_join(&self, left: &Cost, right: &Cost) -> Cost {
    // Build hash table on smaller relation
    let (build, probe) = if left.rows < right.rows {
        (left, right)
    } else {
        (right, left)
    };
    
    let hash_table_size = build.rows * 100.0; // bytes per row
    let hash_cost = build.rows * self.factors.cpu_operator_cost;
    let probe_cost = probe.rows * self.factors.cpu_operator_cost;
    
    Cost {
        startup_cost: build.total_cost + hash_cost,
        total_cost: build.total_cost + probe.total_cost + hash_cost + probe_cost,
        rows: left.rows * right.rows * self.join_selectivity(),
    }
}
```

**Merge Join**:
```rust
pub fn cost_merge_join(&self, left: &Cost, right: &Cost, 
                       left_sorted: bool, right_sorted: bool) -> Cost {
    let mut cost = Cost {
        startup_cost: 0.0,
        total_cost: 0.0,
        rows: left.rows * right.rows * self.join_selectivity(),
    };
    
    // Add sort cost if needed
    if !left_sorted {
        let sort_cost = self.cost_sort(left);
        cost.startup_cost += sort_cost.total_cost;
    }
    
    if !right_sorted {
        let sort_cost = self.cost_sort(right);
        cost.startup_cost += sort_cost.total_cost;
    }
    
    // Merge cost
    cost.total_cost = cost.startup_cost 
                    + left.total_cost 
                    + right.total_cost
                    + (left.rows + right.rows) * self.factors.cpu_operator_cost;
    
    cost
}
```

## Statistics

### Table Statistics

```rust
pub struct TableStats {
    pub row_count: u64,
    pub page_count: u64,
    pub avg_row_size: u32,
    pub last_analyzed: SystemTime,
}
```

### Column Statistics

```rust
pub struct ColumnStats {
    pub n_distinct: f64,        // Number of distinct values
    pub null_frac: f64,         // Fraction of NULL values
    pub avg_width: u32,         // Average column width
    pub most_common_vals: Vec<Value>,
    pub most_common_freqs: Vec<f64>,
    pub histogram: Option<Histogram>,
}
```

### Histogram

```rust
pub struct Histogram {
    pub bounds: Vec<Value>,     // Bucket boundaries
    pub frequencies: Vec<f64>,  // Bucket frequencies
}

impl Histogram {
    pub fn estimate_equality(&self, value: &Value) -> f64 {
        let bucket = self.find_bucket(value);
        self.frequencies[bucket] / self.bounds.len() as f64
    }
    
    pub fn estimate_range(&self, op: CompareOp, value: &Value) -> f64 {
        let bucket = self.find_bucket(value);
        
        match op {
            CompareOp::Lt => {
                self.frequencies[..bucket].iter().sum::<f64>()
            }
            CompareOp::Le => {
                self.frequencies[..=bucket].iter().sum::<f64>()
            }
            _ => todo!(),
        }
    }
}
```

### ANALYZE Command

```sql
ANALYZE users;

-- Analyze specific columns
ANALYZE users (email, created_at);

-- Set statistics target (more buckets)
ALTER TABLE users ALTER COLUMN email SET STATISTICS 1000;
```

## Physical Planning

### Physical Plan Nodes

```rust
pub enum PhysicalPlan {
    SeqScan { table: String, filter: Option<Expr> },
    IndexScan { index: String, filter: Expr },
    NestedLoopJoin { left: Box<PhysicalPlan>, right: Box<PhysicalPlan>, on: Expr },
    HashJoin { left: Box<PhysicalPlan>, right: Box<PhysicalPlan>, on: Expr },
    MergeJoin { left: Box<PhysicalPlan>, right: Box<PhysicalPlan>, on: Expr },
    HashAggregate { input: Box<PhysicalPlan>, group_by: Vec<Expr>, aggs: Vec<AggExpr> },
    Sort { input: Box<PhysicalPlan>, order_by: Vec<SortExpr> },
    Limit { input: Box<PhysicalPlan>, limit: usize, offset: usize },
}
```

### Plan Selection

```rust
impl PhysicalPlanner {
    pub fn plan(&self, logical: LogicalPlan) -> PhysicalPlan {
        match logical {
            LogicalPlan::Scan { table, filter } => {
                self.plan_scan(&table, filter.as_ref())
            }
            LogicalPlan::Join { left, right, on, join_type } => {
                self.plan_join(*left, *right, on, join_type)
            }
            _ => todo!(),
        }
    }
    
    fn plan_scan(&self, table: &str, filter: Option<&Expr>) -> PhysicalPlan {
        let table_meta = self.catalog.get_table(table);
        
        // Try to use index
        if let Some(filter) = filter {
            if let Some(index) = self.find_best_index(table_meta, filter) {
                return PhysicalPlan::IndexScan {
                    index: index.name.clone(),
                    filter: filter.clone(),
                };
            }
        }
        
        // Fall back to sequential scan
        PhysicalPlan::SeqScan {
            table: table.to_string(),
            filter: filter.cloned(),
        }
    }
    
    fn plan_join(&self, left: LogicalPlan, right: LogicalPlan, 
                 on: Expr, join_type: JoinType) -> PhysicalPlan {
        let left_plan = self.plan(left);
        let right_plan = self.plan(right);
        
        let left_cost = self.estimate_cost(&left_plan);
        let right_cost = self.estimate_cost(&right_plan);
        
        // Choose join algorithm
        let hash_cost = self.cost_hash_join(&left_cost, &right_cost);
        let merge_cost = self.cost_merge_join(&left_cost, &right_cost, false, false);
        let nested_cost = self.cost_nested_loop(&left_cost, &right_cost);
        
        if hash_cost.total_cost < merge_cost.total_cost 
            && hash_cost.total_cost < nested_cost.total_cost {
            PhysicalPlan::HashJoin {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                on,
            }
        } else if merge_cost.total_cost < nested_cost.total_cost {
            PhysicalPlan::MergeJoin {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                on,
            }
        } else {
            PhysicalPlan::NestedLoopJoin {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                on,
            }
        }
    }
}
```

## EXPLAIN

### Query Plan Display

```sql
EXPLAIN SELECT * FROM users WHERE email = 'user@example.com';

-- Output:
-- Index Scan using idx_users_email on users  (cost=0.42..8.44 rows=1 width=40)
--   Index Cond: (email = 'user@example.com'::text)

EXPLAIN ANALYZE SELECT * FROM users JOIN orders ON users.id = orders.user_id;

-- Output:
-- Hash Join  (cost=15.50..45.75 rows=100 width=80) (actual time=0.123..0.456 rows=95 loops=1)
--   Hash Cond: (orders.user_id = users.id)
--   ->  Seq Scan on orders  (cost=0.00..25.00 rows=1000 width=40) (actual time=0.010..0.150 rows=1000 loops=1)
--   ->  Hash  (cost=10.00..10.00 rows=100 width=40) (actual time=0.100..0.100 rows=100 loops=1)
--         Buckets: 1024  Batches: 1  Memory Usage: 8kB
--         ->  Seq Scan on users  (cost=0.00..10.00 rows=100 width=40) (actual time=0.005..0.050 rows=100 loops=1)
-- Planning Time: 0.234 ms
-- Execution Time: 0.567 ms
```

## Configuration

```ini
# Planner cost constants
seq_page_cost = 1.0
random_page_cost = 4.0          # 1.1 for SSD
cpu_tuple_cost = 0.01
cpu_index_tuple_cost = 0.005
cpu_operator_cost = 0.0025

# Planner method configuration
enable_seqscan = on
enable_indexscan = on
enable_bitmapscan = on
enable_hashjoin = on
enable_mergejoin = on
enable_nestloop = on

# Genetic query optimizer
geqo = on
geqo_threshold = 12             # Use GEQO for 12+ tables
geqo_effort = 5
geqo_pool_size = 0
geqo_generations = 0

# Other planner options
default_statistics_target = 100
constraint_exclusion = partition
cursor_tuple_fraction = 0.1
from_collapse_limit = 8
join_collapse_limit = 8
```
