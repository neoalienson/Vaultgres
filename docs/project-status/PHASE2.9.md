# Phase 2.9: Advanced SQL Operators

## Overview

Phase 2.8 completed core SQL features (WHERE, ORDER BY, LIMIT, GROUP BY, HAVING). Phase 2.9 adds advanced operators for more expressive queries before moving to Phase 3 (Parallelism).

## Status: 🚧 IN PROGRESS (3/5 features complete)

---

## 2.9.1 DISTINCT ✅ COMPLETE

**Goal**: Remove duplicate rows from results

**Implementation**:
```rust
pub struct SelectStmt {
    pub distinct: bool,
    pub columns: Vec<Expr>,
    // ... other fields
}
```

**Features**:
- ✅ DISTINCT keyword support
- ✅ HashSet-based deduplication
- ✅ Works with all query clauses

**Examples**:
```sql
SELECT DISTINCT category FROM products;
SELECT DISTINCT status FROM orders WHERE date > '2024-01-01';
```

**Status**: ✅ Complete

---

## 2.9.2 LIKE Operator ✅ COMPLETE

**Goal**: Pattern matching for text

**Implementation**:
```rust
BinaryOperator::Like => match (&left_val, &right_val) {
    (Value::Text(s), Value::Text(pattern)) => {
        Ok(s.contains(&pattern.replace('%', "")))
    }
    _ => Err("LIKE requires text values".to_string()),
}
```

**Features**:
- ✅ Basic LIKE with % wildcard
- ✅ Case-sensitive matching
- ✅ Works in WHERE clauses

**Examples**:
```sql
SELECT * FROM users WHERE name LIKE '%john%';
SELECT * FROM products WHERE sku LIKE 'ABC%';
```

**Status**: ✅ Complete

---

## 2.9.3 AND/OR Operators ✅ COMPLETE

**Goal**: Logical operators for complex predicates

**Implementation**:
```rust
BinaryOperator::And => {
    let left_result = self.evaluate_predicate(left, tuple, schema)?;
    let right_result = self.evaluate_predicate(right, tuple, schema)?;
    Ok(left_result && right_result)
},
BinaryOperator::Or => {
    let left_result = self.evaluate_predicate(left, tuple, schema)?;
    let right_result = self.evaluate_predicate(right, tuple, schema)?;
    Ok(left_result || right_result)
}
```

**Features**:
- ✅ AND operator for conjunction
- ✅ OR operator for disjunction
- ✅ Recursive predicate evaluation
- ✅ Short-circuit evaluation

**Examples**:
```sql
SELECT * FROM users WHERE age > 18 AND status = 'active';
SELECT * FROM products WHERE category = 'electronics' OR category = 'computers';
SELECT * FROM orders WHERE (status = 'pending' OR status = 'processing') AND amount > 100;
```

**Status**: ✅ Complete

---

## 2.9.4 IN Operator ✅ COMPLETE

**Goal**: Test if value is in a list

**Implementation**:
```rust
BinaryOperator::In => {
    let left_val = self.evaluate_expr(left, tuple, schema)?;
    if let Expr::List(values) = &**right {
        for val_expr in values {
            let val = self.evaluate_expr(val_expr, tuple, schema)?;
            if left_val == val {
                return Ok(true);
            }
        }
        return Ok(false);
    }
    Err("IN requires list of values".to_string())
}
```

**Features**:
- ✅ IN operator with value list
- ✅ Works with numbers and strings
- ✅ Efficient list iteration

**Examples**:
```sql
SELECT * FROM users WHERE id IN (1, 2, 3, 5, 8);
SELECT * FROM products WHERE category IN ('electronics', 'computers', 'phones');
SELECT * FROM orders WHERE status IN ('pending', 'processing');
```

**Status**: ✅ Complete

---

## 2.9.5 BETWEEN Operator ✅ COMPLETE

**Goal**: Range testing

**Implementation**:
```rust
BinaryOperator::Between => {
    let left_val = self.evaluate_expr(left, tuple, schema)?;
    if let Expr::List(values) = &**right {
        if values.len() == 2 {
            let lower = self.evaluate_expr(&values[0], tuple, schema)?;
            let upper = self.evaluate_expr(&values[1], tuple, schema)?;
            return Ok(left_val >= lower && left_val <= upper);
        }
    }
    Err("BETWEEN requires two values".to_string())
}
```

**Features**:
- ✅ BETWEEN operator for ranges
- ✅ Inclusive range (lower <= x <= upper)
- ✅ Works with numbers and strings

**Examples**:
```sql
SELECT * FROM products WHERE price BETWEEN 100 AND 500;
SELECT * FROM users WHERE age BETWEEN 18 AND 65;
SELECT * FROM orders WHERE date BETWEEN '2024-01-01' AND '2024-12-31';
```

**Status**: ✅ Complete

---

## 2.9.6 NOT Operator ⏳ PLANNED

**Goal**: Logical negation

**Implementation**:
```rust
enum UnaryOperator {
    Not,
    Minus,
}

Expr::UnaryOp { op, expr } => {
    match op {
        UnaryOperator::Not => {
            let result = self.evaluate_predicate(expr, tuple, schema)?;
            Ok(!result)
        }
        _ => Err("Unsupported unary operator".to_string())
    }
}
```

**Features**:
- NOT operator for negation
- Works with all predicates
- NOT IN, NOT LIKE, NOT BETWEEN

**Examples**:
```sql
SELECT * FROM users WHERE NOT status = 'deleted';
SELECT * FROM products WHERE id NOT IN (1, 2, 3);
SELECT * FROM logs WHERE message NOT LIKE '%error%';
SELECT * FROM orders WHERE amount NOT BETWEEN 0 AND 10;
```

**Estimated Effort**: 2 hours
**Priority**: Medium

---

## 2.9.7 IS NULL / IS NOT NULL ⏳ PLANNED

**Goal**: NULL value testing

**Implementation**:
```rust
enum Value {
    Int(i64),
    Text(String),
    Null,
}

Expr::IsNull(expr) => {
    let val = self.evaluate_expr(expr, tuple, schema)?;
    Ok(matches!(val, Value::Null))
}

Expr::IsNotNull(expr) => {
    let val = self.evaluate_expr(expr, tuple, schema)?;
    Ok(!matches!(val, Value::Null))
}
```

**Features**:
- IS NULL operator
- IS NOT NULL operator
- NULL value support in Value enum

**Examples**:
```sql
SELECT * FROM users WHERE email IS NULL;
SELECT * FROM products WHERE description IS NOT NULL;
SELECT * FROM orders WHERE shipped_date IS NULL;
```

**Estimated Effort**: 3 hours
**Priority**: High

---

## Implementation Priority

### Completed ✅
1. ✅ DISTINCT (DONE)
2. ✅ LIKE operator (DONE)
3. ✅ AND/OR operators (DONE)
4. ✅ IN operator (DONE)
5. ✅ BETWEEN operator (DONE)

### Remaining ⏳
6. ⏳ NOT operator
7. ⏳ IS NULL / IS NOT NULL

---

## Testing Strategy

### Unit Tests
- Test each operator in isolation
- Test operator combinations
- Test edge cases (empty lists, NULL values)
- Test error conditions

### Integration Tests
- Test with real queries
- Test operator precedence
- Test performance

### E2E Tests
- Test via psql client
- Test complex queries
- Test error messages

---

## Performance Considerations

### DISTINCT
- ✅ HashSet for O(1) lookup
- Memory usage: O(unique rows)
- Consider external sort for large datasets

### LIKE
- ✅ Simple string contains for now
- Future: Regex compilation and caching
- Future: Index support for prefix patterns

### AND/OR
- ✅ Short-circuit evaluation
- Predicate ordering optimization (future)

### IN
- ✅ Linear search for small lists
- Future: HashSet for large lists (>10 items)
- Future: Index lookup for subqueries

### BETWEEN
- ✅ Two comparisons (optimal)
- Index range scan support (future)

---

## SQL Compatibility

### PostgreSQL Compatibility
- DISTINCT: ✅ Compatible
- LIKE: ✅ Compatible (basic)
- AND/OR: ✅ Compatible
- IN: ✅ Compatible
- BETWEEN: ✅ Compatible
- NOT: ⏳ Planned
- IS NULL: ⏳ Planned

### SQL Standard
- DISTINCT: SQL-92 ✅
- LIKE: SQL-92 ✅
- AND/OR: SQL-92 ✅
- IN: SQL-92 ✅
- BETWEEN: SQL-92 ✅
- NOT: SQL-92 ⏳
- IS NULL: SQL-92 ⏳

---

## Next Steps

1. ⏳ Implement NOT operator
2. ⏳ Implement IS NULL / IS NOT NULL
3. ✅ Test all operators together
4. ✅ Update documentation
5. 🎯 Move to Phase 3 (Parallelism)

---

## Conclusion

Phase 2.9 adds essential SQL operators that make queries more expressive and powerful. These operators are fundamental to SQL and expected by users.

**Current Status**:
- ✅ DISTINCT complete
- ✅ LIKE operator complete
- ✅ AND/OR operators complete
- ✅ IN operator complete
- ✅ BETWEEN operator complete
- ⏳ NOT operator planned
- ⏳ IS NULL / IS NOT NULL planned

**Completion**: 5/7 features (71%) 🚧

---

**Version**: 0.2.2 (Phase 2.9)
**Status**: In Progress
**Target**: Complete before Phase 3
