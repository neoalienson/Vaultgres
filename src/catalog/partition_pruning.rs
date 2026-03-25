use crate::catalog::Value;
use crate::parser::ast::{
    BinaryOperator, Expr, PartitionHashBound, PartitionKey, PartitionListBound, PartitionRangeBound,
};
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq)]
pub enum PartitionPredicate {
    Equals(String, Value),
    LessThan(String, Value),
    LessThanOrEqual(String, Value),
    GreaterThan(String, Value),
    GreaterThanOrEqual(String, Value),
    In(String, Vec<Value>),
    Between(String, Value, Value),
    And(Vec<PartitionPredicate>),
    Or(Vec<PartitionPredicate>),
    AlwaysTrue,
    AlwaysFalse,
    Unknown,
}

impl PartitionPredicate {
    fn invert_for_lower_bound(&self) -> Option<(String, Value)> {
        match self {
            PartitionPredicate::GreaterThan(col, val) => Some((col.clone(), val.clone())),
            PartitionPredicate::GreaterThanOrEqual(col, val) => {
                Some((col.clone(), increment_value(val)?))
            }
            _ => None,
        }
    }

    fn invert_for_upper_bound(&self) -> Option<(String, Value)> {
        match self {
            PartitionPredicate::LessThan(col, val) => Some((col.clone(), decrement_value(val)?)),
            PartitionPredicate::LessThanOrEqual(col, val) => Some((col.clone(), val.clone())),
            _ => None,
        }
    }
}

fn increment_value(val: &Value) -> Option<Value> {
    match val {
        Value::Int(n) => Some(Value::Int(n + 1)),
        Value::Float(f) => Some(Value::Float(f + 1.0)),
        _ => None,
    }
}

fn decrement_value(val: &Value) -> Option<Value> {
    match val {
        Value::Int(n) => Some(Value::Int(n - 1)),
        Value::Float(f) => Some(Value::Float(f - 1.0)),
        _ => None,
    }
}

pub struct PartitionPruner;

impl PartitionPruner {
    pub fn extract_predicates(
        expr: &Option<Expr>,
        partition_keys: &[PartitionKey],
    ) -> Vec<PartitionPredicate> {
        let mut predicates = Vec::new();
        if let Some(expr) = expr {
            let extracted = Self::extract_from_expr(expr, partition_keys);
            predicates.push(extracted);
        }
        predicates
    }

    fn extract_from_expr(expr: &Expr, partition_keys: &[PartitionKey]) -> PartitionPredicate {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                if *op == BinaryOperator::And {
                    let left_preds = Self::extract_from_expr(left, partition_keys);
                    let right_preds = Self::extract_from_expr(right, partition_keys);
                    match (left_preds, right_preds) {
                        (PartitionPredicate::And(mut l), PartitionPredicate::And(mut r)) => {
                            l.append(&mut r);
                            PartitionPredicate::And(l)
                        }
                        (PartitionPredicate::And(mut l), r) => {
                            l.push(r);
                            PartitionPredicate::And(l)
                        }
                        (l, PartitionPredicate::And(mut r)) => {
                            r.push(l);
                            PartitionPredicate::And(r)
                        }
                        (l, r) => PartitionPredicate::And(vec![l, r]),
                    }
                } else if *op == BinaryOperator::Or {
                    let left_preds = Self::extract_from_expr(left, partition_keys);
                    let right_preds = Self::extract_from_expr(right, partition_keys);
                    match (left_preds, right_preds) {
                        (PartitionPredicate::Or(mut l), PartitionPredicate::Or(mut r)) => {
                            l.append(&mut r);
                            PartitionPredicate::Or(l)
                        }
                        (PartitionPredicate::Or(mut l), r) => {
                            l.push(r);
                            PartitionPredicate::Or(l)
                        }
                        (l, PartitionPredicate::Or(mut r)) => {
                            r.push(l);
                            PartitionPredicate::Or(r)
                        }
                        (l, r) => PartitionPredicate::Or(vec![l, r]),
                    }
                } else {
                    Self::extract_binary_op(left, op, right, partition_keys)
                }
            }
            Expr::IsNull(inner) => {
                if let Expr::Column(name) = inner.as_ref() {
                    if Self::is_partition_key(name, partition_keys) {
                        return PartitionPredicate::And(vec![
                            PartitionPredicate::GreaterThanOrEqual(name.clone(), Value::Null),
                            PartitionPredicate::LessThan(name.clone(), Value::Null),
                        ]);
                    }
                }
                PartitionPredicate::Unknown
            }
            Expr::IsNotNull(inner) => {
                if let Expr::Column(name) = inner.as_ref() {
                    if Self::is_partition_key(name, partition_keys) {
                        return PartitionPredicate::AlwaysTrue;
                    }
                }
                PartitionPredicate::Unknown
            }
            _ => PartitionPredicate::Unknown,
        }
    }

    fn extract_binary_op(
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
        partition_keys: &[PartitionKey],
    ) -> PartitionPredicate {
        let (col, val, swapped) = match (left, right) {
            (Expr::Column(name), val) if Self::is_partition_key(name, partition_keys) => {
                (name.clone(), val, false)
            }
            (val, Expr::Column(name)) if Self::is_partition_key(name, partition_keys) => {
                (name.clone(), val, true)
            }
            _ => return PartitionPredicate::Unknown,
        };

        if let Some(const_val) = Self::evaluate_to_value(val) {
            let predicate = match op {
                BinaryOperator::Equals => PartitionPredicate::Equals(col.clone(), const_val),
                BinaryOperator::NotEquals => return PartitionPredicate::AlwaysTrue,
                BinaryOperator::LessThan => {
                    if swapped {
                        PartitionPredicate::GreaterThan(col.clone(), const_val)
                    } else {
                        PartitionPredicate::LessThan(col.clone(), const_val)
                    }
                }
                BinaryOperator::LessThanOrEqual => {
                    if swapped {
                        PartitionPredicate::GreaterThanOrEqual(col.clone(), const_val)
                    } else {
                        PartitionPredicate::LessThanOrEqual(col.clone(), const_val)
                    }
                }
                BinaryOperator::GreaterThan => {
                    if swapped {
                        PartitionPredicate::LessThan(col.clone(), const_val)
                    } else {
                        PartitionPredicate::GreaterThan(col.clone(), const_val)
                    }
                }
                BinaryOperator::GreaterThanOrEqual => {
                    if swapped {
                        PartitionPredicate::LessThanOrEqual(col.clone(), const_val)
                    } else {
                        PartitionPredicate::GreaterThanOrEqual(col.clone(), const_val)
                    }
                }
                BinaryOperator::In => {
                    if let Expr::List(values) = val {
                        let mut evaluated = Vec::new();
                        for v in values {
                            if let Some(ev) = Self::evaluate_to_value(v) {
                                evaluated.push(ev);
                            } else {
                                return PartitionPredicate::Unknown;
                            }
                        }
                        PartitionPredicate::In(col.clone(), evaluated)
                    } else {
                        PartitionPredicate::Unknown
                    }
                }
                _ => PartitionPredicate::Unknown,
            };
            return predicate;
        }

        PartitionPredicate::Unknown
    }

    fn is_partition_key(col: &str, partition_keys: &[PartitionKey]) -> bool {
        partition_keys.iter().any(|pk| &pk.column == col)
    }

    fn evaluate_to_value(expr: &Expr) -> Option<Value> {
        match expr {
            Expr::Number(n) => Some(Value::Int(*n)),
            Expr::String(s) => Some(Value::Text(s.clone())),
            Expr::Float(f) => Some(Value::Float(*f)),
            Expr::Null => Some(Value::Null),
            Expr::Parameter(_) => None,
            _ => None,
        }
    }

    pub fn prune_partitions_range(
        partitions: &[(String, PartitionRangeBound)],
        predicates: &[PartitionPredicate],
    ) -> Vec<String> {
        if partitions.is_empty() {
            return Vec::new();
        }

        if predicates.is_empty()
            || predicates.iter().all(|p| matches!(p, PartitionPredicate::AlwaysTrue))
        {
            return partitions.iter().map(|(name, _)| name.clone()).collect();
        }

        let mut indices: Vec<usize> = (0..partitions.len()).collect();

        for pred in predicates {
            indices = Self::filter_partitions_by_predicate(partitions, &indices, pred);
        }

        if indices.is_empty() {
            partitions.iter().map(|(name, _)| name.clone()).collect()
        } else {
            indices.into_iter().map(|idx| partitions[idx].0.clone()).collect()
        }
    }

    fn filter_partitions_by_predicate(
        partitions: &[(String, PartitionRangeBound)],
        indices: &[usize],
        pred: &PartitionPredicate,
    ) -> Vec<usize> {
        match pred {
            PartitionPredicate::Equals(_, val) => indices
                .iter()
                .filter(|&idx| {
                    let (_, bound) = &partitions[*idx];
                    let from_val = Self::extract_value_from_expr(&bound.from_values, 0);
                    let to_val = Self::extract_value_from_expr(&bound.to_values, 0);
                    match (from_val, to_val) {
                        (Some(fv), Some(tv)) => {
                            compare_values(val, &fv) != Some(std::cmp::Ordering::Less)
                                && compare_values(val, &tv) == Some(std::cmp::Ordering::Less)
                        }
                        _ => true,
                    }
                })
                .copied()
                .collect(),
            PartitionPredicate::GreaterThan(_, val) => indices
                .iter()
                .filter(|&idx| {
                    let (_, bound) = &partitions[*idx];
                    let to_val = Self::extract_value_from_expr(&bound.to_values, 0);
                    match to_val {
                        Some(tv) => compare_values(val, &tv) == Some(std::cmp::Ordering::Less),
                        None => true,
                    }
                })
                .copied()
                .collect(),
            PartitionPredicate::GreaterThanOrEqual(_, val) => indices
                .iter()
                .filter(|&idx| {
                    let (_, bound) = &partitions[*idx];
                    let to_val = Self::extract_value_from_expr(&bound.to_values, 0);
                    match to_val {
                        Some(tv) => compare_values(val, &tv) != Some(std::cmp::Ordering::Greater),
                        None => true,
                    }
                })
                .copied()
                .collect(),
            PartitionPredicate::LessThan(_, val) => indices
                .iter()
                .filter(|&idx| {
                    let (_, bound) = &partitions[*idx];
                    let from_val = Self::extract_value_from_expr(&bound.from_values, 0);
                    match from_val {
                        Some(fv) => compare_values(val, &fv) == Some(std::cmp::Ordering::Greater),
                        None => true,
                    }
                })
                .copied()
                .collect(),
            PartitionPredicate::LessThanOrEqual(_, val) => indices
                .iter()
                .filter(|&idx| {
                    let (_, bound) = &partitions[*idx];
                    let from_val = Self::extract_value_from_expr(&bound.from_values, 0);
                    match from_val {
                        Some(fv) => compare_values(val, &fv) != Some(std::cmp::Ordering::Less),
                        None => true,
                    }
                })
                .copied()
                .collect(),
            PartitionPredicate::In(_, values) => {
                if values.is_empty() {
                    return indices.to_vec();
                }
                let min_val: Option<Value> = values
                    .iter()
                    .filter_map(|v| match v {
                        Value::Int(n) => Some(v.clone()),
                        _ => None,
                    })
                    .min_by(|a, b| compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal));
                let max_val: Option<Value> = values
                    .iter()
                    .filter_map(|v| match v {
                        Value::Int(n) => Some(v.clone()),
                        _ => None,
                    })
                    .max_by(|a, b| compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal));
                indices
                    .iter()
                    .filter(|&idx| {
                        let (_, bound) = &partitions[*idx];
                        let from_val = Self::extract_value_from_expr(&bound.from_values, 0);
                        let to_val = Self::extract_value_from_expr(&bound.to_values, 0);
                        match (&from_val, &to_val, &min_val, &max_val) {
                            (Some(fv), Some(tv), Some(min_v), Some(max_v)) => {
                                compare_values(max_v, fv) != Some(std::cmp::Ordering::Less)
                                    && compare_values(min_v, tv) == Some(std::cmp::Ordering::Less)
                            }
                            _ => true,
                        }
                    })
                    .copied()
                    .collect()
            }
            PartitionPredicate::Between(_, lower, upper) => indices
                .iter()
                .filter(|&idx| {
                    let (_, bound) = &partitions[*idx];
                    let from_val = Self::extract_value_from_expr(&bound.from_values, 0);
                    let to_val = Self::extract_value_from_expr(&bound.to_values, 0);
                    match (from_val, to_val) {
                        (Some(fv), Some(tv)) => {
                            compare_values(lower, &tv) == Some(std::cmp::Ordering::Less)
                                && compare_values(upper, &fv) != Some(std::cmp::Ordering::Less)
                        }
                        _ => true,
                    }
                })
                .copied()
                .collect(),
            PartitionPredicate::And(preds) => {
                let mut result = indices.to_vec();
                for p in preds {
                    result = Self::filter_partitions_by_predicate(partitions, &result, p);
                }
                result
            }
            PartitionPredicate::Or(_) => indices.to_vec(),
            PartitionPredicate::AlwaysTrue => indices.to_vec(),
            PartitionPredicate::AlwaysFalse => vec![],
            PartitionPredicate::Unknown => indices.to_vec(),
        }
    }

    fn range_bound_matches_predicate(
        pred: &PartitionPredicate,
    ) -> Option<(Option<(String, Value)>, Option<(String, Value)>)> {
        match pred {
            PartitionPredicate::Equals(col, val) => {
                Some((Some((col.clone(), val.clone())), Some((col.clone(), val.clone()))))
            }
            PartitionPredicate::GreaterThan(col, val) => {
                Some((Some((col.clone(), val.clone())), None))
            }
            PartitionPredicate::GreaterThanOrEqual(col, val) => {
                let incremented = increment_value(val)?;
                Some((Some((col.clone(), incremented)), None))
            }
            PartitionPredicate::LessThan(col, val) => {
                let decremented = decrement_value(val)?;
                Some((None, Some((col.clone(), decremented))))
            }
            PartitionPredicate::LessThanOrEqual(col, val) => {
                Some((None, Some((col.clone(), val.clone()))))
            }
            PartitionPredicate::Between(col, lower, upper) => {
                Some((Some((col.clone(), lower.clone())), Some((col.clone(), upper.clone()))))
            }
            PartitionPredicate::And(preds) => {
                let mut lower: Option<(String, Value)> = None;
                let mut upper: Option<(String, Value)> = None;

                for p in preds {
                    if let Some((Some((col, val)), None)) =
                        Some(Self::range_bound_matches_predicate(p)?)
                    {
                        lower = match &lower {
                            None => Some((col, val)),
                            Some((_, existing)) => Some((col.clone(), max_value(existing, &val)?)),
                        };
                    }
                    if let Some((None, Some((col, val)))) =
                        Some(Self::range_bound_matches_predicate(p)?)
                    {
                        upper = match &upper {
                            None => Some((col, val)),
                            Some((_, existing)) => Some((col.clone(), min_value(existing, &val)?)),
                        };
                    }
                }

                Some((lower, upper))
            }
            PartitionPredicate::Or(_) => None,
            PartitionPredicate::AlwaysTrue => None,
            PartitionPredicate::AlwaysFalse => Some((None, None)),
            PartitionPredicate::Unknown => None,
            PartitionPredicate::In(col, values) => {
                let min_val = values
                    .iter()
                    .filter_map(|v| match v {
                        Value::Int(n) => Some(*n),
                        _ => None,
                    })
                    .min()
                    .map(Value::Int);
                let max_val = values
                    .iter()
                    .filter_map(|v| match v {
                        Value::Int(n) => Some(*n),
                        _ => None,
                    })
                    .max()
                    .map(Value::Int);
                Some((min_val.map(|v| (col.clone(), v)), max_val.map(|v| (col.clone(), v))))
            }
        }
    }

    pub fn prune_partitions_list(
        partitions: &[(String, PartitionListBound)],
        predicates: &[PartitionPredicate],
    ) -> Vec<String> {
        if partitions.is_empty() {
            return Vec::new();
        }

        if predicates.is_empty()
            || predicates.iter().all(|p| matches!(p, PartitionPredicate::AlwaysTrue))
        {
            return partitions.iter().map(|(name, _)| name.clone()).collect();
        }

        let mut matching_partitions: HashSet<String> = HashSet::new();

        for pred in predicates {
            match pred {
                PartitionPredicate::Equals(col, val) => {
                    for (name, bound) in partitions {
                        for v in &bound.values {
                            if let Some(PartitionPredicate::Equals(_, bound_val)) =
                                Self::extract_single_value(v)
                            {
                                if bound_val == *val {
                                    matching_partitions.insert(name.clone());
                                }
                            }
                        }
                    }
                }
                PartitionPredicate::In(col, values) => {
                    let value_set: HashSet<&Value> = values.iter().collect();
                    for (name, bound) in partitions {
                        for v in &bound.values {
                            if let Some(PartitionPredicate::Equals(_, bound_val)) =
                                Self::extract_single_value(v)
                            {
                                if value_set.contains(&bound_val) {
                                    matching_partitions.insert(name.clone());
                                }
                            }
                        }
                    }
                }
                PartitionPredicate::AlwaysTrue => {
                    return partitions.iter().map(|(name, _)| name.clone()).collect();
                }
                _ => {
                    return partitions.iter().map(|(name, _)| name.clone()).collect();
                }
            }
        }

        if matching_partitions.is_empty() {
            partitions.iter().map(|(name, _)| name.clone()).collect()
        } else {
            matching_partitions.into_iter().collect()
        }
    }

    pub fn prune_partitions_hash(
        partitions: &[(String, PartitionHashBound)],
        predicates: &[PartitionPredicate],
    ) -> Vec<String> {
        if partitions.is_empty() {
            return Vec::new();
        }

        if predicates.is_empty()
            || predicates.iter().all(|p| matches!(p, PartitionPredicate::AlwaysTrue))
        {
            return partitions.iter().map(|(name, _)| name.clone()).collect();
        }

        for pred in predicates {
            match pred {
                PartitionPredicate::Equals(col, val) => {
                    let hash_val = hash_value(val);
                    let matching: Vec<String> = partitions
                        .iter()
                        .filter(|(_, bound)| {
                            bound.modulus > 0 && hash_val % bound.modulus == bound.remainder
                        })
                        .map(|(name, _)| name.clone())
                        .collect();
                    if !matching.is_empty() {
                        return matching;
                    }
                }
                PartitionPredicate::In(col, values) => {
                    let mut matching_set: HashSet<String> = HashSet::new();
                    for val in values {
                        let hash_val = hash_value(val);
                        for (name, bound) in partitions {
                            if bound.modulus > 0 && hash_val % bound.modulus == bound.remainder {
                                matching_set.insert(name.clone());
                            }
                        }
                    }
                    if !matching_set.is_empty() {
                        return matching_set.into_iter().collect();
                    }
                }
                PartitionPredicate::AlwaysTrue => {
                    return partitions.iter().map(|(name, _)| name.clone()).collect();
                }
                _ => {}
            }
        }

        partitions.iter().map(|(name, _)| name.clone()).collect()
    }

    fn extract_single_value(expr: &Expr) -> Option<PartitionPredicate> {
        match expr {
            Expr::Number(n) => Some(PartitionPredicate::Equals(String::new(), Value::Int(*n))),
            Expr::String(s) => {
                Some(PartitionPredicate::Equals(String::new(), Value::Text(s.clone())))
            }
            Expr::Null => Some(PartitionPredicate::Equals(String::new(), Value::Null)),
            _ => None,
        }
    }

    fn extract_value_from_expr(exprs: &[Expr], idx: usize) -> Option<Value> {
        exprs.get(idx).and_then(|e| Self::evaluate_to_value(e))
    }
}

fn compare_values(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Int(ai), Value::Int(bi)) => Some(ai.cmp(bi)),
        (Value::Float(af), Value::Float(bf)) => af.partial_cmp(bf),
        (Value::Text(at), Value::Text(bt)) => Some(at.cmp(bt)),
        _ => None,
    }
}

fn max_value(a: &Value, b: &Value) -> Option<Value> {
    match (a, b) {
        (Value::Int(ai), Value::Int(bi)) => Some(Value::Int(*ai.max(bi))),
        (Value::Float(af), Value::Float(bf)) => Some(Value::Float(af.max(*bf))),
        _ => None,
    }
}

fn min_value(a: &Value, b: &Value) -> Option<Value> {
    match (a, b) {
        (Value::Int(ai), Value::Int(bi)) => Some(Value::Int(*ai.min(bi))),
        (Value::Float(af), Value::Float(bf)) => Some(Value::Float(af.min(*bf))),
        _ => None,
    }
}

fn hash_value(val: &Value) -> u64 {
    match val {
        Value::Int(n) => (*n as u64).wrapping_mul(31),
        Value::Text(s) => {
            let mut h: u64 = 0;
            for c in s.bytes() {
                h = h.wrapping_mul(31).wrapping_add(c as u64);
            }
            h
        }
        _ => 0,
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{DataType, PartitionKey};

    fn make_partition_keys() -> Vec<PartitionKey> {
        vec![PartitionKey { column: "date_col".to_string(), opclass: None }]
    }

    fn make_partition_keys_int() -> Vec<PartitionKey> {
        vec![PartitionKey { column: "id".to_string(), opclass: None }]
    }

    #[test]
    fn test_extract_equals_predicate() {
        let keys = make_partition_keys();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("date_col".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(100)),
        };
        let pred = PartitionPruner::extract_from_expr(&expr, &keys);
        assert!(matches!(pred, PartitionPredicate::Equals(_, Value::Int(100))));
    }

    #[test]
    fn test_extract_greater_than_predicate() {
        let keys = make_partition_keys();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("date_col".to_string())),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expr::Number(50)),
        };
        let pred = PartitionPruner::extract_from_expr(&expr, &keys);
        assert!(matches!(pred, PartitionPredicate::GreaterThan(_, Value::Int(50))));
    }

    #[test]
    fn test_extract_less_than_predicate() {
        let keys = make_partition_keys();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("date_col".to_string())),
            op: BinaryOperator::LessThan,
            right: Box::new(Expr::Number(200)),
        };
        let pred = PartitionPruner::extract_from_expr(&expr, &keys);
        assert!(matches!(pred, PartitionPredicate::LessThan(_, Value::Int(200))));
    }

    #[test]
    fn test_extract_between_predicate() {
        let keys = make_partition_keys();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("date_col".to_string())),
            op: BinaryOperator::GreaterThanOrEqual,
            right: Box::new(Expr::Number(100)),
        };
        let pred = PartitionPruner::extract_from_expr(&expr, &keys);
        assert!(matches!(pred, PartitionPredicate::GreaterThanOrEqual(_, Value::Int(100))));
    }

    #[test]
    fn test_extract_non_partition_column() {
        let keys = make_partition_keys();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("other_col".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(100)),
        };
        let pred = PartitionPruner::extract_from_expr(&expr, &keys);
        assert!(matches!(pred, PartitionPredicate::Unknown));
    }

    #[test]
    fn test_extract_and_predicates() {
        let keys = make_partition_keys();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("date_col".to_string())),
                op: BinaryOperator::GreaterThanOrEqual,
                right: Box::new(Expr::Number(100)),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("date_col".to_string())),
                op: BinaryOperator::LessThan,
                right: Box::new(Expr::Number(200)),
            }),
        };
        let pred = PartitionPruner::extract_from_expr(&expr, &keys);
        assert!(matches!(pred, PartitionPredicate::And(_)));
        if let PartitionPredicate::And(preds) = pred {
            assert_eq!(preds.len(), 2);
        }
    }

    #[test]
    fn test_prune_range_empty_predicates() {
        let partitions = vec![
            (
                "p1".to_string(),
                PartitionRangeBound {
                    from_values: vec![Expr::Number(0)],
                    to_values: vec![Expr::Number(100)],
                },
            ),
            (
                "p2".to_string(),
                PartitionRangeBound {
                    from_values: vec![Expr::Number(100)],
                    to_values: vec![Expr::Number(200)],
                },
            ),
            (
                "p3".to_string(),
                PartitionRangeBound {
                    from_values: vec![Expr::Number(200)],
                    to_values: vec![Expr::Number(300)],
                },
            ),
        ];
        let result = PartitionPruner::prune_partitions_range(&partitions, &[]);
        assert_eq!(result.len(), 3);
        assert!(result.contains(&"p1".to_string()));
        assert!(result.contains(&"p2".to_string()));
        assert!(result.contains(&"p3".to_string()));
    }

    #[test]
    fn test_prune_range_with_equals() {
        let partitions = vec![
            (
                "p1".to_string(),
                PartitionRangeBound {
                    from_values: vec![Expr::Number(0)],
                    to_values: vec![Expr::Number(100)],
                },
            ),
            (
                "p2".to_string(),
                PartitionRangeBound {
                    from_values: vec![Expr::Number(100)],
                    to_values: vec![Expr::Number(200)],
                },
            ),
            (
                "p3".to_string(),
                PartitionRangeBound {
                    from_values: vec![Expr::Number(200)],
                    to_values: vec![Expr::Number(300)],
                },
            ),
        ];
        let pred = PartitionPredicate::Equals("date_col".to_string(), Value::Int(150));
        let result = PartitionPruner::prune_partitions_range(&partitions, &[pred]);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&"p2".to_string()));
    }

    #[test]
    fn test_prune_range_with_greater_than() {
        let partitions = vec![
            (
                "p1".to_string(),
                PartitionRangeBound {
                    from_values: vec![Expr::Number(0)],
                    to_values: vec![Expr::Number(100)],
                },
            ),
            (
                "p2".to_string(),
                PartitionRangeBound {
                    from_values: vec![Expr::Number(100)],
                    to_values: vec![Expr::Number(200)],
                },
            ),
            (
                "p3".to_string(),
                PartitionRangeBound {
                    from_values: vec![Expr::Number(200)],
                    to_values: vec![Expr::Number(300)],
                },
            ),
        ];
        let pred = PartitionPredicate::GreaterThan("date_col".to_string(), Value::Int(150));
        let result = PartitionPruner::prune_partitions_range(&partitions, &[pred]);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&"p2".to_string()));
        assert!(result.contains(&"p3".to_string()));
    }

    #[test]
    fn test_prune_range_with_between() {
        let partitions = vec![
            (
                "p1".to_string(),
                PartitionRangeBound {
                    from_values: vec![Expr::Number(0)],
                    to_values: vec![Expr::Number(100)],
                },
            ),
            (
                "p2".to_string(),
                PartitionRangeBound {
                    from_values: vec![Expr::Number(100)],
                    to_values: vec![Expr::Number(200)],
                },
            ),
            (
                "p3".to_string(),
                PartitionRangeBound {
                    from_values: vec![Expr::Number(200)],
                    to_values: vec![Expr::Number(300)],
                },
            ),
        ];
        let pred =
            PartitionPredicate::Between("date_col".to_string(), Value::Int(50), Value::Int(150));
        let result = PartitionPruner::prune_partitions_range(&partitions, &[pred]);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&"p1".to_string()));
        assert!(result.contains(&"p2".to_string()));
    }

    #[test]
    fn test_prune_list_empty_predicates() {
        let partitions = vec![
            (
                "east".to_string(),
                PartitionListBound {
                    values: vec![Expr::String("A".to_string()), Expr::String("B".to_string())],
                },
            ),
            (
                "west".to_string(),
                PartitionListBound {
                    values: vec![Expr::String("C".to_string()), Expr::String("D".to_string())],
                },
            ),
        ];
        let result = PartitionPruner::prune_partitions_list(&partitions, &[]);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_prune_list_with_equals() {
        let partitions = vec![
            (
                "east".to_string(),
                PartitionListBound {
                    values: vec![Expr::String("A".to_string()), Expr::String("B".to_string())],
                },
            ),
            (
                "west".to_string(),
                PartitionListBound {
                    values: vec![Expr::String("C".to_string()), Expr::String("D".to_string())],
                },
            ),
        ];
        let pred = PartitionPredicate::Equals("region".to_string(), Value::Text("A".to_string()));
        let result = PartitionPruner::prune_partitions_list(&partitions, &[pred]);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&"east".to_string()));
    }

    #[test]
    fn test_prune_list_with_in() {
        let partitions = vec![
            (
                "east".to_string(),
                PartitionListBound {
                    values: vec![Expr::String("A".to_string()), Expr::String("B".to_string())],
                },
            ),
            (
                "west".to_string(),
                PartitionListBound {
                    values: vec![Expr::String("C".to_string()), Expr::String("D".to_string())],
                },
            ),
        ];
        let pred = PartitionPredicate::In(
            "region".to_string(),
            vec![Value::Text("A".to_string()), Value::Text("C".to_string())],
        );
        let result = PartitionPruner::prune_partitions_list(&partitions, &[pred]);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_prune_hash_empty_predicates() {
        let partitions = vec![
            ("p0".to_string(), PartitionHashBound { modulus: 4, remainder: 0 }),
            ("p1".to_string(), PartitionHashBound { modulus: 4, remainder: 1 }),
            ("p2".to_string(), PartitionHashBound { modulus: 4, remainder: 2 }),
            ("p3".to_string(), PartitionHashBound { modulus: 4, remainder: 3 }),
        ];
        let result = PartitionPruner::prune_partitions_hash(&partitions, &[]);
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn test_prune_hash_with_equals() {
        let partitions = vec![
            ("p0".to_string(), PartitionHashBound { modulus: 4, remainder: 0 }),
            ("p1".to_string(), PartitionHashBound { modulus: 4, remainder: 1 }),
            ("p2".to_string(), PartitionHashBound { modulus: 4, remainder: 2 }),
            ("p3".to_string(), PartitionHashBound { modulus: 4, remainder: 3 }),
        ];
        let pred = PartitionPredicate::Equals("id".to_string(), Value::Int(5));
        let result = PartitionPruner::prune_partitions_hash(&partitions, &[pred]);
        assert!(!result.is_empty());
    }

    #[test]
    fn test_increment_value_int() {
        assert_eq!(increment_value(&Value::Int(5)), Some(Value::Int(6)));
        assert_eq!(increment_value(&Value::Int(-1)), Some(Value::Int(0)));
    }

    #[test]
    fn test_decrement_value_int() {
        assert_eq!(decrement_value(&Value::Int(5)), Some(Value::Int(4)));
        assert_eq!(decrement_value(&Value::Int(0)), Some(Value::Int(-1)));
    }

    #[test]
    fn test_hash_value_int() {
        let h1 = hash_value(&Value::Int(0));
        let h2 = hash_value(&Value::Int(1));
        let h3 = hash_value(&Value::Int(2));
        assert_ne!(h1, h2);
        assert_ne!(h2, h3);
    }

    #[test]
    fn test_hash_value_text() {
        let h1 = hash_value(&Value::Text("a".to_string()));
        let h2 = hash_value(&Value::Text("b".to_string()));
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_always_true_predicate() {
        let keys = make_partition_keys();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("date_col".to_string())),
            op: BinaryOperator::NotEquals,
            right: Box::new(Expr::Null),
        };
        let pred = PartitionPruner::extract_from_expr(&expr, &keys);
        assert!(matches!(pred, PartitionPredicate::AlwaysTrue));
    }

    #[test]
    fn test_comparison_chains() {
        let keys = make_partition_keys();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("date_col".to_string())),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expr::Number(100)),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("date_col".to_string())),
                op: BinaryOperator::LessThanOrEqual,
                right: Box::new(Expr::Number(200)),
            }),
        };
        let pred = PartitionPruner::extract_from_expr(&expr, &keys);
        assert!(matches!(pred, PartitionPredicate::And(_)));
    }

    #[test]
    fn test_multiple_partition_keys() {
        let keys = vec![
            PartitionKey { column: "a".to_string(), opclass: None },
            PartitionKey { column: "b".to_string(), opclass: None },
        ];
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("a".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(1)),
        };
        let pred = PartitionPruner::extract_from_expr(&expr, &keys);
        assert!(matches!(pred, PartitionPredicate::Equals(_, Value::Int(1))));
    }

    #[test]
    fn test_extract_predicates_no_where() {
        let keys = make_partition_keys();
        let predicates = PartitionPruner::extract_predicates(&None, &keys);
        assert!(predicates.is_empty());
    }

    #[test]
    fn test_extract_predicates_with_where() {
        let keys = make_partition_keys();
        let where_clause = Expr::BinaryOp {
            left: Box::new(Expr::Column("date_col".to_string())),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expr::Number(50)),
        };
        let predicates = PartitionPruner::extract_predicates(&Some(where_clause), &keys);
        assert_eq!(predicates.len(), 1);
    }
}
