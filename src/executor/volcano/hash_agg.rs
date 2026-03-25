//! HashAggExecutor - Performs hash-based aggregation (GROUP BY and aggregates)

use crate::catalog::{Aggregate, Catalog, TableSchema, Value};
use crate::executor::eval::Eval;
use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};
use crate::parser::ast::{AggregateFunc, DataType, Expr};
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug, Clone)]
enum AggregateState {
    Count(i64),
    Sum(i64),
    Avg { sum: i64, count: i64 },
    Min(Value),
    Max(Value),
    Custom(CustomAggregateState),
}

#[derive(Debug, Clone)]
struct CustomAggregateState {
    info: Aggregate,
    state: Value,
}

pub struct HashAggExecutor {
    buffered_results: Vec<Tuple>,
    current_idx: usize,
    output_schema: TableSchema,
}

impl HashAggExecutor {
    pub fn new(
        mut child: Box<dyn Executor>,
        group_by: Vec<Expr>,
        aggregates: Vec<Expr>,
        output_schema: TableSchema,
    ) -> Result<Self, ExecutorError> {
        Self::new_with_catalog(child, group_by, aggregates, output_schema, None)
    }

    pub fn new_with_catalog(
        mut child: Box<dyn Executor>,
        group_by: Vec<Expr>,
        aggregates: Vec<Expr>,
        output_schema: TableSchema,
        catalog: Option<Arc<Catalog>>,
    ) -> Result<Self, ExecutorError> {
        let catalog_clone = catalog.as_ref().map(|c| Arc::clone(c));
        let mut groups: HashMap<u64, (Tuple, Vec<AggregateState>)> = HashMap::new();

        while let Some(tuple) = child.next()? {
            let group_key =
                if group_by.is_empty() { 0 } else { Self::compute_group_key(&tuple, &group_by)? };

            let entry = groups.entry(group_key).or_insert_with(|| {
                let mut group_tuple = Tuple::new();
                for expr in &group_by {
                    match expr {
                        Expr::Column(name) => {
                            if let Some(val) = tuple.get(name) {
                                group_tuple.insert(name.clone(), val.clone());
                            }
                        }
                        Expr::QualifiedColumn { table, column } => {
                            let qualified_name = format!("{}.{}", table, column);
                            if let Some(val) =
                                tuple.get(&qualified_name).or_else(|| tuple.get(column))
                            {
                                group_tuple.insert(qualified_name, val.clone());
                            }
                        }
                        _ => {}
                    }
                }
                let agg_states: Vec<AggregateState> = aggregates
                    .iter()
                    .map(|agg_expr| Self::create_initial_state(agg_expr, &catalog_clone))
                    .collect();
                (group_tuple, agg_states)
            });

            for (i, agg_expr) in aggregates.iter().enumerate() {
                match agg_expr {
                    Expr::Aggregate { func: _func, arg } => {
                        let arg_val = if matches!(arg.as_ref(), Expr::Star) {
                            Value::Int(1)
                        } else {
                            Eval::eval_expr(arg, &tuple)?
                        };

                        match &mut entry.1[i] {
                            AggregateState::Count(c) => {
                                if !matches!(arg_val, Value::Null) {
                                    *c += 1;
                                }
                            }
                            AggregateState::Sum(s) => {
                                if let Value::Int(v) = arg_val {
                                    *s += v;
                                }
                            }
                            AggregateState::Avg { sum, count } => {
                                if let Value::Int(v) = arg_val {
                                    *sum += v;
                                    *count += 1;
                                }
                            }
                            AggregateState::Min(current_min) => {
                                if matches!(current_min, Value::Null)
                                    || Self::compare_values(&arg_val, current_min)?
                                        == std::cmp::Ordering::Less
                                {
                                    entry.1[i] = AggregateState::Min(arg_val);
                                }
                            }
                            AggregateState::Max(current_max) => {
                                if matches!(current_max, Value::Null)
                                    || Self::compare_values(&arg_val, current_max)?
                                        == std::cmp::Ordering::Greater
                                {
                                    entry.1[i] = AggregateState::Max(arg_val);
                                }
                            }
                            AggregateState::Custom(_) => {}
                        }
                    }
                    Expr::FunctionCall { name, args } => {
                        if let Some(agg) =
                            catalog_clone.as_ref().and_then(|c| c.get_aggregate(name))
                        {
                            let arg_val = if !args.is_empty() {
                                Eval::eval_expr(&args[0], &tuple)?
                            } else {
                                Value::Null
                            };

                            if let AggregateState::Custom(ref mut custom_state) = entry.1[i] {
                                let new_state = Self::call_sfunc(
                                    &custom_state.info.sfunc,
                                    custom_state.state.clone(),
                                    arg_val,
                                    &catalog_clone,
                                )?;
                                custom_state.state = new_state;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        let mut buffered_results = Vec::new();

        if group_by.is_empty() && groups.is_empty() {
            let mut group_tuple = Tuple::new();
            let agg_states: Vec<AggregateState> = aggregates
                .iter()
                .map(|agg_expr| Self::create_initial_state(agg_expr, &catalog_clone))
                .collect();

            for (i, agg_expr) in aggregates.iter().enumerate() {
                let agg_name = Self::get_aggregate_name(agg_expr);
                let agg_value =
                    Self::compute_final_value(&agg_states[i], agg_expr, &catalog_clone)?;
                group_tuple.insert(agg_name, agg_value);
            }
            buffered_results.push(group_tuple);
        } else {
            for (_, (mut group_tuple, agg_states)) in groups {
                for (i, agg_expr) in aggregates.iter().enumerate() {
                    let agg_name = Self::get_aggregate_name(agg_expr);
                    let agg_value =
                        Self::compute_final_value(&agg_states[i], agg_expr, &catalog_clone)?;
                    group_tuple.insert(agg_name, agg_value);
                }
                buffered_results.push(group_tuple);
            }
        }

        Ok(Self { buffered_results, current_idx: 0, output_schema })
    }

    fn create_initial_state(agg_expr: &Expr, catalog: &Option<Arc<Catalog>>) -> AggregateState {
        match agg_expr {
            Expr::Aggregate { func, .. } => match func {
                AggregateFunc::Count => AggregateState::Count(0),
                AggregateFunc::Sum => AggregateState::Sum(0),
                AggregateFunc::Avg => AggregateState::Avg { sum: 0, count: 0 },
                AggregateFunc::Min => AggregateState::Min(Value::Null),
                AggregateFunc::Max => AggregateState::Max(Value::Null),
            },
            Expr::FunctionCall { name, .. } => {
                if let Some(agg) = catalog.as_ref().and_then(|c| c.get_aggregate(name)) {
                    let init_state = Self::parse_initcond(&agg.initcond, &agg.stype);
                    AggregateState::Custom(CustomAggregateState {
                        info: agg.clone(),
                        state: init_state,
                    })
                } else {
                    AggregateState::Count(0)
                }
            }
            _ => AggregateState::Count(0),
        }
    }

    fn parse_initcond(initcond: &Option<String>, stype: &str) -> Value {
        match initcond {
            Some(cond) => {
                if cond.is_empty() {
                    return Self::default_state_for_type(stype);
                }
                if let Ok(v) = cond.parse::<i64>() {
                    return Value::Int(v);
                }
                if let Ok(v) = cond.parse::<f64>() {
                    return Value::Float(v);
                }
                if cond == "NULL" || cond.is_empty() {
                    return Value::Null;
                }
                Value::Text(cond.to_string())
            }
            None => Self::default_state_for_type(stype),
        }
    }

    fn default_state_for_type(stype: &str) -> Value {
        match stype.to_uppercase().as_str() {
            "INT" | "INT4" | "INT8" | "BIGINT" | "SMALLINT" | "OID" => Value::Int(0),
            "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "NUMERIC" | "DECIMAL" => Value::Float(0.0),
            "TEXT" | "VARCHAR" | "CHAR" | "BPCHAR" => Value::Text(String::new()),
            "BOOL" | "BOOLEAN" => Value::Bool(false),
            _ => Value::Null,
        }
    }

    fn call_sfunc(
        sfunc_name: &str,
        current_state: Value,
        new_value: Value,
        catalog: &Option<Arc<Catalog>>,
    ) -> Result<Value, ExecutorError> {
        if let Some(cat) = catalog {
            Eval::eval_function_call(sfunc_name, vec![current_state, new_value], Some(cat.as_ref()))
        } else {
            Err(ExecutorError::InternalError(
                "Catalog required for custom aggregate sfunc".to_string(),
            ))
        }
    }

    fn call_finalfunc(
        finalfunc_name: &str,
        state: Value,
        catalog: &Option<Arc<Catalog>>,
    ) -> Result<Value, ExecutorError> {
        if let Some(cat) = catalog {
            Eval::eval_function_call(finalfunc_name, vec![state], Some(cat.as_ref()))
        } else {
            Err(ExecutorError::InternalError(
                "Catalog required for custom aggregate finalfunc".to_string(),
            ))
        }
    }

    fn compute_final_value(
        state: &AggregateState,
        agg_expr: &Expr,
        catalog: &Option<Arc<Catalog>>,
    ) -> Result<Value, ExecutorError> {
        match state {
            AggregateState::Count(c) => Ok(Value::Int(*c)),
            AggregateState::Sum(s) => Ok(Value::Int(*s)),
            AggregateState::Avg { sum, count } => {
                if *count > 0 {
                    Ok(Value::Int(*sum / *count))
                } else {
                    Ok(Value::Null)
                }
            }
            AggregateState::Min(v) => Ok(v.clone()),
            AggregateState::Max(v) => Ok(v.clone()),
            AggregateState::Custom(custom_state) => {
                if let Some(ref finalfunc) = custom_state.info.finalfunc {
                    Self::call_finalfunc(finalfunc, custom_state.state.clone(), catalog)
                } else {
                    Ok(custom_state.state.clone())
                }
            }
        }
    }

    fn compute_group_key(tuple: &Tuple, group_by: &[Expr]) -> Result<u64, ExecutorError> {
        let mut hasher = DefaultHasher::new();
        for expr in group_by {
            match expr {
                Expr::Column(name) => {
                    if let Some(val) = tuple.get(name) {
                        Self::hash_value(val, &mut hasher);
                    }
                }
                Expr::QualifiedColumn { table, column } => {
                    let qualified_name = format!("{}.{}", table, column);
                    if let Some(val) = tuple.get(&qualified_name).or_else(|| tuple.get(column)) {
                        Self::hash_value(val, &mut hasher);
                    }
                }
                _ => {
                    return Err(ExecutorError::UnsupportedExpression(format!(
                        "Unsupported GROUP BY expression: {:?}",
                        expr
                    )));
                }
            }
        }
        Ok(hasher.finish())
    }

    fn hash_value(value: &Value, hasher: &mut DefaultHasher) {
        match value {
            Value::Int(n) => {
                "int".hash(hasher);
                n.hash(hasher);
            }
            Value::Text(s) => {
                "text".hash(hasher);
                s.hash(hasher);
            }
            Value::Bool(b) => {
                "bool".hash(hasher);
                b.hash(hasher);
            }
            Value::Null => {
                "null".hash(hasher);
            }
            _ => {
                format!("{:?}", value).hash(hasher);
            }
        }
    }

    fn compare_values(a: &Value, b: &Value) -> Result<std::cmp::Ordering, ExecutorError> {
        match (a, b) {
            (Value::Int(a), Value::Int(b)) => Ok(a.cmp(b)),
            (Value::Text(a), Value::Text(b)) => Ok(a.cmp(b)),
            (Value::Null, Value::Null) => Ok(std::cmp::Ordering::Equal),
            (Value::Null, _) => Ok(std::cmp::Ordering::Less),
            (_, Value::Null) => Ok(std::cmp::Ordering::Greater),
            _ => Err(ExecutorError::TypeMismatch("Cannot compare different types".to_string())),
        }
    }

    fn get_aggregate_name(expr: &Expr) -> String {
        match expr {
            Expr::Aggregate { func, arg } => {
                let arg_name = match arg.as_ref() {
                    Expr::Column(col_name) => col_name.clone(),
                    Expr::QualifiedColumn { column, .. } => column.clone(),
                    Expr::Star => "*".to_string(),
                    _ => "expr".to_string(),
                };
                format!("{:?}({})", func, arg_name).to_lowercase()
            }
            Expr::FunctionCall { name, args } => {
                let arg_name = if !args.is_empty() {
                    match &args[0] {
                        Expr::Column(col_name) => col_name.clone(),
                        _ => "expr".to_string(),
                    }
                } else {
                    "expr".to_string()
                };
                format!("{}({})", name, arg_name)
            }
            Expr::Alias { alias, .. } => alias.clone(),
            _ => format!("{:?}", expr),
        }
    }
}

impl Executor for HashAggExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if self.current_idx >= self.buffered_results.len() {
            return Ok(None);
        }

        let tuple = self.buffered_results[self.current_idx].clone();
        self.current_idx += 1;
        Ok(Some(tuple))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::TableSchema;
    use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};
    use crate::parser::ast::{AggregateFunc, Expr};

    struct MockExecutor {
        tuples: Vec<Tuple>,
        idx: usize,
    }

    impl MockExecutor {
        fn new(tuples: Vec<Tuple>) -> Self {
            Self { tuples, idx: 0 }
        }
    }

    impl Executor for MockExecutor {
        fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
            if self.idx >= self.tuples.len() {
                Ok(None)
            } else {
                self.idx += 1;
                Ok(Some(self.tuples[self.idx - 1].clone()))
            }
        }
    }

    #[test]
    fn test_simple_count() {
        let tuples = vec![
            [("a".to_string(), Value::Int(1))].into(),
            [("a".to_string(), Value::Int(2))].into(),
            [("a".to_string(), Value::Int(3))].into(),
        ];
        let child = Box::new(MockExecutor::new(tuples));
        let aggregates = vec![Expr::Aggregate {
            func: AggregateFunc::Count,
            arg: Box::new(Expr::Column("a".to_string())),
        }];
        let output_schema = TableSchema::new("agg".to_string(), vec![]);
        let mut agg_executor =
            HashAggExecutor::new(child, vec![], aggregates, output_schema).unwrap();

        let result = agg_executor.next().unwrap().unwrap();
        assert_eq!(result.get("count(a)"), Some(&Value::Int(3)));
        assert!(agg_executor.next().unwrap().is_none());
    }

    #[test]
    fn test_sum_and_avg() {
        let tuples = vec![
            [("a".to_string(), Value::Int(10))].into(),
            [("a".to_string(), Value::Int(20))].into(),
            [("a".to_string(), Value::Int(30))].into(),
        ];
        let child = Box::new(MockExecutor::new(tuples));
        let aggregates = vec![
            Expr::Aggregate {
                func: AggregateFunc::Sum,
                arg: Box::new(Expr::Column("a".to_string())),
            },
            Expr::Aggregate {
                func: AggregateFunc::Avg,
                arg: Box::new(Expr::Column("a".to_string())),
            },
        ];
        let output_schema = TableSchema::new("agg".to_string(), vec![]);
        let mut agg_executor =
            HashAggExecutor::new(child, vec![], aggregates, output_schema).unwrap();

        let result = agg_executor.next().unwrap().unwrap();
        assert_eq!(result.get("sum(a)"), Some(&Value::Int(60)));
        assert_eq!(result.get("avg(a)"), Some(&Value::Int(20)));
        assert!(agg_executor.next().unwrap().is_none());
    }

    #[test]
    fn test_min_max() {
        let tuples = vec![
            [("a".to_string(), Value::Int(10))].into(),
            [("a".to_string(), Value::Int(5))].into(),
            [("a".to_string(), Value::Int(20))].into(),
        ];
        let child = Box::new(MockExecutor::new(tuples));
        let aggregates = vec![
            Expr::Aggregate {
                func: AggregateFunc::Min,
                arg: Box::new(Expr::Column("a".to_string())),
            },
            Expr::Aggregate {
                func: AggregateFunc::Max,
                arg: Box::new(Expr::Column("a".to_string())),
            },
        ];
        let output_schema = TableSchema::new("agg".to_string(), vec![]);
        let mut agg_executor =
            HashAggExecutor::new(child, vec![], aggregates, output_schema).unwrap();

        let result = agg_executor.next().unwrap().unwrap();
        assert_eq!(result.get("min(a)"), Some(&Value::Int(5)));
        assert_eq!(result.get("max(a)"), Some(&Value::Int(20)));
        assert!(agg_executor.next().unwrap().is_none());
    }

    #[test]
    fn test_group_by_single_column() {
        let tuples = vec![
            [("a".to_string(), Value::Text("foo".to_string())), ("b".to_string(), Value::Int(10))]
                .into(),
            [("a".to_string(), Value::Text("bar".to_string())), ("b".to_string(), Value::Int(20))]
                .into(),
            [("a".to_string(), Value::Text("foo".to_string())), ("b".to_string(), Value::Int(30))]
                .into(),
        ];
        let child = Box::new(MockExecutor::new(tuples));
        let group_by = vec![Expr::Column("a".to_string())];
        let aggregates = vec![Expr::Aggregate {
            func: AggregateFunc::Sum,
            arg: Box::new(Expr::Column("b".to_string())),
        }];
        let output_schema = TableSchema::new("agg".to_string(), vec![]);
        let mut agg_executor =
            HashAggExecutor::new(child, group_by, aggregates, output_schema).unwrap();

        let mut results: Vec<Tuple> = Vec::new();
        while let Some(tuple) = agg_executor.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
        for r in results {
            if r.get("a") == Some(&Value::Text("foo".to_string())) {
                assert_eq!(r.get("sum(b)"), Some(&Value::Int(40)));
            } else if r.get("a") == Some(&Value::Text("bar".to_string())) {
                assert_eq!(r.get("sum(b)"), Some(&Value::Int(20)));
            } else {
                panic!("Unexpected group key");
            }
        }
    }

    #[test]
    fn test_empty_input() {
        let tuples = vec![];
        let child = Box::new(MockExecutor::new(tuples));
        let aggregates = vec![Expr::Aggregate {
            func: AggregateFunc::Count,
            arg: Box::new(Expr::Column("a".to_string())),
        }];
        let output_schema = TableSchema::new("agg".to_string(), vec![]);
        let mut agg_executor =
            HashAggExecutor::new(child, vec![], aggregates, output_schema).unwrap();

        let result = agg_executor.next().unwrap().unwrap();
        assert_eq!(result.get("count(a)"), Some(&Value::Int(0)));
        assert!(agg_executor.next().unwrap().is_none());
    }

    #[test]
    fn test_null_values() {
        let tuples = vec![
            [("a".to_string(), Value::Int(10))].into(),
            [("a".to_string(), Value::Null)].into(),
            [("a".to_string(), Value::Int(20))].into(),
        ];
        let child = Box::new(MockExecutor::new(tuples));
        let aggregates = vec![
            Expr::Aggregate {
                func: AggregateFunc::Count,
                arg: Box::new(Expr::Column("a".to_string())),
            },
            Expr::Aggregate {
                func: AggregateFunc::Sum,
                arg: Box::new(Expr::Column("a".to_string())),
            },
        ];
        let output_schema = TableSchema::new("agg".to_string(), vec![]);
        let mut agg_executor =
            HashAggExecutor::new(child, vec![], aggregates, output_schema).unwrap();

        let result = agg_executor.next().unwrap().unwrap();
        assert_eq!(result.get("count(a)"), Some(&Value::Int(2)));
        assert_eq!(result.get("sum(a)"), Some(&Value::Int(30)));
        assert!(agg_executor.next().unwrap().is_none());
    }

    #[test]
    fn test_count_star() {
        let tuples = vec![
            [("a".to_string(), Value::Int(10))].into(),
            [("a".to_string(), Value::Null)].into(),
            [("a".to_string(), Value::Int(20))].into(),
        ];
        let child = Box::new(MockExecutor::new(tuples));
        let aggregates =
            vec![Expr::Aggregate { func: AggregateFunc::Count, arg: Box::new(Expr::Star) }];
        let output_schema = TableSchema::new("agg".to_string(), vec![]);
        let mut agg_executor =
            HashAggExecutor::new(child, vec![], aggregates, output_schema).unwrap();

        let result = agg_executor.next().unwrap().unwrap();
        assert_eq!(result.get("count(*)"), Some(&Value::Int(3)));
        assert!(agg_executor.next().unwrap().is_none());
    }

    #[test]
    fn test_count_star_empty_input() {
        let tuples = vec![];
        let child = Box::new(MockExecutor::new(tuples));
        let aggregates =
            vec![Expr::Aggregate { func: AggregateFunc::Count, arg: Box::new(Expr::Star) }];
        let output_schema = TableSchema::new("agg".to_string(), vec![]);
        let mut agg_executor =
            HashAggExecutor::new(child, vec![], aggregates, output_schema).unwrap();

        let result = agg_executor.next().unwrap().unwrap();
        assert_eq!(result.get("count(*)"), Some(&Value::Int(0)));
        assert!(agg_executor.next().unwrap().is_none());
    }

    #[test]
    fn test_count_star_with_group_by() {
        let tuples = vec![
            [
                ("category".to_string(), Value::Text("A".to_string())),
                ("value".to_string(), Value::Int(1)),
            ]
            .into(),
            [
                ("category".to_string(), Value::Text("A".to_string())),
                ("value".to_string(), Value::Int(2)),
            ]
            .into(),
            [
                ("category".to_string(), Value::Text("B".to_string())),
                ("value".to_string(), Value::Int(3)),
            ]
            .into(),
            [
                ("category".to_string(), Value::Text("B".to_string())),
                ("value".to_string(), Value::Null),
            ]
            .into(),
            [
                ("category".to_string(), Value::Text("B".to_string())),
                ("value".to_string(), Value::Int(4)),
            ]
            .into(),
        ];
        let child = Box::new(MockExecutor::new(tuples));
        let group_by = vec![Expr::Column("category".to_string())];
        let aggregates =
            vec![Expr::Aggregate { func: AggregateFunc::Count, arg: Box::new(Expr::Star) }];
        let output_schema = TableSchema::new("agg".to_string(), vec![]);
        let mut agg_executor =
            HashAggExecutor::new(child, group_by, aggregates, output_schema).unwrap();

        let mut results: Vec<Tuple> = Vec::new();
        while let Some(tuple) = agg_executor.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
        for r in results {
            if r.get("category") == Some(&Value::Text("A".to_string())) {
                assert_eq!(r.get("count(*)"), Some(&Value::Int(2)));
            } else if r.get("category") == Some(&Value::Text("B".to_string())) {
                assert_eq!(r.get("count(*)"), Some(&Value::Int(3)));
            } else {
                panic!("Unexpected group key");
            }
        }
    }

    #[test]
    fn test_parse_initcond_int() {
        let agg = Aggregate {
            name: "my_sum".to_string(),
            input_type: "INT".to_string(),
            sfunc: "int8pl".to_string(),
            stype: "INT8".to_string(),
            finalfunc: Some("int8out".to_string()),
            initcond: Some("0".to_string()),
            volatility: crate::catalog::FunctionVolatility::Immutable,
            cost: 100.0,
        };

        let catalog = Catalog::new();
        catalog.create_aggregate(agg).unwrap();

        let state = HashAggExecutor::create_initial_state(
            &Expr::FunctionCall {
                name: "my_sum".to_string(),
                args: vec![Expr::Column("x".to_string())],
            },
            &Some(Arc::new(catalog)),
        );

        assert!(matches!(state, AggregateState::Custom(_)));
    }

    #[test]
    fn test_parse_initcond_null() {
        let agg = Aggregate {
            name: "my_sum".to_string(),
            input_type: "INT".to_string(),
            sfunc: "int8pl".to_string(),
            stype: "INT8".to_string(),
            finalfunc: Some("int8out".to_string()),
            initcond: None,
            volatility: crate::catalog::FunctionVolatility::Immutable,
            cost: 100.0,
        };

        let catalog = Catalog::new();
        catalog.create_aggregate(agg).unwrap();

        let state = HashAggExecutor::create_initial_state(
            &Expr::FunctionCall {
                name: "my_sum".to_string(),
                args: vec![Expr::Column("x".to_string())],
            },
            &Some(Arc::new(catalog)),
        );

        assert!(matches!(state, AggregateState::Custom(_)));
    }

    #[test]
    fn test_custom_aggregate_with_catalog() {
        let agg = Aggregate {
            name: "my_count".to_string(),
            input_type: "INT".to_string(),
            sfunc: "my_count_sfunc".to_string(),
            stype: "INT8".to_string(),
            finalfunc: None,
            initcond: Some("0".to_string()),
            volatility: crate::catalog::FunctionVolatility::Immutable,
            cost: 100.0,
        };

        let catalog = Catalog::new();
        catalog.create_aggregate(agg).unwrap();

        let state = HashAggExecutor::create_initial_state(
            &Expr::FunctionCall {
                name: "my_count".to_string(),
                args: vec![Expr::Column("x".to_string())],
            },
            &Some(Arc::new(catalog)),
        );

        match state {
            AggregateState::Custom(custom) => {
                assert_eq!(custom.info.name, "my_count");
                assert_eq!(custom.info.sfunc, "my_count_sfunc");
                assert_eq!(custom.state, Value::Int(0));
            }
            _ => panic!("Expected Custom aggregate state"),
        }
    }

    #[test]
    fn test_custom_aggregate_initcond_text() {
        let agg = Aggregate {
            name: "my_concat".to_string(),
            input_type: "TEXT".to_string(),
            sfunc: "text_concat".to_string(),
            stype: "TEXT".to_string(),
            finalfunc: None,
            initcond: Some("hello".to_string()),
            volatility: crate::catalog::FunctionVolatility::Immutable,
            cost: 100.0,
        };

        let catalog = Catalog::new();
        catalog.create_aggregate(agg).unwrap();

        let state = HashAggExecutor::create_initial_state(
            &Expr::FunctionCall {
                name: "my_concat".to_string(),
                args: vec![Expr::Column("x".to_string())],
            },
            &Some(Arc::new(catalog)),
        );

        match state {
            AggregateState::Custom(custom) => {
                assert_eq!(custom.state, Value::Text("hello".to_string()));
            }
            _ => panic!("Expected Custom aggregate state"),
        }
    }

    #[test]
    fn test_parse_initcond_with_numeric() {
        let agg = Aggregate {
            name: "my_agg".to_string(),
            input_type: "INT".to_string(),
            sfunc: "my_sfunc".to_string(),
            stype: "INT8".to_string(),
            finalfunc: None,
            initcond: Some("42".to_string()),
            volatility: crate::catalog::FunctionVolatility::Immutable,
            cost: 100.0,
        };

        let catalog = Catalog::new();
        catalog.create_aggregate(agg).unwrap();

        let state = HashAggExecutor::create_initial_state(
            &Expr::FunctionCall {
                name: "my_agg".to_string(),
                args: vec![Expr::Column("x".to_string())],
            },
            &Some(Arc::new(catalog)),
        );

        match state {
            AggregateState::Custom(custom) => {
                assert_eq!(custom.state, Value::Int(42));
            }
            _ => panic!("Expected Custom aggregate state"),
        }
    }

    #[test]
    fn test_parse_initcond_with_null() {
        let agg = Aggregate {
            name: "my_agg".to_string(),
            input_type: "INT".to_string(),
            sfunc: "my_sfunc".to_string(),
            stype: "INT8".to_string(),
            finalfunc: None,
            initcond: Some("NULL".to_string()),
            volatility: crate::catalog::FunctionVolatility::Immutable,
            cost: 100.0,
        };

        let catalog = Catalog::new();
        catalog.create_aggregate(agg).unwrap();

        let state = HashAggExecutor::create_initial_state(
            &Expr::FunctionCall {
                name: "my_agg".to_string(),
                args: vec![Expr::Column("x".to_string())],
            },
            &Some(Arc::new(catalog)),
        );

        match state {
            AggregateState::Custom(custom) => {
                assert_eq!(custom.state, Value::Null);
            }
            _ => panic!("Expected Custom aggregate state"),
        }
    }

    #[test]
    fn test_parse_initcond_default_for_int() {
        let agg = Aggregate {
            name: "my_agg".to_string(),
            input_type: "INT".to_string(),
            sfunc: "my_sfunc".to_string(),
            stype: "INT8".to_string(),
            finalfunc: None,
            initcond: None,
            volatility: crate::catalog::FunctionVolatility::Immutable,
            cost: 100.0,
        };

        let catalog = Catalog::new();
        catalog.create_aggregate(agg).unwrap();

        let state = HashAggExecutor::create_initial_state(
            &Expr::FunctionCall {
                name: "my_agg".to_string(),
                args: vec![Expr::Column("x".to_string())],
            },
            &Some(Arc::new(catalog)),
        );

        match state {
            AggregateState::Custom(custom) => {
                assert_eq!(custom.state, Value::Int(0));
            }
            _ => panic!("Expected Custom aggregate state"),
        }
    }

    #[test]
    fn test_parse_initcond_default_for_float() {
        let agg = Aggregate {
            name: "my_agg".to_string(),
            input_type: "FLOAT".to_string(),
            sfunc: "my_sfunc".to_string(),
            stype: "FLOAT8".to_string(),
            finalfunc: None,
            initcond: None,
            volatility: crate::catalog::FunctionVolatility::Immutable,
            cost: 100.0,
        };

        let catalog = Catalog::new();
        catalog.create_aggregate(agg).unwrap();

        let state = HashAggExecutor::create_initial_state(
            &Expr::FunctionCall {
                name: "my_agg".to_string(),
                args: vec![Expr::Column("x".to_string())],
            },
            &Some(Arc::new(catalog)),
        );

        match state {
            AggregateState::Custom(custom) => {
                assert_eq!(custom.state, Value::Float(0.0));
            }
            _ => panic!("Expected Custom aggregate state"),
        }
    }

    #[test]
    fn test_parse_initcond_default_for_bool() {
        let agg = Aggregate {
            name: "my_agg".to_string(),
            input_type: "BOOL".to_string(),
            sfunc: "my_sfunc".to_string(),
            stype: "BOOL".to_string(),
            finalfunc: None,
            initcond: None,
            volatility: crate::catalog::FunctionVolatility::Immutable,
            cost: 100.0,
        };

        let catalog = Catalog::new();
        catalog.create_aggregate(agg).unwrap();

        let state = HashAggExecutor::create_initial_state(
            &Expr::FunctionCall {
                name: "my_agg".to_string(),
                args: vec![Expr::Column("x".to_string())],
            },
            &Some(Arc::new(catalog)),
        );

        match state {
            AggregateState::Custom(custom) => {
                assert_eq!(custom.state, Value::Bool(false));
            }
            _ => panic!("Expected Custom aggregate state"),
        }
    }

    #[test]
    fn test_parse_initcond_unknown_type_defaults_to_null() {
        let agg = Aggregate {
            name: "my_agg".to_string(),
            input_type: "UNKNOWN".to_string(),
            sfunc: "my_sfunc".to_string(),
            stype: "UNKNOWN".to_string(),
            finalfunc: None,
            initcond: None,
            volatility: crate::catalog::FunctionVolatility::Immutable,
            cost: 100.0,
        };

        let catalog = Catalog::new();
        catalog.create_aggregate(agg).unwrap();

        let state = HashAggExecutor::create_initial_state(
            &Expr::FunctionCall {
                name: "my_agg".to_string(),
                args: vec![Expr::Column("x".to_string())],
            },
            &Some(Arc::new(catalog)),
        );

        match state {
            AggregateState::Custom(custom) => {
                assert_eq!(custom.state, Value::Null);
            }
            _ => panic!("Expected Custom aggregate state"),
        }
    }

    #[test]
    fn test_non_custom_function_call_returns_count() {
        let catalog = Catalog::new();

        let state = HashAggExecutor::create_initial_state(
            &Expr::FunctionCall {
                name: "regular_function".to_string(),
                args: vec![Expr::Column("x".to_string())],
            },
            &Some(Arc::new(catalog)),
        );

        assert!(matches!(state, AggregateState::Count(0)));
    }

    #[test]
    fn test_get_aggregate_name_function_call() {
        let name = HashAggExecutor::get_aggregate_name(&Expr::FunctionCall {
            name: "my_sum".to_string(),
            args: vec![Expr::Column("x".to_string())],
        });
        assert_eq!(name, "my_sum(x)");

        let name2 = HashAggExecutor::get_aggregate_name(&Expr::FunctionCall {
            name: "count_star".to_string(),
            args: vec![],
        });
        assert_eq!(name2, "count_star(expr)");
    }

    #[test]
    fn test_custom_aggregate_with_finalfunc() {
        let agg = Aggregate {
            name: "my_avg".to_string(),
            input_type: "INT".to_string(),
            sfunc: "int8_avg_accum".to_string(),
            stype: "INT8".to_string(),
            finalfunc: Some("int8_avg".to_string()),
            initcond: Some("0".to_string()),
            volatility: crate::catalog::FunctionVolatility::Immutable,
            cost: 100.0,
        };

        let catalog = Catalog::new();
        catalog.create_aggregate(agg).unwrap();

        let state = HashAggExecutor::create_initial_state(
            &Expr::FunctionCall {
                name: "my_avg".to_string(),
                args: vec![Expr::Column("x".to_string())],
            },
            &Some(Arc::new(catalog)),
        );

        match state {
            AggregateState::Custom(custom) => {
                assert_eq!(custom.info.finalfunc, Some("int8_avg".to_string()));
            }
            _ => panic!("Expected Custom aggregate state"),
        }
    }
}
