use crate::catalog::Value;
use crate::executor::operators::executor::{ExecutorError, Tuple};
use crate::executor::parallel::config::ParallelConfig;
use crate::executor::parallel::morsel::Morsel;
use crate::executor::parallel::operator::ParallelOperator;
use crate::executor::parallel::partition::PartitionStrategy;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

pub struct ParallelHashJoin {
    build_side: Arc<dyn ParallelOperator>,
    probe_side: Arc<dyn ParallelOperator>,
    hash_table: Arc<ConcurrentHashTable>,
    partition_strategy: Arc<PartitionStrategy>,
    build_keys: Vec<String>,
    probe_keys: Vec<String>,
    join_type: JoinType,
    left_alias: String,
    right_alias: String,
    build_tuples: Vec<Tuple>,
    probe_tuples: Vec<Tuple>,
}

pub struct ConcurrentHashTable {
    partitions: Vec<Mutex<HashMap<Vec<u8>, Vec<Tuple>>>>,
}

impl ConcurrentHashTable {
    pub fn new(num_partitions: usize) -> Self {
        let partitions = (0..num_partitions).map(|_| Mutex::new(HashMap::new())).collect();
        Self { partitions }
    }

    pub fn insert(&self, partition_id: usize, key: Vec<u8>, tuple: Tuple) {
        let mut partition = self.partitions[partition_id].lock().unwrap();
        partition.entry(key).or_default().push(tuple);
    }

    pub fn get(&self, partition_id: usize, key: &[u8]) -> Vec<Tuple> {
        let partition = self.partitions[partition_id].lock().unwrap();
        partition.get(key).cloned().unwrap_or_default()
    }

    pub fn clear(&self) {
        for partition in &self.partitions {
            partition.lock().unwrap().clear();
        }
    }

    pub fn mark_matched(&self, partition_id: usize, key: &[u8], index: usize) {
        let mut partition = self.partitions[partition_id].lock().unwrap();
        if let Some(vals) = partition.get_mut(key) {
            if index < vals.len() {
                vals[index].insert("__matched".to_string(), Value::Bool(true));
            }
        }
    }

    pub fn get_all_unmatched(&self, partition_id: usize) -> Vec<Tuple> {
        let partition = self.partitions[partition_id].lock().unwrap();
        let mut unmatched = Vec::new();
        for tuples in partition.values() {
            for tuple in tuples {
                if !tuple.contains_key("__matched") {
                    let mut clean_tuple = tuple.clone();
                    clean_tuple.remove("__matched");
                    unmatched.push(clean_tuple);
                }
            }
        }
        unmatched
    }
}

impl ParallelHashJoin {
    pub fn new(
        build_side: Arc<dyn ParallelOperator>,
        probe_side: Arc<dyn ParallelOperator>,
        num_partitions: usize,
        build_keys: Vec<String>,
        probe_keys: Vec<String>,
        join_type: JoinType,
        left_alias: String,
        right_alias: String,
    ) -> Self {
        let partition_strategy = Arc::new(PartitionStrategy::new(num_partitions));
        let hash_table = Arc::new(ConcurrentHashTable::new(num_partitions));

        Self {
            build_side,
            probe_side,
            hash_table,
            partition_strategy,
            build_keys,
            probe_keys,
            join_type,
            left_alias,
            right_alias,
            build_tuples: Vec::new(),
            probe_tuples: Vec::new(),
        }
    }

    pub fn inner(
        build_side: Arc<dyn ParallelOperator>,
        probe_side: Arc<dyn ParallelOperator>,
        num_partitions: usize,
        build_keys: Vec<String>,
        probe_keys: Vec<String>,
        left_alias: String,
        right_alias: String,
    ) -> Self {
        Self::new(
            build_side,
            probe_side,
            num_partitions,
            build_keys,
            probe_keys,
            JoinType::Inner,
            left_alias,
            right_alias,
        )
    }

    pub fn left(
        build_side: Arc<dyn ParallelOperator>,
        probe_side: Arc<dyn ParallelOperator>,
        num_partitions: usize,
        build_keys: Vec<String>,
        probe_keys: Vec<String>,
        left_alias: String,
        right_alias: String,
    ) -> Self {
        Self::new(
            build_side,
            probe_side,
            num_partitions,
            build_keys,
            probe_keys,
            JoinType::Left,
            left_alias,
            right_alias,
        )
    }

    pub fn right(
        build_side: Arc<dyn ParallelOperator>,
        probe_side: Arc<dyn ParallelOperator>,
        num_partitions: usize,
        build_keys: Vec<String>,
        probe_keys: Vec<String>,
        left_alias: String,
        right_alias: String,
    ) -> Self {
        Self::new(
            build_side,
            probe_side,
            num_partitions,
            build_keys,
            probe_keys,
            JoinType::Right,
            left_alias,
            right_alias,
        )
    }

    pub fn full(
        build_side: Arc<dyn ParallelOperator>,
        probe_side: Arc<dyn ParallelOperator>,
        num_partitions: usize,
        build_keys: Vec<String>,
        probe_keys: Vec<String>,
        left_alias: String,
        right_alias: String,
    ) -> Self {
        Self::new(
            build_side,
            probe_side,
            num_partitions,
            build_keys,
            probe_keys,
            JoinType::Full,
            left_alias,
            right_alias,
        )
    }

    fn extract_composite_key(
        &self,
        tuple: &Tuple,
        keys: &[String],
    ) -> Result<Vec<u8>, ExecutorError> {
        let mut key_bytes = Vec::new();
        for key in keys {
            match tuple.get(key) {
                Some(value) => {
                    key_bytes.extend_from_slice(&self.value_to_key(value));
                    key_bytes.push(0xFF);
                }
                None => {
                    return Err(ExecutorError::ColumnNotFound(key.clone()));
                }
            }
        }
        Ok(key_bytes)
    }

    fn value_to_key(&self, value: &Value) -> Vec<u8> {
        match value {
            Value::Int(i) => i.to_le_bytes().to_vec(),
            Value::Float(f) => f.to_le_bytes().to_vec(),
            Value::Text(s) => s.as_bytes().to_vec(),
            Value::Bool(b) => vec![*b as u8],
            Value::Bytea(b) => b.clone(),
            Value::Null => vec![],
            Value::Enum(e) => {
                let mut bytes = e.type_name.as_bytes().to_vec();
                bytes.push(0);
                bytes.extend_from_slice(&e.index.to_le_bytes());
                bytes
            }
            Value::Array(arr) => {
                let mut bytes = Vec::new();
                for v in arr {
                    bytes.extend_from_slice(&self.value_to_key(v));
                    bytes.push(0xFF);
                }
                bytes
            }
            Value::Json(s) => s.as_bytes().to_vec(),
            Value::Date(d) => d.to_le_bytes().to_vec(),
            Value::Time(t) => t.to_le_bytes().to_vec(),
            Value::Timestamp(ts) => ts.to_le_bytes().to_vec(),
            Value::Decimal(m, e) => {
                let mut bytes = m.to_le_bytes().to_vec();
                bytes.extend_from_slice(&e.to_le_bytes());
                bytes
            }
            Value::Range(r) => {
                let mut bytes = vec![];
                bytes.push(if r.lower_inclusive() { b'[' } else { b'(' });
                if let Some(l) = r.lower.as_ref() {
                    bytes.extend_from_slice(self.value_to_key(l.value.as_ref()).as_slice());
                }
                bytes.push(b',');
                if let Some(u) = r.upper.as_ref() {
                    bytes.extend_from_slice(self.value_to_key(u.value.as_ref()).as_slice());
                }
                bytes.push(if r.upper_inclusive() { b']' } else { b')' });
                bytes
            }
            Value::Composite(comp) => comp.to_string().as_bytes().to_vec(),
        }
    }

    fn partition_key(&self, key: &[u8]) -> usize {
        self.partition_strategy.partition_key(key)
    }

    fn prefix_tuple(tuple: &Tuple, alias: &str) -> Tuple {
        tuple.iter().map(|(k, v)| (format!("{}.{}", alias, k), v.clone())).collect()
    }

    fn merge_tuples(left: &Tuple, right: &Tuple, left_alias: &str, right_alias: &str) -> Tuple {
        let mut result = Self::prefix_tuple(left, left_alias);
        for (key, value) in right {
            result.insert(format!("{}.{}", right_alias, key), value.clone());
        }
        result
    }

    fn left_only_tuple(
        left: &Tuple,
        left_alias: &str,
        right_schema: &[String],
        right_alias: &str,
    ) -> Tuple {
        let mut result = Self::prefix_tuple(left, left_alias);
        for col in right_schema {
            result.insert(format!("{}.{}", right_alias, col), Value::Null);
        }
        result
    }

    fn right_only_tuple(
        right: &Tuple,
        left_schema: &[String],
        left_alias: &str,
        right_alias: &str,
    ) -> Tuple {
        let mut result: Tuple = left_schema
            .iter()
            .map(|col| (format!("{}.{}", left_alias, col), Value::Null))
            .collect();
        for (key, value) in right {
            result.insert(format!("{}.{}", right_alias, key), value.clone());
        }
        result
    }

    pub fn set_build_tuples(&mut self, tuples: Vec<Tuple>) {
        self.build_tuples = tuples;
    }

    pub fn set_probe_tuples(&mut self, tuples: Vec<Tuple>) {
        self.probe_tuples = tuples;
    }

    pub fn build_phase(&self, morsel: Morsel) -> Result<(), ExecutorError> {
        for tuple in morsel.tuples {
            let key = self.extract_composite_key(&tuple, &self.build_keys)?;
            let partition_id = self.partition_key(&key);
            self.hash_table.insert(partition_id, key, tuple);
        }
        Ok(())
    }

    pub fn probe_phase(&self, morsel: Morsel) -> Result<Vec<Tuple>, ExecutorError> {
        let mut results = Vec::new();

        for tuple in morsel.tuples {
            let key = self.extract_composite_key(&tuple, &self.probe_keys)?;
            let partition_id = self.partition_key(&key);
            let matches = self.hash_table.get(partition_id, &key);

            let has_match = !matches.is_empty();

            if has_match {
                for build_tuple in &matches {
                    results.push(Self::merge_tuples(
                        build_tuple,
                        &tuple,
                        &self.left_alias,
                        &self.right_alias,
                    ));
                }
            }

            if matches!(self.join_type, JoinType::Left | JoinType::Right | JoinType::Full) {
                for (idx, _build_tuple) in matches.iter().enumerate() {
                    self.hash_table.mark_matched(partition_id, &key, idx);
                }
            }
        }

        Ok(results)
    }

    pub fn execute(
        &self,
        _config: &ParallelConfig,
        _build_rows: usize,
        _probe_rows: usize,
    ) -> Result<Vec<Tuple>, ExecutorError> {
        self.hash_table.clear();

        let build_morsel = Morsel {
            tuples: self.build_tuples.clone(),
            start_offset: 0,
            end_offset: self.build_tuples.len(),
            partition_id: 0,
        };
        self.build_phase(build_morsel)?;

        let probe_morsel = Morsel {
            tuples: self.probe_tuples.clone(),
            start_offset: 0,
            end_offset: self.probe_tuples.len(),
            partition_id: 0,
        };
        let mut results = self.probe_phase(probe_morsel)?;

        if matches!(self.join_type, JoinType::Left | JoinType::Full) {
            let mut unmatched_build_tuples: Vec<Tuple> = Vec::new();
            for partition_id in 0..self.partition_strategy.num_partitions() {
                unmatched_build_tuples
                    .extend_from_slice(&self.hash_table.get_all_unmatched(partition_id));
            }

            let right_schema: Vec<String> =
                self.probe_tuples.first().map(|t| t.keys().cloned().collect()).unwrap_or_default();
            for build_tuple in unmatched_build_tuples {
                results.push(Self::left_only_tuple(
                    &build_tuple,
                    &self.left_alias,
                    &right_schema,
                    &self.right_alias,
                ));
            }
        }

        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
            let mut unmatched_probe_tuples: Vec<Tuple> = Vec::new();
            let mut matched_probe_keys: std::collections::HashSet<Vec<u8>> =
                std::collections::HashSet::new();

            for probe_tuple in &self.probe_tuples {
                let key = self.extract_composite_key(probe_tuple, &self.probe_keys)?;
                let partition_id = self.partition_key(&key);
                let matches = self.hash_table.get(partition_id, &key);

                if matches.is_empty() {
                    unmatched_probe_tuples.push(probe_tuple.clone());
                } else {
                    matched_probe_keys.insert(key);
                }
            }

            let left_schema: Vec<String> = vec![];
            for probe_tuple in unmatched_probe_tuples {
                results.push(Self::right_only_tuple(
                    &probe_tuple,
                    &left_schema,
                    &self.left_alias,
                    &self.right_alias,
                ));
            }
        }

        Ok(results)
    }

    pub fn degree_of_parallelism(&self) -> usize {
        self.build_side.degree_of_parallelism()
    }
}

impl ParallelOperator for ParallelHashJoin {
    fn process_morsel(&self, morsel: Morsel) -> Result<Morsel, ExecutorError> {
        self.build_phase(morsel.clone())?;
        let result = self.probe_phase(morsel)?;
        Ok(Morsel { tuples: result, start_offset: 0, end_offset: 0, partition_id: 0 })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Value;
    use crate::executor::test_helpers::TupleBuilder;

    struct MockOperator {
        tuples: Vec<Tuple>,
    }

    impl MockOperator {
        fn new(tuples: Vec<Tuple>) -> Self {
            Self { tuples }
        }
    }

    impl ParallelOperator for MockOperator {
        fn process_morsel(&self, mut morsel: Morsel) -> Result<Morsel, ExecutorError> {
            morsel.tuples = self.tuples.clone();
            Ok(morsel)
        }
    }

    struct RangeAwareMockOperator {
        tuples: Vec<Tuple>,
    }

    impl RangeAwareMockOperator {
        fn new(tuples: Vec<Tuple>) -> Self {
            Self { tuples }
        }
    }

    impl ParallelOperator for RangeAwareMockOperator {
        fn process_morsel(&self, mut morsel: Morsel) -> Result<Morsel, ExecutorError> {
            let start = morsel.start_offset.min(self.tuples.len());
            let end = morsel.end_offset.min(self.tuples.len());
            if start < end {
                morsel.tuples = self.tuples[start..end].to_vec();
            } else {
                morsel.tuples = vec![];
            }
            Ok(morsel)
        }
    }

    fn create_tuple_with_int_key(
        key_column: &str,
        key: i64,
        extra_columns: Vec<(&str, Value)>,
    ) -> Tuple {
        let mut tuple = Tuple::new();
        tuple.insert(key_column.to_string(), Value::Int(key));
        for (col, val) in extra_columns {
            tuple.insert(col.to_string(), val);
        }
        tuple
    }

    fn create_tuple_with_text_key(
        key_column: &str,
        key: &str,
        extra_columns: Vec<(&str, Value)>,
    ) -> Tuple {
        let mut tuple = Tuple::new();
        tuple.insert(key_column.to_string(), Value::Text(key.to_string()));
        for (col, val) in extra_columns {
            tuple.insert(col.to_string(), val);
        }
        tuple
    }

    #[test]
    fn test_concurrent_hash_table_insert_and_probe() {
        let ht = ConcurrentHashTable::new(4);
        let tuple = create_tuple_with_int_key("id", 1, vec![]);
        ht.insert(0, vec![1], tuple.clone());

        let result = ht.get(0, &[1]);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_concurrent_hash_table_multiple_partitions() {
        let ht = ConcurrentHashTable::new(4);
        ht.insert(0, vec![1], create_tuple_with_int_key("id", 1, vec![]));
        ht.insert(1, vec![2], create_tuple_with_int_key("id", 2, vec![]));
        ht.insert(2, vec![3], create_tuple_with_int_key("id", 3, vec![]));

        assert_eq!(ht.get(0, &[1]).len(), 1);
        assert_eq!(ht.get(1, &[2]).len(), 1);
        assert_eq!(ht.get(2, &[3]).len(), 1);
        assert_eq!(ht.get(3, &[1]).len(), 0);
    }

    #[test]
    fn test_concurrent_hash_table_clear() {
        let ht = ConcurrentHashTable::new(4);
        ht.insert(0, vec![1], create_tuple_with_int_key("id", 1, vec![]));
        ht.insert(1, vec![2], create_tuple_with_int_key("id", 2, vec![]));

        ht.clear();

        assert_eq!(ht.get(0, &[1]).len(), 0);
        assert_eq!(ht.get(1, &[2]).len(), 0);
    }

    #[test]
    fn test_parallel_hash_join_inner_basic() {
        let build_tuples = vec![
            create_tuple_with_int_key("id", 1, vec![("val", Value::Text("a".to_string()))]),
            create_tuple_with_int_key("id", 2, vec![("val", Value::Text("b".to_string()))]),
        ];

        let probe_tuples = vec![
            create_tuple_with_int_key("id", 1, vec![("amt", Value::Int(100))]),
            create_tuple_with_int_key("id", 3, vec![("amt", Value::Int(200))]),
        ];

        let build_op = Arc::new(MockOperator::new(build_tuples.clone()));
        let probe_op = Arc::new(MockOperator::new(probe_tuples.clone()));

        let mut join = ParallelHashJoin::inner(
            build_op,
            probe_op,
            4,
            vec!["id".to_string()],
            vec!["id".to_string()],
            "b".to_string(),
            "p".to_string(),
        );
        join.set_build_tuples(build_tuples);
        join.set_probe_tuples(probe_tuples);

        let config = ParallelConfig::new(4);
        let results = join.execute(&config, 2, 2).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("b.id"), Some(&Value::Int(1)));
        assert_eq!(results[0].get("b.val"), Some(&Value::Text("a".to_string())));
        assert_eq!(results[0].get("p.id"), Some(&Value::Int(1)));
        assert_eq!(results[0].get("p.amt"), Some(&Value::Int(100)));
    }

    #[test]
    fn test_parallel_hash_join_left_outer() {
        let build_tuples = vec![
            create_tuple_with_int_key("id", 1, vec![("val", Value::Text("a".to_string()))]),
            create_tuple_with_int_key("id", 2, vec![("val", Value::Text("b".to_string()))]),
        ];

        let probe_tuples = vec![
            create_tuple_with_int_key("id", 1, vec![("amt", Value::Int(100))]),
            create_tuple_with_int_key("id", 3, vec![("amt", Value::Int(200))]),
        ];

        let build_op = Arc::new(MockOperator::new(build_tuples.clone()));
        let probe_op = Arc::new(MockOperator::new(probe_tuples.clone()));

        let mut join = ParallelHashJoin::left(
            build_op,
            probe_op,
            4,
            vec!["id".to_string()],
            vec!["id".to_string()],
            "b".to_string(),
            "p".to_string(),
        );
        join.set_build_tuples(build_tuples);
        join.set_probe_tuples(probe_tuples);

        let config = ParallelConfig::new(4);
        let results = join.execute(&config, 2, 2).unwrap();

        assert_eq!(results.len(), 2);

        let matched = results.iter().find(|t| t.get("p.id") == Some(&Value::Int(1))).unwrap();
        assert_eq!(matched.get("b.id"), Some(&Value::Int(1)));
        assert_eq!(matched.get("b.val"), Some(&Value::Text("a".to_string())));
        assert_eq!(matched.get("p.amt"), Some(&Value::Int(100)));

        let unmatched_build =
            results.iter().find(|t| t.get("b.id") == Some(&Value::Int(2))).unwrap();
        assert_eq!(unmatched_build.get("b.id"), Some(&Value::Int(2)));
        assert_eq!(unmatched_build.get("b.val"), Some(&Value::Text("b".to_string())));
        assert_eq!(unmatched_build.get("p.id"), Some(&Value::Null));
        assert_eq!(unmatched_build.get("p.amt"), Some(&Value::Null));

        assert!(results.iter().all(|t| t.get("p.id") != Some(&Value::Int(3))));
    }

    #[test]
    fn test_parallel_hash_join_right_outer() {
        let build_tuples = vec![
            create_tuple_with_int_key("id", 1, vec![("val", Value::Text("a".to_string()))]),
            create_tuple_with_int_key("id", 3, vec![("val", Value::Text("c".to_string()))]),
        ];

        let probe_tuples = vec![
            create_tuple_with_int_key("id", 1, vec![("amt", Value::Int(100))]),
            create_tuple_with_int_key("id", 2, vec![("amt", Value::Int(200))]),
        ];

        let build_op = Arc::new(MockOperator::new(build_tuples.clone()));
        let probe_op = Arc::new(MockOperator::new(probe_tuples.clone()));

        let mut join = ParallelHashJoin::right(
            build_op,
            probe_op,
            4,
            vec!["id".to_string()],
            vec!["id".to_string()],
            "b".to_string(),
            "p".to_string(),
        );
        join.set_build_tuples(build_tuples);
        join.set_probe_tuples(probe_tuples);

        let config = ParallelConfig::new(4);
        let results = join.execute(&config, 2, 2).unwrap();

        assert_eq!(results.len(), 2);

        let matched = results.iter().find(|t| t.get("b.id") == Some(&Value::Int(1))).unwrap();
        assert_eq!(matched.get("p.id"), Some(&Value::Int(1)));

        let unmatched = results.iter().find(|t| t.get("p.id") == Some(&Value::Int(2))).unwrap();
        assert_eq!(unmatched.get("p.id"), Some(&Value::Int(2)));
    }

    #[test]
    fn test_parallel_hash_join_full_outer() {
        let build_tuples = vec![
            create_tuple_with_int_key("id", 1, vec![("val", Value::Text("a".to_string()))]),
            create_tuple_with_int_key("id", 4, vec![("val", Value::Text("d".to_string()))]),
        ];

        let probe_tuples = vec![
            create_tuple_with_int_key("id", 1, vec![("amt", Value::Int(100))]),
            create_tuple_with_int_key("id", 3, vec![("amt", Value::Int(200))]),
        ];

        let build_op = Arc::new(MockOperator::new(build_tuples.clone()));
        let probe_op = Arc::new(MockOperator::new(probe_tuples.clone()));

        let mut join = ParallelHashJoin::full(
            build_op,
            probe_op,
            4,
            vec!["id".to_string()],
            vec!["id".to_string()],
            "b".to_string(),
            "p".to_string(),
        );
        join.set_build_tuples(build_tuples);
        join.set_probe_tuples(probe_tuples);

        let config = ParallelConfig::new(4);
        let results = join.execute(&config, 2, 2).unwrap();

        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_parallel_hash_join_composite_key() {
        let build_tuples = vec![
            {
                let mut tuple = Tuple::new();
                tuple.insert("id".to_string(), Value::Int(1));
                tuple.insert("type".to_string(), Value::Text("A".to_string()));
                tuple.insert("val".to_string(), Value::Text("a1".to_string()));
                tuple
            },
            {
                let mut tuple = Tuple::new();
                tuple.insert("id".to_string(), Value::Int(1));
                tuple.insert("type".to_string(), Value::Text("B".to_string()));
                tuple.insert("val".to_string(), Value::Text("b1".to_string()));
                tuple
            },
        ];

        let probe_tuples = vec![
            {
                let mut tuple = Tuple::new();
                tuple.insert("id".to_string(), Value::Int(1));
                tuple.insert("type".to_string(), Value::Text("A".to_string()));
                tuple.insert("amt".to_string(), Value::Int(100));
                tuple
            },
            {
                let mut tuple = Tuple::new();
                tuple.insert("id".to_string(), Value::Int(1));
                tuple.insert("type".to_string(), Value::Text("C".to_string()));
                tuple.insert("amt".to_string(), Value::Int(200));
                tuple
            },
        ];

        let build_op = Arc::new(MockOperator::new(build_tuples.clone()));
        let probe_op = Arc::new(MockOperator::new(probe_tuples.clone()));

        let mut join = ParallelHashJoin::inner(
            build_op,
            probe_op,
            4,
            vec!["id".to_string(), "type".to_string()],
            vec!["id".to_string(), "type".to_string()],
            "b".to_string(),
            "p".to_string(),
        );
        join.set_build_tuples(build_tuples);
        join.set_probe_tuples(probe_tuples);

        let config = ParallelConfig::new(4);
        let results = join.execute(&config, 2, 2).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("b.id"), Some(&Value::Int(1)));
        assert_eq!(results[0].get("b.type"), Some(&Value::Text("A".to_string())));
        assert_eq!(results[0].get("b.val"), Some(&Value::Text("a1".to_string())));
        assert_eq!(results[0].get("p.amt"), Some(&Value::Int(100)));
    }

    #[test]
    fn test_parallel_hash_join_multiple_matches() {
        let build_tuples = vec![
            create_tuple_with_int_key("id", 1, vec![("val", Value::Text("a".to_string()))]),
            create_tuple_with_int_key("id", 1, vec![("val", Value::Text("a2".to_string()))]),
        ];

        let probe_tuples = vec![create_tuple_with_int_key("id", 1, vec![("amt", Value::Int(100))])];

        let build_op = Arc::new(MockOperator::new(build_tuples.clone()));
        let probe_op = Arc::new(MockOperator::new(probe_tuples.clone()));

        let mut join = ParallelHashJoin::inner(
            build_op,
            probe_op,
            4,
            vec!["id".to_string()],
            vec!["id".to_string()],
            "b".to_string(),
            "p".to_string(),
        );
        join.set_build_tuples(build_tuples);
        join.set_probe_tuples(probe_tuples);

        let config = ParallelConfig::new(4);
        let results = join.execute(&config, 2, 1).unwrap();

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_parallel_hash_join_empty_build() {
        let build_tuples: Vec<Tuple> = vec![];
        let probe_tuples = vec![create_tuple_with_int_key("id", 1, vec![("amt", Value::Int(100))])];

        let build_op = Arc::new(MockOperator::new(build_tuples.clone()));
        let probe_op = Arc::new(MockOperator::new(probe_tuples.clone()));

        let mut join = ParallelHashJoin::inner(
            build_op,
            probe_op,
            4,
            vec!["id".to_string()],
            vec!["id".to_string()],
            "b".to_string(),
            "p".to_string(),
        );
        join.set_build_tuples(build_tuples);
        join.set_probe_tuples(probe_tuples);

        let config = ParallelConfig::new(4);
        let results = join.execute(&config, 0, 1).unwrap();

        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_parallel_hash_join_empty_probe() {
        let build_tuples =
            vec![create_tuple_with_int_key("id", 1, vec![("val", Value::Text("a".to_string()))])];
        let probe_tuples: Vec<Tuple> = vec![];

        let build_op = Arc::new(MockOperator::new(build_tuples.clone()));
        let probe_op = Arc::new(MockOperator::new(probe_tuples.clone()));

        let mut join = ParallelHashJoin::inner(
            build_op,
            probe_op,
            4,
            vec!["id".to_string()],
            vec!["id".to_string()],
            "b".to_string(),
            "p".to_string(),
        );
        join.set_build_tuples(build_tuples);
        join.set_probe_tuples(probe_tuples);

        let config = ParallelConfig::new(4);
        let results = join.execute(&config, 1, 0).unwrap();

        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_parallel_hash_join_text_key() {
        let build_tuples = vec![
            create_tuple_with_text_key("name", "alice", vec![("age", Value::Int(30))]),
            create_tuple_with_text_key("name", "bob", vec![("age", Value::Int(25))]),
        ];

        let probe_tuples = vec![
            create_tuple_with_text_key(
                "name",
                "alice",
                vec![("city", Value::Text("NYC".to_string()))],
            ),
            create_tuple_with_text_key(
                "name",
                "charlie",
                vec![("city", Value::Text("LA".to_string()))],
            ),
        ];

        let build_op = Arc::new(MockOperator::new(build_tuples.clone()));
        let probe_op = Arc::new(MockOperator::new(probe_tuples.clone()));

        let mut join = ParallelHashJoin::inner(
            build_op,
            probe_op,
            4,
            vec!["name".to_string()],
            vec!["name".to_string()],
            "b".to_string(),
            "p".to_string(),
        );
        join.set_build_tuples(build_tuples);
        join.set_probe_tuples(probe_tuples);

        let config = ParallelConfig::new(4);
        let results = join.execute(&config, 2, 2).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("b.name"), Some(&Value::Text("alice".to_string())));
        assert_eq!(results[0].get("b.age"), Some(&Value::Int(30)));
        assert_eq!(results[0].get("p.city"), Some(&Value::Text("NYC".to_string())));
    }

    #[test]
    fn test_parallel_hash_join_bool_key() {
        let build_tuples = vec![
            {
                let mut tuple = Tuple::new();
                tuple.insert("active".to_string(), Value::Bool(true));
                tuple.insert("val".to_string(), Value::Text("yes".to_string()));
                tuple
            },
            {
                let mut tuple = Tuple::new();
                tuple.insert("active".to_string(), Value::Bool(false));
                tuple.insert("val".to_string(), Value::Text("no".to_string()));
                tuple
            },
        ];

        let probe_tuples = vec![{
            let mut tuple = Tuple::new();
            tuple.insert("active".to_string(), Value::Bool(true));
            tuple.insert("name".to_string(), Value::Text("Alice".to_string()));
            tuple
        }];

        let build_op = Arc::new(MockOperator::new(build_tuples.clone()));
        let probe_op = Arc::new(MockOperator::new(probe_tuples.clone()));

        let mut join = ParallelHashJoin::inner(
            build_op,
            probe_op,
            4,
            vec!["active".to_string()],
            vec!["active".to_string()],
            "b".to_string(),
            "p".to_string(),
        );
        join.set_build_tuples(build_tuples);
        join.set_probe_tuples(probe_tuples);

        let config = ParallelConfig::new(4);
        let results = join.execute(&config, 2, 1).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("b.active"), Some(&Value::Bool(true)));
        assert_eq!(results[0].get("p.name"), Some(&Value::Text("Alice".to_string())));
    }

    #[test]
    fn test_parallel_hash_join_partition_aware() {
        let ht = ConcurrentHashTable::new(4);

        for i in 0i64..100 {
            let tuple = create_tuple_with_int_key("id", i, vec![]);
            let key = i.to_le_bytes().to_vec();
            let partition = (key[0] as usize) & 3;
            ht.insert(partition, key, tuple);
        }

        for i in 0i64..100 {
            let key = i.to_le_bytes().to_vec();
            let partition = (key[0] as usize) & 3;
            let results = ht.get(partition, &key);
            assert_eq!(results.len(), 1);
        }
    }

    #[test]
    fn test_parallel_hash_join_no_match() {
        let build_tuples =
            vec![create_tuple_with_int_key("id", 1, vec![("val", Value::Text("a".to_string()))])];

        let probe_tuples = vec![create_tuple_with_int_key("id", 2, vec![("amt", Value::Int(100))])];

        let build_op = Arc::new(MockOperator::new(build_tuples.clone()));
        let probe_op = Arc::new(MockOperator::new(probe_tuples.clone()));

        let mut join = ParallelHashJoin::inner(
            build_op,
            probe_op,
            4,
            vec!["id".to_string()],
            vec!["id".to_string()],
            "b".to_string(),
            "p".to_string(),
        );
        join.set_build_tuples(build_tuples);
        join.set_probe_tuples(probe_tuples);

        let config = ParallelConfig::new(4);
        let results = join.execute(&config, 1, 1).unwrap();

        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_build_phase_and_probe_phase() {
        let build_tuples = vec![
            create_tuple_with_int_key("id", 1, vec![("val", Value::Text("a".to_string()))]),
            create_tuple_with_int_key("id", 2, vec![("val", Value::Text("b".to_string()))]),
        ];

        let probe_tuples = vec![create_tuple_with_int_key("id", 1, vec![("amt", Value::Int(100))])];

        let build_op = Arc::new(MockOperator::new(build_tuples.clone()));
        let probe_op = Arc::new(MockOperator::new(probe_tuples.clone()));

        let join = ParallelHashJoin::inner(
            build_op,
            probe_op,
            4,
            vec!["id".to_string()],
            vec!["id".to_string()],
            "b".to_string(),
            "p".to_string(),
        );

        let build_morsel =
            Morsel { tuples: build_tuples, start_offset: 0, end_offset: 2, partition_id: 0 };
        join.build_phase(build_morsel).unwrap();

        let probe_morsel =
            Morsel { tuples: probe_tuples, start_offset: 0, end_offset: 1, partition_id: 0 };
        let results = join.probe_phase(probe_morsel).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("b.id"), Some(&Value::Int(1)));
    }

    #[test]
    fn test_tuple_merge() {
        let left = create_tuple_with_int_key("id", 1, vec![("val", Value::Text("a".to_string()))]);
        let right = create_tuple_with_int_key("id", 1, vec![("amt", Value::Int(100))]);

        let merged = ParallelHashJoin::merge_tuples(&left, &right, "b", "p");

        assert_eq!(merged.get("b.id"), Some(&Value::Int(1)));
        assert_eq!(merged.get("b.val"), Some(&Value::Text("a".to_string())));
        assert_eq!(merged.get("p.id"), Some(&Value::Int(1)));
        assert_eq!(merged.get("p.amt"), Some(&Value::Int(100)));
    }

    #[test]
    fn test_range_aware_operator() {
        let tuples = vec![
            create_tuple_with_int_key("id", 1, vec![]),
            create_tuple_with_int_key("id", 2, vec![]),
            create_tuple_with_int_key("id", 3, vec![]),
            create_tuple_with_int_key("id", 4, vec![]),
        ];

        let op = Arc::new(RangeAwareMockOperator::new(tuples));

        let morsel1 = Morsel { tuples: vec![], start_offset: 0, end_offset: 2, partition_id: 0 };
        let result1 = op.process_morsel(morsel1).unwrap();
        assert_eq!(result1.tuples.len(), 2);

        let morsel2 = Morsel { tuples: vec![], start_offset: 2, end_offset: 4, partition_id: 0 };
        let result2 = op.process_morsel(morsel2).unwrap();
        assert_eq!(result2.tuples.len(), 2);

        let morsel3 = Morsel { tuples: vec![], start_offset: 1, end_offset: 3, partition_id: 0 };
        let result3 = op.process_morsel(morsel3).unwrap();
        assert_eq!(result3.tuples.len(), 2);
    }
}
