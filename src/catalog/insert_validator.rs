use super::{CompositeTypeDef, TableSchema, Tuple, UniqueValidator, Value};
use crate::parser::ast::{ColumnDef, DataType, EnumTypeDef, Expr};
use crate::transaction::TransactionManager;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct InsertValidator;

impl InsertValidator {
    pub fn resolve_value(
        col: &ColumnDef,
        i: usize,
        values: &[Expr],
        table: &str,
        sequences: &Arc<RwLock<HashMap<String, i64>>>,
        enum_types: &Arc<RwLock<HashMap<String, EnumTypeDef>>>,
        composite_types: &Arc<RwLock<HashMap<String, CompositeTypeDef>>>,
    ) -> Result<Value, String> {
        if i < values.len() {
            Self::parse_and_validate_value(&values[i], col, enum_types, composite_types)
        } else if col.is_auto_increment || col.data_type == DataType::Serial {
            Self::generate_sequence(table, &col.name, sequences)
        } else if let Some(ref default_expr) = col.default_value {
            Self::parse_value(default_expr, enum_types, composite_types)
        } else {
            Err(format!("Column '{}' has no default value", col.name))
        }
    }

    fn parse_value(
        expr: &Expr,
        _enum_types: &Arc<RwLock<HashMap<String, EnumTypeDef>>>,
        _composite_types: &Arc<RwLock<HashMap<String, CompositeTypeDef>>>,
    ) -> Result<Value, String> {
        match expr {
            Expr::Number(n) => Ok(Value::Int(*n)),
            Expr::Float(f) => Ok(Value::Float(*f)),
            Expr::String(s) => Ok(Value::Text(s.clone())),
            Expr::Null => Ok(Value::Null),
            Expr::Array(arr) => {
                let mut values = Vec::new();
                for elem in arr {
                    values.push(Self::parse_value(elem, _enum_types, _composite_types)?);
                }
                Ok(Value::Array(values))
            }
            Expr::Row(row_exprs) => {
                let mut values = Vec::new();
                for elem in row_exprs {
                    values.push(Self::parse_value(elem, _enum_types, _composite_types)?);
                }
                Ok(Value::Text(format!(
                    "ROW({})",
                    values.iter().map(|v| format!("{}", v)).collect::<Vec<_>>().join(", ")
                )))
            }
            _ => Err("Invalid value expression".to_string()),
        }
    }

    fn parse_and_validate_value(
        expr: &Expr,
        col: &ColumnDef,
        enum_types: &Arc<RwLock<HashMap<String, EnumTypeDef>>>,
        composite_types: &Arc<RwLock<HashMap<String, CompositeTypeDef>>>,
    ) -> Result<Value, String> {
        let val = Self::parse_value(expr, enum_types, composite_types)?;
        if matches!(val, Value::Null) {
            return Ok(val);
        }
        match (&col.data_type, &val) {
            (DataType::Int, Value::Int(_))
            | (DataType::Serial, Value::Int(_))
            | (DataType::Float, Value::Float(_))
            | (DataType::Float, Value::Int(_))
            | (DataType::Text, Value::Text(_))
            | (DataType::Varchar(_), Value::Text(_))
            | (DataType::Array(_), Value::Array(_)) => Ok(val),
            (DataType::Json, Value::Text(s)) => Ok(Value::Json(s.clone())),
            (DataType::Jsonb, Value::Text(s)) => Ok(Value::Json(s.clone())),
            (DataType::Enum(type_name), Value::Text(label)) => {
                let enum_types = enum_types.read().unwrap();
                if let Some(def) = enum_types.get(type_name) {
                    if let Some(index) = def.labels.iter().position(|l| l == label) {
                        Ok(Value::Enum(super::EnumValue {
                            type_name: type_name.clone(),
                            index: index as i32,
                        }))
                    } else {
                        Err(format!("invalid enum label '{}' for type '{}'", label, type_name))
                    }
                } else {
                    Err(format!("type '{}' does not exist", type_name))
                }
            }
            (DataType::Composite(type_name), Value::Text(_)) => {
                let composite_types = composite_types.read().unwrap();
                if let Some(def) = composite_types.get(type_name) {
                    Self::validate_composite_value(&val, def)
                } else {
                    Err(format!("type '{}' does not exist", type_name))
                }
            }
            (DataType::Composite(type_name), Value::Composite(_)) => {
                let composite_types = composite_types.read().unwrap();
                if let Some(def) = composite_types.get(type_name) {
                    Self::validate_composite_value(&val, def)
                } else {
                    Err(format!("type '{}' does not exist", type_name))
                }
            }
            _ => Err(format!("Type mismatch for column '{}'", col.name)),
        }
    }

    fn validate_composite_value(val: &Value, def: &CompositeTypeDef) -> Result<Value, String> {
        match val {
            Value::Text(s) if s.starts_with("ROW(") => {
                let inner = &s[4..s.len() - 1];
                let field_strs: Vec<&str> =
                    if inner.is_empty() { vec![] } else { inner.split(", ").collect() };

                if field_strs.len() != def.fields.len() {
                    return Err(format!(
                        "Composite type '{}' requires {} fields, got {}",
                        def.type_name,
                        def.fields.len(),
                        field_strs.len()
                    ));
                }
                Ok(val.clone())
            }
            Value::Composite(c) => {
                if c.type_name != def.type_name {
                    return Err(format!(
                        "Composite value type '{}' does not match column type '{}'",
                        c.type_name, def.type_name
                    ));
                }
                if c.fields.len() != def.fields.len() {
                    return Err(format!(
                        "Composite type '{}' requires {} fields, got {}",
                        def.type_name,
                        def.fields.len(),
                        c.fields.len()
                    ));
                }
                Ok(val.clone())
            }
            _ => Err(format!("Invalid composite value: {:?}", val)),
        }
    }

    fn generate_sequence(
        table: &str,
        col_name: &str,
        sequences: &Arc<RwLock<HashMap<String, i64>>>,
    ) -> Result<Value, String> {
        let seq_key = format!("{}_{}", table, col_name);
        let mut seqs = sequences.write().unwrap();
        let next_val = seqs.entry(seq_key).or_insert(0);
        *next_val += 1;
        Ok(Value::Int(*next_val))
    }

    pub fn validate_not_null(schema: &TableSchema, tuple_data: &[Value]) -> Result<(), String> {
        for (i, col) in schema.columns.iter().enumerate() {
            if (col.is_not_null || col.is_primary_key) && tuple_data[i] == Value::Null {
                return Err(format!("Column '{}' cannot be NULL", col.name));
            }
        }
        Ok(())
    }

    pub fn validate_primary_key(
        schema: &TableSchema,
        tuple_data: &[Value],
        table: &str,
        data: &HashMap<String, Vec<Tuple>>,
        txn_mgr: &TransactionManager,
    ) -> Result<(), String> {
        let Some(ref pk_cols) = schema.primary_key else { return Ok(()) };

        let pk_indices: Vec<usize> = pk_cols
            .iter()
            .map(|col| schema.columns.iter().position(|c| &c.name == col).unwrap())
            .collect();

        for &idx in &pk_indices {
            if tuple_data[idx] == Value::Null {
                return Err(format!(
                    "Primary key column '{}' cannot be NULL",
                    schema.columns[idx].name
                ));
            }
        }

        if let Some(tuples) = data.get(table) {
            let snapshot = txn_mgr.get_snapshot();
            for existing in tuples {
                if existing.header.is_visible(&snapshot, txn_mgr)
                    && pk_indices.iter().all(|&idx| existing.data[idx] == tuple_data[idx])
                {
                    return Err("Primary key violation: duplicate key value".to_string());
                }
            }
        }
        Ok(())
    }

    pub fn validate_foreign_keys(
        schema: &TableSchema,
        tuple_data: &[Value],
        data: &HashMap<String, Vec<Tuple>>,
        tables: &HashMap<String, TableSchema>,
        txn_mgr: &TransactionManager,
    ) -> Result<(), String> {
        for fk in &schema.foreign_keys {
            let fk_indices: Vec<usize> = fk
                .columns
                .iter()
                .map(|col| schema.columns.iter().position(|c| &c.name == col).unwrap())
                .collect();

            let fk_values: Vec<Value> =
                fk_indices.iter().map(|&idx| tuple_data[idx].clone()).collect();

            let ref_schema = tables
                .get(&fk.ref_table)
                .ok_or_else(|| format!("Referenced table '{}' does not exist", fk.ref_table))?;

            let ref_indices: Vec<usize> = fk
                .ref_columns
                .iter()
                .map(|col| ref_schema.columns.iter().position(|c| &c.name == col).unwrap())
                .collect();

            let ref_tuples = data
                .get(&fk.ref_table)
                .ok_or_else(|| format!("Referenced table '{}' has no data", fk.ref_table))?;

            let snapshot = txn_mgr.get_snapshot();
            let found = ref_tuples.iter().any(|ref_tuple| {
                ref_tuple.header.is_visible(&snapshot, txn_mgr)
                    && ref_indices
                        .iter()
                        .enumerate()
                        .all(|(i, &ref_idx)| ref_tuple.data[ref_idx] == fk_values[i])
            });

            if !found {
                return Err(format!(
                    "Foreign key violation: referenced row does not exist in table '{}'",
                    fk.ref_table
                ));
            }
        }
        Ok(())
    }

    pub fn validate_unique(
        schema: &TableSchema,
        tuple_data: &[Value],
        table: &str,
        data: &HashMap<String, Vec<Tuple>>,
        txn_mgr: &TransactionManager,
    ) -> Result<(), String> {
        let Some(tuples) = data.get(table) else { return Ok(()) };

        let snapshot = txn_mgr.get_snapshot();
        let visible_tuples: Vec<Tuple> =
            tuples.iter().filter(|t| t.header.is_visible(&snapshot, txn_mgr)).cloned().collect();

        for (i, col) in schema.columns.iter().enumerate() {
            if col.is_unique {
                let constraint = crate::parser::ast::UniqueConstraint {
                    name: Some(format!("{}_{}_unique", table, col.name)),
                    columns: vec![col.name.clone()],
                };
                UniqueValidator::validate(&constraint, tuple_data, &visible_tuples, &[i])?;
            }
        }

        for unique_constraint in &schema.unique_constraints {
            let indices: Vec<usize> = unique_constraint
                .columns
                .iter()
                .map(|col| schema.columns.iter().position(|c| &c.name == col).unwrap())
                .collect();
            UniqueValidator::validate(unique_constraint, tuple_data, &visible_tuples, &indices)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{TableSchema, Value};
    use crate::parser::ast::{ColumnDef, DataType, Expr};
    use std::sync::{Arc, RwLock};

    // Helper function to create a ColumnDef
    fn create_col_def(
        name: &str,
        data_type: DataType,
        is_not_null: bool,
        is_primary_key: bool,
        is_auto_increment: bool,
        default_value: Option<Expr>,
    ) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            is_primary_key,
            is_not_null,
            is_unique: false,
            is_auto_increment,
            default_value,
            foreign_key: None,
        }
    }

    #[test]
    fn test_resolve_value_explicit_value() {
        let col = create_col_def("id", DataType::Int, false, false, false, None);
        let values = vec![Expr::Number(10)];
        let sequences = Arc::new(RwLock::new(HashMap::new()));
        let enum_types = Arc::new(RwLock::new(HashMap::new()));
        let composite_types = Arc::new(RwLock::new(HashMap::new()));
        let resolved = InsertValidator::resolve_value(
            &col,
            0,
            &values,
            "test_table",
            &sequences,
            &enum_types,
            &composite_types,
        )
        .unwrap();
        assert_eq!(resolved, Value::Int(10));
    }

    #[test]
    fn test_resolve_value_type_mismatch() {
        let col = create_col_def("id", DataType::Int, false, false, false, None);
        let values = vec![Expr::String("hello".to_string())];
        let sequences = Arc::new(RwLock::new(HashMap::new()));
        let enum_types = Arc::new(RwLock::new(HashMap::new()));
        let composite_types = Arc::new(RwLock::new(HashMap::new()));
        let resolved = InsertValidator::resolve_value(
            &col,
            0,
            &values,
            "test_table",
            &sequences,
            &enum_types,
            &composite_types,
        );
        assert!(resolved.is_err());
        assert_eq!(resolved.unwrap_err(), "Type mismatch for column 'id'");
    }

    #[test]
    fn test_resolve_value_auto_increment() {
        let col = create_col_def("id", DataType::Serial, false, false, true, None);
        let values = vec![];
        let sequences = Arc::new(RwLock::new(HashMap::new()));
        let enum_types = Arc::new(RwLock::new(HashMap::new()));
        let composite_types = Arc::new(RwLock::new(HashMap::new()));
        let resolved1 = InsertValidator::resolve_value(
            &col,
            0,
            &values,
            "test_table",
            &sequences,
            &enum_types,
            &composite_types,
        )
        .unwrap();
        assert_eq!(resolved1, Value::Int(1));

        let resolved2 = InsertValidator::resolve_value(
            &col,
            0,
            &values,
            "test_table",
            &sequences,
            &enum_types,
            &composite_types,
        )
        .unwrap();
        assert_eq!(resolved2, Value::Int(2));
    }

    #[test]
    fn test_resolve_value_default_value() {
        let col = create_col_def(
            "name",
            DataType::Text,
            false,
            false,
            false,
            Some(Expr::String("default_name".to_string())),
        );
        let values = vec![];
        let sequences = Arc::new(RwLock::new(HashMap::new()));
        let enum_types = Arc::new(RwLock::new(HashMap::new()));
        let composite_types = Arc::new(RwLock::new(HashMap::new()));
        let resolved = InsertValidator::resolve_value(
            &col,
            0,
            &values,
            "test_table",
            &sequences,
            &enum_types,
            &composite_types,
        )
        .unwrap();
        assert_eq!(resolved, Value::Text("default_name".to_string()));
    }

    #[test]
    fn test_resolve_value_no_value_no_default_no_auto_increment() {
        let col = create_col_def("age", DataType::Int, false, false, false, None);
        let values = vec![];
        let sequences = Arc::new(RwLock::new(HashMap::new()));
        let enum_types = Arc::new(RwLock::new(HashMap::new()));
        let composite_types = Arc::new(RwLock::new(HashMap::new()));
        let resolved = InsertValidator::resolve_value(
            &col,
            0,
            &values,
            "test_table",
            &sequences,
            &enum_types,
            &composite_types,
        );
        assert!(resolved.is_err());
        assert_eq!(resolved.unwrap_err(), "Column 'age' has no default value");
    }

    #[test]
    fn test_validate_not_null_success() {
        let col1 = create_col_def("id", DataType::Int, true, false, false, None);
        let col2 = create_col_def("name", DataType::Text, false, false, false, None);
        let schema = TableSchema {
            name: "test_table".to_string(),
            columns: vec![col1, col2],
            primary_key: None,
            foreign_keys: vec![],
            unique_constraints: vec![],
            check_constraints: vec![],
            partition_method: None,
            partition_keys: vec![],
            is_partition: false,
            parent_table: None,
            partition_bound: None,
        };
        let tuple_data = vec![Value::Int(1), Value::Text("test".to_string())];
        assert!(InsertValidator::validate_not_null(&schema, &tuple_data).is_ok());
    }

    #[test]
    fn test_validate_not_null_failure_not_null_column() {
        let col1 = create_col_def("id", DataType::Int, true, false, false, None);
        let col2 = create_col_def("name", DataType::Text, false, false, false, None);
        let schema = TableSchema {
            name: "test_table".to_string(),
            columns: vec![col1, col2],
            primary_key: None,
            foreign_keys: vec![],
            unique_constraints: vec![],
            check_constraints: vec![],
            partition_method: None,
            partition_keys: vec![],
            is_partition: false,
            parent_table: None,
            partition_bound: None,
        };
        let tuple_data = vec![Value::Null, Value::Text("test".to_string())];
        let result = InsertValidator::validate_not_null(&schema, &tuple_data);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Column 'id' cannot be NULL");
    }

    #[test]
    fn test_validate_not_null_failure_primary_key_column() {
        let col1 = create_col_def("id", DataType::Int, false, true, false, None);
        let col2 = create_col_def("name", DataType::Text, false, false, false, None);
        let schema = TableSchema {
            name: "test_table".to_string(),
            columns: vec![col1, col2],
            primary_key: Some(vec!["id".to_string()]),
            foreign_keys: vec![],
            unique_constraints: vec![],
            check_constraints: vec![],
            partition_method: None,
            partition_keys: vec![],
            is_partition: false,
            parent_table: None,
            partition_bound: None,
        };
        let tuple_data = vec![Value::Null, Value::Text("test".to_string())];
        let result = InsertValidator::validate_not_null(&schema, &tuple_data);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Column 'id' cannot be NULL");
    }

    #[test]
    fn test_validate_not_null_empty_schema() {
        let schema = TableSchema {
            name: "test_table".to_string(),
            columns: vec![],
            primary_key: None,
            foreign_keys: vec![],
            unique_constraints: vec![],
            check_constraints: vec![],
            partition_method: None,
            partition_keys: vec![],
            is_partition: false,
            parent_table: None,
            partition_bound: None,
        };
        let tuple_data = vec![];
        assert!(InsertValidator::validate_not_null(&schema, &tuple_data).is_ok());
    }
}
