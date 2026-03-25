use crate::parser::ast::{
    CheckConstraint, ColumnDef, ForeignKeyDef, PartitionBoundSpec, PartitionKey, PartitionMethod,
    UniqueConstraint,
};
use crate::storage::compression::CompressionAlgorithm;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableStorageOptions {
    pub compression_algorithm: CompressionAlgorithm,
}

impl Default for TableStorageOptions {
    fn default() -> Self {
        Self { compression_algorithm: CompressionAlgorithm::Lz4 }
    }
}

impl TableStorageOptions {
    pub fn new(algorithm: CompressionAlgorithm) -> Self {
        Self { compression_algorithm: algorithm }
    }
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Option<Vec<String>>,
    pub foreign_keys: Vec<ForeignKeyDef>,
    pub check_constraints: Vec<CheckConstraint>,
    pub unique_constraints: Vec<UniqueConstraint>,
    pub partition_method: Option<PartitionMethod>,
    pub partition_keys: Vec<PartitionKey>,
    pub is_partition: bool,
    pub parent_table: Option<String>,
    pub partition_bound: Option<PartitionBoundSpec>,
    pub storage_options: TableStorageOptions,
}

impl TableSchema {
    pub fn new(name: String, columns: Vec<ColumnDef>) -> Self {
        Self {
            name,
            columns,
            primary_key: None,
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
            unique_constraints: Vec::new(),
            partition_method: None,
            partition_keys: Vec::new(),
            is_partition: false,
            parent_table: None,
            partition_bound: None,
            storage_options: TableStorageOptions::default(),
        }
    }

    pub fn with_constraints(
        name: String,
        columns: Vec<ColumnDef>,
        primary_key: Option<Vec<String>>,
        foreign_keys: Vec<ForeignKeyDef>,
    ) -> Self {
        Self {
            name,
            columns,
            primary_key,
            foreign_keys,
            check_constraints: Vec::new(),
            unique_constraints: Vec::new(),
            partition_method: None,
            partition_keys: Vec::new(),
            is_partition: false,
            parent_table: None,
            partition_bound: None,
            storage_options: TableStorageOptions::default(),
        }
    }

    pub fn with_all_constraints(
        name: String,
        columns: Vec<ColumnDef>,
        primary_key: Option<Vec<String>>,
        foreign_keys: Vec<ForeignKeyDef>,
        check_constraints: Vec<CheckConstraint>,
        unique_constraints: Vec<UniqueConstraint>,
    ) -> Self {
        Self {
            name,
            columns,
            primary_key,
            foreign_keys,
            check_constraints,
            unique_constraints,
            partition_method: None,
            partition_keys: Vec::new(),
            is_partition: false,
            parent_table: None,
            partition_bound: None,
            storage_options: TableStorageOptions::default(),
        }
    }

    pub fn with_partition(
        name: String,
        columns: Vec<ColumnDef>,
        partition_method: PartitionMethod,
        partition_keys: Vec<PartitionKey>,
    ) -> Self {
        Self {
            name,
            columns,
            primary_key: None,
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
            unique_constraints: Vec::new(),
            partition_method: Some(partition_method),
            partition_keys,
            is_partition: false,
            parent_table: None,
            partition_bound: None,
            storage_options: TableStorageOptions::default(),
        }
    }

    pub fn as_partition(name: String, parent_table: String, bound: PartitionBoundSpec) -> Self {
        Self {
            name,
            columns: Vec::new(),
            primary_key: None,
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
            unique_constraints: Vec::new(),
            partition_method: None,
            partition_keys: Vec::new(),
            is_partition: true,
            parent_table: Some(parent_table),
            partition_bound: Some(bound),
            storage_options: TableStorageOptions::default(),
        }
    }

    pub fn with_storage_options(
        name: String,
        columns: Vec<ColumnDef>,
        storage_options: TableStorageOptions,
    ) -> Self {
        Self {
            name,
            columns,
            primary_key: None,
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
            unique_constraints: Vec::new(),
            partition_method: None,
            partition_keys: Vec::new(),
            is_partition: false,
            parent_table: None,
            partition_bound: None,
            storage_options,
        }
    }

    pub fn compression_algorithm(&self) -> CompressionAlgorithm {
        self.storage_options.compression_algorithm
    }

    pub fn set_compression_algorithm(&mut self, algorithm: CompressionAlgorithm) {
        self.storage_options.compression_algorithm = algorithm;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{
        BinaryOperator, CheckConstraint, ColumnDef, DataType, Expr, ForeignKeyAction, ForeignKeyDef,
    };

    fn create_col_def(name: &str, data_type: DataType) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            is_primary_key: false,
            is_unique: false,
            is_auto_increment: false,
            is_not_null: false,
            default_value: None,
            foreign_key: None,
        }
    }

    #[test]
    fn test_new_table_schema() {
        let cols = vec![create_col_def("id", DataType::Int)];
        let schema = TableSchema::new("users".to_string(), cols.clone());

        assert_eq!(schema.name, "users");
        assert_eq!(schema.columns, cols);
        assert_eq!(schema.primary_key, None);
        assert!(schema.foreign_keys.is_empty());
        assert!(schema.check_constraints.is_empty());
        assert!(schema.unique_constraints.is_empty());
    }

    #[test]
    fn test_with_constraints() {
        let cols = vec![create_col_def("id", DataType::Int)];
        let pk = Some(vec!["id".to_string()]);
        let fk = vec![ForeignKeyDef {
            ref_table: "orders".to_string(),
            columns: vec!["order_id".to_string()],
            ref_columns: vec!["id".to_string()],
            on_delete: ForeignKeyAction::Restrict,
            on_update: ForeignKeyAction::Restrict,
        }];

        let schema = TableSchema::with_constraints(
            "products".to_string(),
            cols.clone(),
            pk.clone(),
            fk.clone(),
        );

        assert_eq!(schema.name, "products");
        assert_eq!(schema.columns, cols);
        assert_eq!(schema.primary_key, pk);
        assert_eq!(schema.foreign_keys, fk);
        assert!(schema.check_constraints.is_empty());
        assert!(schema.unique_constraints.is_empty());
    }

    #[test]
    fn test_with_all_constraints() {
        let cols = vec![create_col_def("id", DataType::Int)];
        let pk = Some(vec!["id".to_string()]);
        let fk = vec![ForeignKeyDef {
            ref_table: "orders".to_string(),
            columns: vec!["order_id".to_string()],
            ref_columns: vec!["id".to_string()],
            on_delete: ForeignKeyAction::Restrict,
            on_update: ForeignKeyAction::Restrict,
        }];
        let check_c = vec![CheckConstraint {
            name: Some("age_check".to_string()),
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expr::Number(18)),
            },
        }];
        let unique_c = vec![UniqueConstraint {
            name: Some("email_unique".to_string()),
            columns: vec!["email".to_string()],
        }];

        let schema = TableSchema::with_all_constraints(
            "customers".to_string(),
            cols.clone(),
            pk.clone(),
            fk.clone(),
            check_c.clone(),
            unique_c.clone(),
        );

        assert_eq!(schema.name, "customers");
        assert_eq!(schema.columns, cols);
        assert_eq!(schema.primary_key, pk);
        assert_eq!(schema.foreign_keys, fk);
        assert_eq!(schema.check_constraints, check_c);
        assert_eq!(schema.unique_constraints, unique_c);
    }

    #[test]
    fn test_with_all_constraints_empty_options() {
        let cols = vec![create_col_def("id", DataType::Int)];

        let schema = TableSchema::with_all_constraints(
            "empty_constraints".to_string(),
            cols.clone(),
            None,
            vec![],
            vec![],
            vec![],
        );

        assert_eq!(schema.name, "empty_constraints");
        assert_eq!(schema.columns, cols);
        assert_eq!(schema.primary_key, None);
        assert!(schema.foreign_keys.is_empty());
        assert!(schema.check_constraints.is_empty());
        assert!(schema.unique_constraints.is_empty());
    }

    #[test]
    fn test_table_storage_options_default() {
        let opts = TableStorageOptions::default();
        assert_eq!(opts.compression_algorithm, CompressionAlgorithm::Lz4);
    }

    #[test]
    fn test_table_storage_options_with_zstd() {
        let opts = TableStorageOptions::new(CompressionAlgorithm::Zstd);
        assert_eq!(opts.compression_algorithm, CompressionAlgorithm::Zstd);
    }

    #[test]
    fn test_table_schema_default_compression() {
        let cols = vec![create_col_def("id", DataType::Int)];
        let schema = TableSchema::new("users".to_string(), cols);

        assert_eq!(schema.compression_algorithm(), CompressionAlgorithm::Lz4);
    }

    #[test]
    fn test_table_schema_with_storage_options() {
        let cols = vec![create_col_def("id", DataType::Int)];
        let opts = TableStorageOptions::new(CompressionAlgorithm::Zstd);
        let schema = TableSchema::with_storage_options("compressed_table".to_string(), cols, opts);

        assert_eq!(schema.compression_algorithm(), CompressionAlgorithm::Zstd);
    }

    #[test]
    fn test_table_schema_set_compression() {
        let cols = vec![create_col_def("id", DataType::Int)];
        let mut schema = TableSchema::new("users".to_string(), cols);

        assert_eq!(schema.compression_algorithm(), CompressionAlgorithm::Lz4);

        schema.set_compression_algorithm(CompressionAlgorithm::Zstd);
        assert_eq!(schema.compression_algorithm(), CompressionAlgorithm::Zstd);
    }

    #[test]
    fn test_table_schema_compression_none() {
        let cols = vec![create_col_def("id", DataType::Int)];
        let opts = TableStorageOptions::new(CompressionAlgorithm::None);
        let schema =
            TableSchema::with_storage_options("uncompressed_table".to_string(), cols, opts);

        assert_eq!(schema.compression_algorithm(), CompressionAlgorithm::None);
    }
}
