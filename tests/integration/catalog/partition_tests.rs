use std::sync::Arc;
use vaultgres::catalog::*;
use vaultgres::parser::ast::{
    AttachPartitionStmt, ColumnDef, DataType, DetachPartitionStmt, PartitionBoundSpec,
    PartitionHashBound, PartitionKey, PartitionMethod, PartitionRangeBound,
};

#[test]
fn test_create_partitioned_table() {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("order_date".to_string(), DataType::Date),
    ];

    let schema = TableSchema::with_partition(
        "orders".to_string(),
        columns,
        PartitionMethod::Range,
        vec![PartitionKey { column: "order_date".to_string(), opclass: None }],
    );

    assert!(catalog.create_partitioned_table(schema).is_ok());
    assert!(catalog.get_table("orders").is_some());

    let table = catalog.get_table("orders").unwrap();
    assert!(table.partition_method.is_some());
    assert_eq!(table.partition_method.unwrap(), PartitionMethod::Range);
    assert_eq!(table.partition_keys.len(), 1);
}

#[test]
fn test_create_partition() {
    let catalog = Catalog::new();
    let parent_columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("order_date".to_string(), DataType::Date),
    ];

    let parent_schema = TableSchema::with_partition(
        "orders".to_string(),
        parent_columns,
        PartitionMethod::Range,
        vec![PartitionKey { column: "order_date".to_string(), opclass: None }],
    );

    catalog.create_partitioned_table(parent_schema).unwrap();

    let partition_schema = TableSchema::as_partition(
        "orders_2024_01".to_string(),
        "orders".to_string(),
        PartitionBoundSpec::Range(PartitionRangeBound { from_values: vec![], to_values: vec![] }),
    );

    assert!(catalog.create_partition(partition_schema).is_ok());
    assert!(catalog.get_table("orders_2024_01").is_some());

    let partition = catalog.get_table("orders_2024_01").unwrap();
    assert!(partition.is_partition);
    assert_eq!(partition.parent_table, Some("orders".to_string()));
}

#[test]
fn test_create_partition_hash() {
    let catalog = Catalog::new();
    let parent_columns = vec![ColumnDef::new("customer_id".to_string(), DataType::Int)];

    let parent_schema = TableSchema::with_partition(
        "customers".to_string(),
        parent_columns,
        PartitionMethod::Hash,
        vec![PartitionKey { column: "customer_id".to_string(), opclass: None }],
    );

    catalog.create_partitioned_table(parent_schema).unwrap();

    let partition_schema = TableSchema::as_partition(
        "customers_0".to_string(),
        "customers".to_string(),
        PartitionBoundSpec::Hash(PartitionHashBound { modulus: 4, remainder: 0 }),
    );

    assert!(catalog.create_partition(partition_schema).is_ok());

    let partition = catalog.get_table("customers_0").unwrap();
    match &partition.partition_bound {
        Some(PartitionBoundSpec::Hash(hash_bound)) => {
            assert_eq!(hash_bound.modulus, 4);
            assert_eq!(hash_bound.remainder, 0);
        }
        _ => panic!("Expected Hash bound"),
    }
}

#[test]
fn test_create_partition_list() {
    let catalog = Catalog::new();
    let parent_columns = vec![ColumnDef::new("region".to_string(), DataType::Text)];

    let parent_schema = TableSchema::with_partition(
        "sales".to_string(),
        parent_columns,
        PartitionMethod::List,
        vec![PartitionKey { column: "region".to_string(), opclass: None }],
    );

    catalog.create_partitioned_table(parent_schema).unwrap();

    let partition_schema = TableSchema::as_partition(
        "sales_east".to_string(),
        "sales".to_string(),
        PartitionBoundSpec::List(vaultgres::parser::ast::PartitionListBound { values: vec![] }),
    );

    assert!(catalog.create_partition(partition_schema).is_ok());
}

#[test]
fn test_create_partition_default() {
    let catalog = Catalog::new();
    let parent_columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("order_date".to_string(), DataType::Date),
    ];

    let parent_schema = TableSchema::with_partition(
        "orders".to_string(),
        parent_columns,
        PartitionMethod::Range,
        vec![PartitionKey { column: "order_date".to_string(), opclass: None }],
    );

    catalog.create_partitioned_table(parent_schema).unwrap();

    let default_partition = TableSchema::as_partition(
        "orders_default".to_string(),
        "orders".to_string(),
        PartitionBoundSpec::Default,
    );

    assert!(catalog.create_partition(default_partition).is_ok());
}

#[test]
fn test_attach_partition() {
    let catalog = Catalog::new();

    let parent_columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("order_date".to_string(), DataType::Date),
    ];

    let parent_schema = TableSchema::with_partition(
        "orders".to_string(),
        parent_columns,
        PartitionMethod::Range,
        vec![PartitionKey { column: "order_date".to_string(), opclass: None }],
    );

    catalog.create_partitioned_table(parent_schema).unwrap();

    let partition_columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("order_date".to_string(), DataType::Date),
    ];

    catalog.create_table("orders_2024_01".to_string(), partition_columns.clone()).unwrap();

    let attach_stmt = AttachPartitionStmt {
        parent_table: "orders".to_string(),
        partition_name: "orders_2024_01".to_string(),
        bound: PartitionBoundSpec::Range(PartitionRangeBound {
            from_values: vec![],
            to_values: vec![],
        }),
    };

    assert!(catalog.attach_partition(&attach_stmt).is_ok());

    let partition = catalog.get_table("orders_2024_01").unwrap();
    assert!(partition.is_partition);
    assert_eq!(partition.parent_table, Some("orders".to_string()));
}

#[test]
fn test_detach_partition() {
    let catalog = Catalog::new();

    let parent_columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("order_date".to_string(), DataType::Date),
    ];

    let parent_schema = TableSchema::with_partition(
        "orders".to_string(),
        parent_columns,
        PartitionMethod::Range,
        vec![PartitionKey { column: "order_date".to_string(), opclass: None }],
    );

    catalog.create_partitioned_table(parent_schema).unwrap();

    let partition_schema = TableSchema::as_partition(
        "orders_2024_01".to_string(),
        "orders".to_string(),
        PartitionBoundSpec::Range(PartitionRangeBound { from_values: vec![], to_values: vec![] }),
    );

    catalog.create_partition(partition_schema).unwrap();

    let detach_stmt = DetachPartitionStmt {
        parent_table: "orders".to_string(),
        partition_name: "orders_2024_01".to_string(),
    };

    assert!(catalog.detach_partition(&detach_stmt).is_ok());

    let partition = catalog.get_table("orders_2024_01").unwrap();
    assert!(!partition.is_partition);
    assert_eq!(partition.parent_table, None);
}

#[test]
fn test_attach_partition_nonexistent_parent() {
    let catalog = Catalog::new();
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];
    catalog.create_table("t".to_string(), columns).unwrap();

    let attach_stmt = AttachPartitionStmt {
        parent_table: "nonexistent".to_string(),
        partition_name: "t".to_string(),
        bound: PartitionBoundSpec::Default,
    };

    assert!(catalog.attach_partition(&attach_stmt).is_err());
}

#[test]
fn test_detach_partition_not_attached() {
    let catalog = Catalog::new();
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];
    catalog.create_table("t".to_string(), columns).unwrap();

    let detach_stmt =
        DetachPartitionStmt { parent_table: "orders".to_string(), partition_name: "t".to_string() };

    assert!(catalog.detach_partition(&detach_stmt).is_err());
}

#[test]
fn test_multiple_partitions_per_parent() {
    let catalog = Catalog::new();

    let parent_columns = vec![ColumnDef::new("customer_id".to_string(), DataType::Int)];

    let parent_schema = TableSchema::with_partition(
        "customers".to_string(),
        parent_columns,
        PartitionMethod::Hash,
        vec![PartitionKey { column: "customer_id".to_string(), opclass: None }],
    );

    catalog.create_partitioned_table(parent_schema).unwrap();

    for i in 0..4 {
        let partition_schema = TableSchema::as_partition(
            format!("customers_{}", i),
            "customers".to_string(),
            PartitionBoundSpec::Hash(PartitionHashBound { modulus: 4, remainder: i as u64 }),
        );

        assert!(catalog.create_partition(partition_schema).is_ok());
    }

    let table = catalog.get_table("customers").unwrap();
    assert!(table.partition_method.is_some());
    assert_eq!(table.partition_method.unwrap(), PartitionMethod::Hash);
}

#[test]
fn test_is_partitioned_table() {
    let catalog = Catalog::new();
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("regular_table".to_string(), columns.clone()).unwrap();

    let partition_schema = TableSchema::with_partition(
        "partitioned_table".to_string(),
        columns,
        PartitionMethod::Range,
        vec![PartitionKey { column: "id".to_string(), opclass: None }],
    );

    catalog.create_partitioned_table(partition_schema).unwrap();

    assert!(!catalog.is_partitioned_table("regular_table"));
    assert!(catalog.is_partitioned_table("partitioned_table"));
    assert!(!catalog.is_partitioned_table("nonexistent"));
}

#[test]
fn test_is_partition() {
    let catalog = Catalog::new();
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("regular_table".to_string(), columns.clone()).unwrap();

    let parent_columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];
    let parent_schema = TableSchema::with_partition(
        "parent".to_string(),
        parent_columns,
        PartitionMethod::Range,
        vec![PartitionKey { column: "id".to_string(), opclass: None }],
    );
    catalog.create_partitioned_table(parent_schema).unwrap();

    let partition_schema = TableSchema::as_partition(
        "my_partition".to_string(),
        "parent".to_string(),
        PartitionBoundSpec::Default,
    );

    catalog.create_partition(partition_schema).unwrap();

    assert!(!catalog.is_partition("regular_table"));
    assert!(catalog.is_partition("my_partition"));
    assert!(!catalog.is_partition("nonexistent"));
}

#[test]
fn test_get_parent_table() {
    let catalog = Catalog::new();
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    let parent_columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];
    let parent_schema = TableSchema::with_partition(
        "parent_table".to_string(),
        parent_columns,
        PartitionMethod::Range,
        vec![PartitionKey { column: "id".to_string(), opclass: None }],
    );
    catalog.create_partitioned_table(parent_schema).unwrap();

    let partition_schema = TableSchema::as_partition(
        "my_partition".to_string(),
        "parent_table".to_string(),
        PartitionBoundSpec::Default,
    );

    catalog.create_partition(partition_schema).unwrap();

    assert_eq!(catalog.get_parent_table("my_partition"), Some("parent_table".to_string()));
    assert_eq!(catalog.get_parent_table("regular_table"), None);
    assert_eq!(catalog.get_parent_table("nonexistent"), None);
}

#[test]
fn test_create_partitioned_table_duplicate() {
    let catalog = Catalog::new();
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    let schema = TableSchema::with_partition(
        "orders".to_string(),
        columns.clone(),
        PartitionMethod::Range,
        vec![PartitionKey { column: "id".to_string(), opclass: None }],
    );

    assert!(catalog.create_partitioned_table(schema.clone()).is_ok());
    assert!(catalog.create_partitioned_table(schema).is_err());
}

#[test]
fn test_create_partition_nonexistent_parent() {
    let catalog = Catalog::new();
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    let partition_schema = TableSchema::as_partition(
        "orders_2024_01".to_string(),
        "nonexistent_parent".to_string(),
        PartitionBoundSpec::Default,
    );

    assert!(catalog.create_partition(partition_schema).is_err());
}
