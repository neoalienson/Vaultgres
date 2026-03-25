#[cfg(test)]
mod tests {
    use crate::parser::ast::*;
    use crate::parser::parse;

    #[test]
    fn test_parse_create_table_partition_by_range() {
        let sql =
            "CREATE TABLE orders (order_id INT, order_date DATE) PARTITION BY RANGE (order_date)";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.table, "orders");
                assert_eq!(create.columns.len(), 2);
                assert!(create.partition_by.is_some());
                let (method, keys) = create.partition_by.unwrap();
                assert_eq!(method, PartitionMethod::Range);
                assert_eq!(keys.len(), 1);
                assert_eq!(keys[0].column, "order_date");
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn test_parse_create_table_partition_by_list() {
        let sql = "CREATE TABLE cities (city_id INT, city_name TEXT) PARTITION BY LIST (city_name)";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.table, "cities");
                assert!(create.partition_by.is_some());
                let (method, keys) = create.partition_by.unwrap();
                assert_eq!(method, PartitionMethod::List);
                assert_eq!(keys[0].column, "city_name");
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn test_parse_create_table_partition_by_hash() {
        let sql = "CREATE TABLE customers (customer_id INT) PARTITION BY HASH (customer_id)";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.table, "customers");
                let (method, keys) = create.partition_by.unwrap();
                assert_eq!(method, PartitionMethod::Hash);
                assert_eq!(keys[0].column, "customer_id");
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn test_parse_create_table_partition_of_range() {
        let sql = "CREATE TABLE orders_2024_01 PARTITION OF orders FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.table, "orders_2024_01");
                assert!(create.is_partition);
                assert_eq!(create.parent_table, Some("orders".to_string()));
                assert!(create.partition_bound.is_some());
                if let PartitionBoundSpec::Range(bound) = create.partition_bound.unwrap() {
                    assert_eq!(bound.from_values.len(), 1);
                    assert_eq!(bound.to_values.len(), 1);
                } else {
                    panic!("Expected RANGE bound");
                }
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn test_parse_create_table_partition_of_list() {
        let sql = "CREATE TABLE cities_a_m PARTITION OF cities FOR VALUES IN ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M')";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.table, "cities_a_m");
                assert!(create.is_partition);
                assert_eq!(create.parent_table, Some("cities".to_string()));
                if let PartitionBoundSpec::List(bound) = create.partition_bound.unwrap() {
                    assert_eq!(bound.values.len(), 13);
                } else {
                    panic!("Expected LIST bound");
                }
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn test_parse_create_table_partition_of_hash() {
        let sql = "CREATE TABLE customers_0 PARTITION OF customers FOR VALUES WITH (MODULUS 4, REMAINDER 0)";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.table, "customers_0");
                assert!(create.is_partition);
                assert_eq!(create.parent_table, Some("customers".to_string()));
                if let PartitionBoundSpec::Hash(bound) = create.partition_bound.unwrap() {
                    assert_eq!(bound.modulus, 4);
                    assert_eq!(bound.remainder, 0);
                } else {
                    panic!("Expected HASH bound");
                }
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn test_parse_create_table_partition_of_default() {
        let sql = "CREATE TABLE orders_default PARTITION OF orders DEFAULT";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.table, "orders_default");
                assert!(create.is_partition);
                assert_eq!(create.parent_table, Some("orders".to_string()));
                assert_eq!(create.partition_bound.unwrap(), PartitionBoundSpec::Default);
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn test_parse_alter_table_attach_partition() {
        let sql = "ALTER TABLE orders ATTACH PARTITION orders_2024_01 FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::AttachPartition(attach) => {
                assert_eq!(attach.parent_table, "orders");
                assert_eq!(attach.partition_name, "orders_2024_01");
                if let PartitionBoundSpec::Range(bound) = attach.bound {
                    assert_eq!(bound.from_values.len(), 1);
                    assert_eq!(bound.to_values.len(), 1);
                } else {
                    panic!("Expected RANGE bound");
                }
            }
            _ => panic!("Expected ATTACH PARTITION statement"),
        }
    }

    #[test]
    fn test_parse_alter_table_detach_partition() {
        let sql = "ALTER TABLE orders DETACH PARTITION orders_2024_01";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::DetachPartition(detach) => {
                assert_eq!(detach.parent_table, "orders");
                assert_eq!(detach.partition_name, "orders_2024_01");
            }
            _ => panic!("Expected DETACH PARTITION statement"),
        }
    }

    #[test]
    fn test_parse_partition_multiple_columns() {
        let sql = "CREATE TABLE sales (product_id INT, region TEXT, sale_date DATE) PARTITION BY RANGE (product_id, region)";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                let (method, keys) = create.partition_by.unwrap();
                assert_eq!(method, PartitionMethod::Range);
                assert_eq!(keys.len(), 2);
                assert_eq!(keys[0].column, "product_id");
                assert_eq!(keys[1].column, "region");
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn test_parse_partition_by_case_insensitive() {
        let sql = "create table orders (order_id INT) partition by range (order_id)";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert!(create.partition_by.is_some());
                let (method, _) = create.partition_by.unwrap();
                assert_eq!(method, PartitionMethod::Range);
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn test_parse_partition_of_case_insensitive() {
        let sql =
            "create table orders_0 partition of orders for values with (modulus 4, remainder 0)";
        let stmt = parse(sql).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert!(create.is_partition);
                if let PartitionBoundSpec::Hash(bound) = create.partition_bound.unwrap() {
                    assert_eq!(bound.modulus, 4);
                    assert_eq!(bound.remainder, 0);
                }
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }
}
