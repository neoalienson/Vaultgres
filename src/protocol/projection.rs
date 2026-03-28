//! Result set building and column projection
//!
//! Handles building result sets from query results and projecting
//! columns based on SELECT expressions.

use crate::catalog::{Catalog, TableSchema, Value};
use crate::executor::Eval;
use crate::parser::ast::Expr;
use std::collections::HashMap;

use super::result_set::{ColumnMetadata, ResultSet, Row};
use super::type_mapping::{serialize_value_with_enum_types, value_to_pg_type};

pub fn build_result_set(
    catalog: &Catalog,
    column_names: &[String],
    rows: Vec<Vec<Value>>,
) -> Result<ResultSet, String> {
    let actual_column_count = if !rows.is_empty() { rows[0].len() } else { column_names.len() };

    let columns: Vec<ColumnMetadata> = (0..actual_column_count)
        .map(|i| {
            let name = if column_names.len() == 1 && column_names[0] == "*" {
                format!("column{}", i + 1)
            } else if i < column_names.len() {
                column_names[i].clone()
            } else {
                format!("column{}", i + 1)
            };

            let (type_oid, type_size) = if !rows.is_empty() && i < rows[0].len() {
                value_to_pg_type(&rows[0][i])
            } else {
                (25, -1)
            };

            ColumnMetadata {
                name,
                table_oid: 0,
                column_attr_number: 0,
                type_oid,
                type_size,
                type_modifier: -1,
                format_code: 0,
            }
        })
        .collect();

    let mut result_set = ResultSet::new(columns);

    for row in rows {
        let fields: Vec<Option<Vec<u8>>> =
            row.iter().map(|v| serialize_value_with_enum_types(v, &catalog.enum_types)).collect();
        result_set.add_row(Row::new(fields));
    }

    Ok(result_set)
}

pub fn build_tuple(row: &[Value], schemas: &[(String, TableSchema)]) -> HashMap<String, Value> {
    let mut tuple = HashMap::new();
    let mut offset = 0;
    for (tbl_alias, schema) in schemas {
        for col in &schema.columns {
            let qualified_name = format!("{}.{}", tbl_alias, col.name);
            let simple_name = &col.name;
            if row.len() > offset {
                tuple.insert(qualified_name.clone(), row[offset].clone());
                if qualified_name != *simple_name {
                    tuple.insert(simple_name.clone(), row[offset].clone());
                }
            }
            offset += 1;
        }
    }
    tuple
}

pub fn extract_column_names(
    exprs: &[Expr],
    _schemas: &[(String, TableSchema)],
) -> Result<Vec<String>, String> {
    exprs
        .iter()
        .map(|expr| match expr {
            Expr::Star => Ok("*".to_string()),
            Expr::Column(name) => Ok(name.clone()),
            Expr::QualifiedColumn { table: _, column } => Ok(column.clone()),
            _ => Ok("?".to_string()),
        })
        .collect()
}

pub fn build_combined_schema(schemas: &[(String, TableSchema)]) -> TableSchema {
    let mut combined_cols = Vec::new();
    for (_, schema) in schemas {
        combined_cols.extend(schema.columns.clone());
    }
    TableSchema::new("combined".to_string(), combined_cols)
}

pub fn project_columns(
    catalog: &Catalog,
    rows: &[Vec<Value>],
    exprs: &[Expr],
    schemas: &[(String, TableSchema)],
) -> Result<Vec<Vec<Value>>, String> {
    log::debug!("[PROJ] Projecting {} expressions, {} rows", exprs.len(), rows.len());
    if exprs.is_empty() || (exprs.len() == 1 && matches!(exprs[0], Expr::Star)) {
        return Ok(rows.to_vec());
    }

    let mut result = Vec::new();
    for row in rows {
        let mut projected = Vec::new();
        let tuple = build_tuple(row, schemas);
        for expr in exprs {
            log::debug!("[PROJ] Processing expression: {:?}", expr);
            match expr {
                Expr::Star => {
                    projected.extend(row.iter().cloned());
                }
                _ => {
                    let value =
                        Eval::eval_expr_with_catalog(expr, &tuple, Some(catalog), None, None)
                            .map_err(|e| format!("Evaluation error: {}", e))?;
                    projected.push(value);
                }
            }
        }
        result.push(projected);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Column, DataType, TableSchema};

    fn create_test_schema() -> (String, TableSchema) {
        let columns = vec![
            Column::new("id".to_string(), DataType::Int),
            Column::new("name".to_string(), DataType::Text),
            Column::new("price".to_string(), DataType::Int),
            Column::new("quantity".to_string(), DataType::Int),
        ];
        let schema = TableSchema::new("test".to_string(), columns);
        ("test".to_string(), schema)
    }

    fn create_test_row() -> Vec<Value> {
        vec![Value::Int(1), Value::Text("Laptop".to_string()), Value::Int(1000), Value::Int(5)]
    }

    #[test]
    fn test_project_columns_select_star() {
        let catalog = Catalog::new();
        let schema = create_test_schema();
        let rows = vec![
            vec![Value::Int(1), Value::Text("Alice".to_string())],
            vec![Value::Int(2), Value::Text("Bob".to_string())],
        ];
        let exprs = vec![Expr::Star];

        let projected_rows = project_columns(&catalog, &rows, &exprs, &[schema.clone()]).unwrap();
        assert_eq!(projected_rows.len(), 2);
        assert_eq!(projected_rows[0].len(), 2);
        assert_eq!(projected_rows[0][0], Value::Int(1));
    }

    #[test]
    fn test_project_columns_select_specific() {
        let catalog = Catalog::new();
        let schema = create_test_schema();
        let rows = vec![
            vec![Value::Int(1), Value::Text("Alice".to_string())],
            vec![Value::Int(2), Value::Text("Bob".to_string())],
        ];
        let exprs = vec![Expr::Column("name".to_string())];

        let projected_rows = project_columns(&catalog, &rows, &exprs, &[schema.clone()]).unwrap();
        assert_eq!(projected_rows.len(), 2);
        assert_eq!(projected_rows[0].len(), 1);
        assert_eq!(projected_rows[0][0], Value::Text("Alice".to_string()));
    }

    #[test]
    fn test_project_columns_qualified() {
        let catalog = Catalog::new();
        let schema = create_test_schema();
        let rows = vec![
            vec![Value::Int(1), Value::Text("Alice".to_string())],
            vec![Value::Int(2), Value::Text("Bob".to_string())],
        ];
        let exprs =
            vec![Expr::QualifiedColumn { table: "u".to_string(), column: "name".to_string() }];

        let projected_rows =
            project_columns(&catalog, &rows, &exprs, &[("u".to_string(), schema.1.clone())])
                .unwrap();
        assert_eq!(projected_rows.len(), 2);
        assert_eq!(projected_rows[0].len(), 1);
        assert_eq!(projected_rows[0][0], Value::Text("Alice".to_string()));
    }

    #[test]
    fn test_project_columns_not_found() {
        let catalog = Catalog::new();
        let schema = create_test_schema();
        let rows = vec![vec![Value::Int(1), Value::Text("Alice".to_string())]];
        let exprs = vec![Expr::Column("nonexistent".to_string())];

        let result = project_columns(&catalog, &rows, &exprs, &[schema.clone()]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Column not found"));
    }

    #[test]
    fn test_project_columns_with_alias() {
        let catalog = Catalog::new();
        let schema = create_test_schema();
        let row = create_test_row();
        let rows = vec![row.clone()];

        let exprs = vec![
            Expr::Alias {
                expr: Box::new(Expr::Column("price".to_string())),
                alias: "item_price".to_string(),
            },
            Expr::Alias {
                expr: Box::new(Expr::Column("name".to_string())),
                alias: "item_name".to_string(),
            },
        ];

        let result = project_columns(&catalog, &rows, &exprs, &[schema.clone()]);
        assert!(result.is_ok());
        let projected = result.unwrap();
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].len(), 2);
        assert_eq!(projected[0][0], Value::Int(1000));
        assert_eq!(projected[0][1], Value::Text("Laptop".to_string()));
    }

    #[test]
    fn test_project_columns_with_arithmetic() {
        let catalog = Catalog::new();
        let schema = create_test_schema();
        let row = create_test_row();
        let rows = vec![row.clone()];

        let exprs = vec![
            Expr::Column("name".to_string()),
            Expr::BinaryOp {
                left: Box::new(Expr::Column("price".to_string())),
                op: crate::parser::ast::BinaryOperator::Multiply,
                right: Box::new(Expr::Number(2)),
            },
        ];

        let result = project_columns(&catalog, &rows, &exprs, &[schema.clone()]);
        assert!(result.is_ok());
        let projected = result.unwrap();
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].len(), 2);
        assert_eq!(projected[0][0], Value::Text("Laptop".to_string()));
        assert_eq!(projected[0][1], Value::Int(2000));
    }

    #[test]
    fn test_project_columns_with_mixed_expressions() {
        let catalog = Catalog::new();
        let schema = create_test_schema();
        let row = create_test_row();
        let rows = vec![row.clone()];

        let exprs = vec![
            Expr::Column("id".to_string()),
            Expr::Alias {
                expr: Box::new(Expr::Column("name".to_string())),
                alias: "product_name".to_string(),
            },
            Expr::BinaryOp {
                left: Box::new(Expr::Column("price".to_string())),
                op: crate::parser::ast::BinaryOperator::Add,
                right: Box::new(Expr::Number(100)),
            },
            Expr::Number(42),
        ];

        let result = project_columns(&catalog, &rows, &exprs, &[schema.clone()]);
        assert!(result.is_ok());
        let projected = result.unwrap();
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].len(), 4);
        assert_eq!(projected[0][0], Value::Int(1));
        assert_eq!(projected[0][1], Value::Text("Laptop".to_string()));
        assert_eq!(projected[0][2], Value::Int(1100));
        assert_eq!(projected[0][3], Value::Int(42));
    }

    #[test]
    fn test_build_result_set_simple() {
        let catalog = Catalog::new();
        let column_names = vec!["id".to_string(), "name".to_string()];
        let rows = vec![
            vec![Value::Int(1), Value::Text("Alice".to_string())],
            vec![Value::Int(2), Value::Text("Bob".to_string())],
        ];

        let result_set = build_result_set(&catalog, &column_names, rows).unwrap();
        assert_eq!(result_set.row_count(), 2);
        assert_eq!(result_set.columns.len(), 2);
        assert_eq!(result_set.columns[0].name, "id");
        assert_eq!(result_set.columns[1].name, "name");
    }

    #[test]
    fn test_build_result_set_star() {
        let catalog = Catalog::new();
        let column_names = vec!["*".to_string()];
        let rows = vec![
            vec![Value::Int(1), Value::Text("Alice".to_string())],
            vec![Value::Int(2), Value::Text("Bob".to_string())],
        ];

        let result_set = build_result_set(&catalog, &column_names, rows).unwrap();
        assert_eq!(result_set.row_count(), 2);
        assert_eq!(result_set.columns.len(), 2);
        assert_eq!(result_set.columns[0].name, "column1");
        assert_eq!(result_set.columns[1].name, "column2");
    }

    #[test]
    fn test_build_result_set_empty() {
        let catalog = Catalog::new();
        let column_names = vec!["id".to_string(), "name".to_string()];
        let rows: Vec<Vec<Value>> = Vec::new();

        let result_set = build_result_set(&catalog, &column_names, rows).unwrap();
        assert_eq!(result_set.row_count(), 0);
        assert_eq!(result_set.columns.len(), 2);
        assert_eq!(result_set.columns[0].name, "id");
        assert_eq!(result_set.columns[1].name, "name");
    }
}
