//! DDL statement execution
//!
//! Handles CREATE and DROP statements for tables, views, indexes,
//! functions, types, triggers, and aggregates.

use crate::catalog::{Aggregate, Catalog, Function, FunctionLanguage, Parameter, Value};
use crate::parser::ast::{
    FunctionParameter as AstFunctionParameter, FunctionReturnType, FunctionVolatility,
    ParameterMode, Statement,
};
use std::sync::Arc;

pub enum DdlResult {
    CommandComplete(String),
}

pub fn execute_ddl(catalog: &Arc<Catalog>, stmt: Statement) -> Result<DdlResult, String> {
    match stmt {
        Statement::CreateTable(create) => {
            if create.is_partition {
                let bound = create
                    .partition_bound
                    .clone()
                    .unwrap_or(crate::parser::ast::PartitionBoundSpec::Default);
                let schema = crate::catalog::TableSchema::as_partition(
                    create.table.clone(),
                    create.parent_table.clone().unwrap_or_default(),
                    bound,
                );
                catalog.create_partition(schema)?;
                Ok(DdlResult::CommandComplete("CREATE TABLE PARTITION".to_string()))
            } else if let Some((method, keys)) = &create.partition_by {
                let schema = crate::catalog::TableSchema::with_partition(
                    create.table.clone(),
                    create.columns.clone(),
                    method.clone(),
                    keys.clone(),
                );
                catalog.create_partitioned_table(schema)?;
                Ok(DdlResult::CommandComplete("CREATE TABLE".to_string()))
            } else {
                catalog.create_table_with_constraints(
                    create.table.clone(),
                    create.columns,
                    create.primary_key,
                    create.foreign_keys,
                )?;
                Ok(DdlResult::CommandComplete("CREATE TABLE".to_string()))
            }
        }
        Statement::AttachPartition(attach) => {
            catalog.attach_partition(&attach)?;
            Ok(DdlResult::CommandComplete("ALTER TABLE ATTACH PARTITION".to_string()))
        }
        Statement::DetachPartition(detach) => {
            catalog.detach_partition(&detach)?;
            Ok(DdlResult::CommandComplete("ALTER TABLE DETACH PARTITION".to_string()))
        }
        Statement::DropTable(drop) => {
            catalog.drop_table(&drop.table, drop.if_exists)?;
            Ok(DdlResult::CommandComplete("DROP TABLE".to_string()))
        }
        Statement::CreateView(create) => {
            catalog.create_view(create.name.clone(), *create.query)?;
            Ok(DdlResult::CommandComplete("CREATE VIEW".to_string()))
        }
        Statement::DropView(drop) => {
            catalog.drop_view(&drop.name, drop.if_exists)?;
            Ok(DdlResult::CommandComplete("DROP VIEW".to_string()))
        }
        Statement::CreateMaterializedView(create) => {
            catalog.create_materialized_view(create.name.clone(), *create.query)?;
            Ok(DdlResult::CommandComplete("CREATE MATERIALIZED VIEW".to_string()))
        }
        Statement::RefreshMaterializedView(refresh) => {
            catalog.refresh_materialized_view(&refresh.name)?;
            Ok(DdlResult::CommandComplete("REFRESH MATERIALIZED VIEW".to_string()))
        }
        Statement::DropMaterializedView(drop) => {
            catalog.drop_materialized_view(&drop.name, drop.if_exists)?;
            Ok(DdlResult::CommandComplete("DROP MATERIALIZED VIEW".to_string()))
        }
        Statement::CreateTrigger(create) => {
            catalog.create_trigger(create)?;
            Ok(DdlResult::CommandComplete("CREATE TRIGGER".to_string()))
        }
        Statement::DropTrigger(drop) => {
            catalog.drop_trigger(&drop.name, drop.if_exists)?;
            Ok(DdlResult::CommandComplete("DROP TRIGGER".to_string()))
        }
        Statement::CreateIndex(create) => {
            catalog.create_index(create)?;
            Ok(DdlResult::CommandComplete("CREATE INDEX".to_string()))
        }
        Statement::DropIndex(drop) => {
            catalog.drop_index(&drop.name, drop.if_exists)?;
            Ok(DdlResult::CommandComplete("DROP INDEX".to_string()))
        }
        Statement::CreateFunction(create) => {
            let return_type_str = match &create.return_type {
                FunctionReturnType::Type(s) => s.clone(),
                FunctionReturnType::Setof(s) => format!("SETOF {}", s),
                FunctionReturnType::Table(cols) => {
                    let cols_str = cols
                        .iter()
                        .map(|(n, t)| format!("{} {}", n, t))
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("TABLE({})", cols_str)
                }
            };

            let is_variadic =
                create.parameters.iter().any(|p| matches!(p.mode, ParameterMode::Variadic));

            let volatility = match create.volatility {
                Some(FunctionVolatility::Immutable) => {
                    crate::catalog::FunctionVolatility::Immutable
                }
                Some(FunctionVolatility::Stable) => crate::catalog::FunctionVolatility::Stable,
                Some(FunctionVolatility::Volatile) => crate::catalog::FunctionVolatility::Volatile,
                None => crate::catalog::FunctionVolatility::Volatile,
            };

            let language = match create.language.to_uppercase().as_str() {
                "SQL" => FunctionLanguage::Sql,
                "PLPGSQL" | "PL/PGSQL" => FunctionLanguage::PlPgSql,
                _ => {
                    return Err(format!("Unsupported language: {}", create.language));
                }
            };

            let func = Function {
                name: create.name,
                parameters: create
                    .parameters
                    .into_iter()
                    .map(|p| Parameter { name: p.name, data_type: p.data_type, default: p.default })
                    .collect(),
                return_type: return_type_str,
                language,
                body: create.body,
                is_variadic,
                volatility,
                cost: create.cost.unwrap_or(100.0),
                rows: create.rows.unwrap_or(1),
            };

            catalog.create_function(func)?;
            Ok(DdlResult::CommandComplete("CREATE FUNCTION".to_string()))
        }
        Statement::DropFunction(drop) => {
            catalog.drop_function(&drop.name, drop.if_exists)?;
            Ok(DdlResult::CommandComplete("DROP FUNCTION".to_string()))
        }
        Statement::CreateAggregate(create) => {
            let volatility = match create.volatility {
                Some(FunctionVolatility::Immutable) => {
                    crate::catalog::FunctionVolatility::Immutable
                }
                Some(FunctionVolatility::Stable) => crate::catalog::FunctionVolatility::Stable,
                Some(FunctionVolatility::Volatile) => crate::catalog::FunctionVolatility::Volatile,
                None => crate::catalog::FunctionVolatility::Volatile,
            };

            let agg = Aggregate {
                name: create.name,
                input_type: create.input_type,
                sfunc: create.sfunc,
                stype: create.stype,
                finalfunc: create.finalfunc,
                initcond: create.initcond,
                volatility,
                cost: create.cost.unwrap_or(100.0),
            };

            catalog.create_aggregate(agg)?;
            Ok(DdlResult::CommandComplete("CREATE AGGREGATE".to_string()))
        }
        Statement::DropAggregate(drop) => {
            catalog.drop_aggregate(&drop.name, drop.if_exists)?;
            Ok(DdlResult::CommandComplete("DROP AGGREGATE".to_string()))
        }
        Statement::CreateType(create) => match &create.kind {
            crate::parser::ast::TypeKind::Enum(labels) => {
                catalog.create_type(create.type_name.clone(), labels.clone())?;
                Ok(DdlResult::CommandComplete("CREATE TYPE".to_string()))
            }
            crate::parser::ast::TypeKind::Composite(fields) => {
                let fields_vec: Vec<(String, crate::parser::ast::DataType)> =
                    fields.iter().map(|col| (col.name.clone(), col.data_type.clone())).collect();
                catalog.create_composite_type(create.type_name.clone(), fields_vec)?;
                Ok(DdlResult::CommandComplete("CREATE TYPE".to_string()))
            }
        },
        Statement::DropType(drop) => {
            catalog.drop_type(&drop.type_name, drop.if_exists, drop.cascade)?;
            Ok(DdlResult::CommandComplete("DROP TYPE".to_string()))
        }
        _ => Ok(DdlResult::CommandComplete("SELECT 0".to_string())),
    }
}
