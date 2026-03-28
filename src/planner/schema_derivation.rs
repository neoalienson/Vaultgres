use crate::catalog::TableSchema;
use crate::executor::operators::executor::ExecutorError;
use crate::parser::ast::{AggregateFunc, ColumnDef, DataType, Expr};

pub fn derive_agg_output_schema(
    input_schema: &TableSchema,
    group_by_exprs: &[Expr],
    agg_exprs: &[Expr],
) -> Result<TableSchema, ExecutorError> {
    let mut output_cols = Vec::new();

    for group_expr in group_by_exprs {
        let col_name = match group_expr {
            Expr::Column(name) => name,
            Expr::QualifiedColumn { column, .. } => column,
            _ => {
                return Err(ExecutorError::UnsupportedExpression(format!(
                    "Unsupported GROUP BY expression: {:?}",
                    group_expr
                )));
            }
        };

        if let Some(col_def) = input_schema.columns.iter().find(|c| &c.name == col_name) {
            output_cols.push(col_def.clone());
        } else {
            return Err(ExecutorError::ColumnNotFound(format!(
                "GROUP BY column '{}' not found in input schema",
                col_name
            )));
        }
    }

    for agg_expr in agg_exprs {
        let (agg_col_name, agg_data_type) = match agg_expr {
            Expr::Aggregate { func, arg } => {
                let agg_col_name = get_aggregate_name(agg_expr);
                let agg_data_type = match func {
                    AggregateFunc::Count => DataType::Int,
                    AggregateFunc::Sum => DataType::Int,
                    AggregateFunc::Avg => DataType::Int,
                    AggregateFunc::Min | AggregateFunc::Max => {
                        if let Expr::Column(col_name) = arg.as_ref() {
                            if let Some(col_def) =
                                input_schema.columns.iter().find(|c| &c.name == col_name)
                            {
                                col_def.data_type.clone()
                            } else {
                                DataType::Text
                            }
                        } else if let Expr::Star = arg.as_ref() {
                            DataType::Int
                        } else {
                            DataType::Text
                        }
                    }
                };
                (agg_col_name, agg_data_type)
            }
            Expr::FunctionCall { name, args } => {
                let agg_col_name = get_aggregate_name(agg_expr);
                let agg_data_type = if !args.is_empty() {
                    if let Expr::Column(col_name) = &args[0] {
                        if let Some(col_def) =
                            input_schema.columns.iter().find(|c| &c.name == col_name)
                        {
                            col_def.data_type.clone()
                        } else {
                            DataType::Text
                        }
                    } else {
                        DataType::Text
                    }
                } else {
                    DataType::Text
                };
                (agg_col_name, agg_data_type)
            }
            Expr::Alias { alias, expr } => {
                if let Expr::Aggregate { func, arg } = expr.as_ref() {
                    let agg_data_type = match func {
                        AggregateFunc::Count => DataType::Int,
                        AggregateFunc::Sum => DataType::Int,
                        AggregateFunc::Avg => DataType::Int,
                        AggregateFunc::Min | AggregateFunc::Max => {
                            if let Expr::Column(col_name) = arg.as_ref() {
                                if let Some(col_def) =
                                    input_schema.columns.iter().find(|c| &c.name == col_name)
                                {
                                    col_def.data_type.clone()
                                } else {
                                    DataType::Text
                                }
                            } else if let Expr::Star = arg.as_ref() {
                                DataType::Int
                            } else {
                                DataType::Text
                            }
                        }
                    };
                    (alias.clone(), agg_data_type)
                } else if let Expr::FunctionCall { name, args } = expr.as_ref() {
                    let agg_col_name = alias.clone();
                    let agg_data_type = if !args.is_empty() {
                        if let Expr::Column(col_name) = &args[0] {
                            if let Some(col_def) =
                                input_schema.columns.iter().find(|c| &c.name == col_name)
                            {
                                col_def.data_type.clone()
                            } else {
                                DataType::Text
                            }
                        } else {
                            DataType::Text
                        }
                    } else {
                        DataType::Text
                    };
                    (agg_col_name, agg_data_type)
                } else {
                    return Err(ExecutorError::InternalError(
                        "Non-aggregate expression passed as aggregate".to_string(),
                    ));
                }
            }
            _ => {
                return Err(ExecutorError::InternalError(
                    "Non-aggregate expression passed as aggregate".to_string(),
                ));
            }
        };

        output_cols.push(ColumnDef {
            name: agg_col_name,
            data_type: agg_data_type,
            is_primary_key: false,
            is_unique: false,
            is_auto_increment: false,
            is_not_null: false,
            default_value: None,
            foreign_key: None,
        });
    }

    Ok(TableSchema::new("aggregated".to_string(), output_cols))
}

pub fn derive_projection_schema(
    input_schema: &TableSchema,
    projection_exprs: &[Expr],
) -> Result<TableSchema, ExecutorError> {
    let mut projected_columns = Vec::new();

    for expr in projection_exprs {
        match expr {
            Expr::Column(col_name) => {
                let lookup_name = if let Some(dot_pos) = col_name.find('.') {
                    &col_name[dot_pos + 1..]
                } else {
                    col_name.as_str()
                };

                if let Some(col_def) = input_schema.columns.iter().find(|c| c.name == lookup_name) {
                    projected_columns.push(col_def.clone());
                } else {
                    return Err(ExecutorError::ColumnNotFound(format!(
                        "Column '{}' not found in schema for projection",
                        col_name
                    )));
                }
            }
            Expr::QualifiedColumn { column, .. } => {
                if let Some(col_def) = input_schema.columns.iter().find(|c| &c.name == column) {
                    projected_columns.push(col_def.clone());
                } else {
                    return Err(ExecutorError::ColumnNotFound(format!(
                        "Column '{}' not found in schema for projection",
                        column
                    )));
                }
            }
            Expr::Star => {
                projected_columns.extend(input_schema.columns.clone());
            }
            Expr::Aggregate { func, arg } => {
                let agg_col_name = get_aggregate_name(expr);
                let agg_data_type = match func {
                    AggregateFunc::Count => DataType::Int,
                    AggregateFunc::Sum => DataType::Int,
                    AggregateFunc::Avg => DataType::Int,
                    AggregateFunc::Min | AggregateFunc::Max => {
                        if let Expr::Column(col_name) = arg.as_ref() {
                            if let Some(col_def) =
                                input_schema.columns.iter().find(|c| &c.name == col_name)
                            {
                                col_def.data_type.clone()
                            } else {
                                DataType::Text
                            }
                        } else {
                            DataType::Text
                        }
                    }
                };
                projected_columns.push(ColumnDef {
                    name: agg_col_name,
                    data_type: agg_data_type,
                    is_primary_key: false,
                    is_unique: false,
                    is_auto_increment: false,
                    is_not_null: false,
                    default_value: None,
                    foreign_key: None,
                });
            }
            Expr::Alias { alias, expr } => {
                let inner_type = if let Expr::Column(col_name) = expr.as_ref() {
                    let lookup_name = if let Some(dot_pos) = col_name.find('.') {
                        &col_name[dot_pos + 1..]
                    } else {
                        col_name.as_str()
                    };
                    input_schema
                        .columns
                        .iter()
                        .find(|c| c.name == lookup_name)
                        .map(|c| c.data_type.clone())
                        .unwrap_or(DataType::Text)
                } else {
                    DataType::Text
                };
                projected_columns.push(ColumnDef {
                    name: alias.clone(),
                    data_type: inner_type,
                    is_primary_key: false,
                    is_unique: false,
                    is_auto_increment: false,
                    is_not_null: false,
                    default_value: None,
                    foreign_key: None,
                });
            }
            _ => {
                projected_columns.push(ColumnDef {
                    name: format!("{:?}", expr),
                    data_type: DataType::Text,
                    is_primary_key: false,
                    is_unique: false,
                    is_auto_increment: false,
                    is_not_null: false,
                    default_value: None,
                    foreign_key: None,
                });
            }
        }
    }

    Ok(TableSchema::new("projected".to_string(), projected_columns))
}

pub fn validate_projection_schema(
    projection_exprs: &[Expr],
    input_schema: &TableSchema,
) -> Result<(), ExecutorError> {
    let available_cols: Vec<&String> = input_schema.columns.iter().map(|c| &c.name).collect();

    for expr in projection_exprs {
        match expr {
            Expr::Column(col_name) => {
                if !input_schema.columns.iter().any(|c| &c.name == col_name) {
                    return Err(ExecutorError::InternalError(format!(
                        "Projection column '{}' not found in schema. Available columns: {:?}",
                        col_name, available_cols
                    )));
                }
            }
            Expr::QualifiedColumn { column, .. } => {
                if !input_schema.columns.iter().any(|c| &c.name == column) {
                    return Err(ExecutorError::InternalError(format!(
                        "Projection qualified column '{}' not found in schema. Available columns: {:?}",
                        column, available_cols
                    )));
                }
            }
            Expr::Alias { alias, .. } => {
                if alias.contains('{') || alias.contains("QualifiedColumn") {
                    return Err(ExecutorError::InternalError(format!(
                        "Malformed alias detected: '{}'. This may indicate a bug in expression handling.",
                        alias
                    )));
                }
            }
            Expr::FunctionCall { name, .. } => {
                if name.contains('{') || name.contains("Expr::") {
                    return Err(ExecutorError::InternalError(format!(
                        "Malformed function column name detected: '{}'. This may indicate a bug.",
                        name
                    )));
                }
            }
            Expr::Star => {}
            _ => {}
        }
    }
    Ok(())
}

fn get_aggregate_name(expr: &Expr) -> String {
    match expr {
        Expr::Aggregate { func, arg } => {
            let col_name = match arg.as_ref() {
                Expr::Column(name) => name.clone(),
                Expr::QualifiedColumn { column, .. } => column.clone(),
                Expr::Star => "*".to_string(),
                _ => "expr".to_string(),
            };
            format!("{:?}({})", func, col_name).to_lowercase()
        }
        Expr::FunctionCall { name, args } => {
            let arg_name = if !args.is_empty() {
                match &args[0] {
                    Expr::Column(col_name) => col_name.clone(),
                    Expr::QualifiedColumn { column, .. } => column.clone(),
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
