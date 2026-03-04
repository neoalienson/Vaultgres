use crate::catalog::{Catalog, TableSchema};
use crate::executor::operators::executor::{Executor, ExecutorError};
use crate::executor::volcano::{
    DistinctExecutor, FilterExecutor, HashAggExecutor, LimitExecutor, ProjectExecutor,
    SeqScanExecutor, SortExecutor, SubqueryScanExecutor,
};
use crate::parser::ast::{AggregateFunc, ColumnDef, DataType, Expr, SelectStmt};

use std::sync::Arc;

pub struct Planner {
    catalog: Option<Arc<Catalog>>,
}

impl Planner {
    pub fn new(catalog: Option<Arc<Catalog>>) -> Self {
        Self { catalog }
    }

    pub fn new_with_catalog(catalog: Arc<Catalog>) -> Self {
        Self { catalog: Some(catalog) }
    }

    pub fn new_without_catalog() -> Self {
        Self { catalog: None }
    }

    pub fn plan(&self, stmt: &SelectStmt) -> Result<Box<dyn Executor>, ExecutorError> {
        let from_table_name = &stmt.from;

        // Track the current schema as we build the plan
        let mut current_schema: TableSchema;

        // Get catalog reference if available
        let catalog = self.catalog.as_ref();

        // 1. SeqScan or SubqueryScan (for views)
        let mut plan: Box<dyn Executor> = if let Some(cat) = catalog {
            if let Some(view_stmt) = cat.get_view(from_table_name) {
                // View expansion: recursively plan the view's query
                let sub_plan = self.plan(&view_stmt)?;
                let inner_table_name = &view_stmt.from;
                current_schema = cat
                    .get_table(inner_table_name)
                    .ok_or_else(|| {
                        ExecutorError::InternalError(format!(
                            "Table '{}' not found for view",
                            inner_table_name
                        ))
                    })?
                    .clone();
                Box::new(SubqueryScanExecutor::new(sub_plan))
            } else {
                current_schema = cat
                    .get_table(from_table_name)
                    .ok_or_else(|| {
                        ExecutorError::InternalError(format!(
                            "Table '{}' not found",
                            from_table_name
                        ))
                    })?
                    .clone();
                Box::new(SeqScanExecutor::new(
                    from_table_name.to_string(),
                    current_schema.clone(),
                    Arc::clone(&cat.data),
                    Arc::clone(&cat.txn_mgr),
                ))
            }
        } else {
            return Err(ExecutorError::InternalError("Catalog required for planning".to_string()));
        };

        // 2. Filter (WHERE clause) - with catalog for subquery support
        if let Some(predicate) = stmt.where_clause.clone() {
            plan = Box::new(
                FilterExecutor::new(plan, predicate).with_catalog(self.catalog.clone().unwrap()),
            );
        }

        // Check for aggregate functions in SELECT clause
        let mut has_aggregates = false;
        let mut agg_exprs = Vec::new();

        for col_expr in &stmt.columns {
            if Self::contains_aggregate(col_expr) {
                has_aggregates = true;
                agg_exprs.push(col_expr.clone());
            }
        }

        // 3. HashAggExecutor (GROUP BY and Aggregation)
        let has_group_by = stmt.group_by.as_ref().map_or(false, |gb| !gb.is_empty());

        if has_group_by || has_aggregates {
            // Derive the output schema for aggregation
            let group_by_exprs = stmt.group_by.as_ref().cloned().unwrap_or_default();
            let agg_output_schema =
                Self::derive_agg_output_schema(&current_schema, &group_by_exprs, &agg_exprs)?;

            plan = Box::new(HashAggExecutor::new(
                plan,
                group_by_exprs,
                agg_exprs.clone(),
                agg_output_schema.clone(),
            )?);
            current_schema = agg_output_schema;
        }

        // 4. Having (applied after aggregation) - with catalog for subquery support
        if let Some(having_clause) = stmt.having.clone() {
            plan = Box::new(
                FilterExecutor::new(plan, having_clause)
                    .with_catalog(self.catalog.clone().unwrap()),
            );
        }

        // 5. Project (SELECT list)
        // After aggregation, the projection should use the aggregate output columns
        let mut final_projection_exprs = Vec::new();
        if has_group_by || has_aggregates {
            // Use the aggregate output schema columns
            for col in &current_schema.columns {
                final_projection_exprs.push(Expr::Column(col.name.clone()));
            }
        } else {
            // Expand star in projection expressions
            for expr in &stmt.columns {
                if let Expr::Star = expr {
                    // Expand star to all columns from current schema
                    for col in &current_schema.columns {
                        final_projection_exprs.push(Expr::Column(col.name.clone()));
                    }
                } else {
                    final_projection_exprs.push(expr.clone());
                }
            }
        }

        // Update the schema to reflect the projection
        let projected_schema =
            Self::derive_projection_schema(&current_schema, &final_projection_exprs)?;
        current_schema = projected_schema;

        plan = Box::new(ProjectExecutor::new(plan, final_projection_exprs));

        // 6. Distinct
        if stmt.distinct {
            plan = Box::new(DistinctExecutor::new(plan)?);
        }

        // 7. Order By
        if let Some(order_by_exprs) = &stmt.order_by {
            if !order_by_exprs.is_empty() {
                plan = Box::new(SortExecutor::new(
                    plan,
                    order_by_exprs.clone(),
                    current_schema.clone(),
                )?);
            }
        }

        // 8. Limit and Offset
        if stmt.limit.is_some() || stmt.offset.is_some() {
            let offset = stmt.offset.unwrap_or(0);
            plan = Box::new(LimitExecutor::new(plan, stmt.limit, offset));
        }

        Ok(plan)
    }

    /// Helper to determine if an Expr is an aggregate function call
    fn contains_aggregate(expr: &Expr) -> bool {
        match expr {
            Expr::Aggregate { .. } => true,
            Expr::FunctionCall { args, .. } => args.iter().any(Self::contains_aggregate),
            Expr::Alias { expr, .. } => Self::contains_aggregate(expr),
            _ => false,
        }
    }

    /// Helper to derive the output schema after aggregation
    fn derive_agg_output_schema(
        input_schema: &TableSchema,
        group_by_exprs: &[Expr],
        agg_exprs: &[Expr],
    ) -> Result<TableSchema, ExecutorError> {
        let mut output_cols = Vec::new();

        // Add group by columns
        for group_expr in group_by_exprs {
            if let Expr::Column(col_name) = group_expr {
                if let Some(col_def) = input_schema.columns.iter().find(|c| &c.name == col_name) {
                    output_cols.push(col_def.clone());
                } else {
                    return Err(ExecutorError::ColumnNotFound(format!(
                        "GROUP BY column '{}' not found in input schema",
                        col_name
                    )));
                }
            } else {
                return Err(ExecutorError::UnsupportedExpression(format!(
                    "Unsupported GROUP BY expression: {:?}",
                    group_expr
                )));
            }
        }

        // Add aggregate columns
        for agg_expr in agg_exprs {
            if let Expr::Aggregate { func, arg } = agg_expr {
                let agg_col_name = Self::get_aggregate_name(agg_expr);

                // Determine the data type for the aggregate column
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
            } else {
                return Err(ExecutorError::InternalError(
                    "Non-aggregate expression passed as aggregate".to_string(),
                ));
            }
        }

        Ok(TableSchema::new("aggregated".to_string(), output_cols))
    }

    /// Derive schema from projection expressions
    fn derive_projection_schema(
        input_schema: &TableSchema,
        projection_exprs: &[Expr],
    ) -> Result<TableSchema, ExecutorError> {
        let mut projected_columns = Vec::new();

        for expr in projection_exprs {
            match expr {
                Expr::Column(col_name) => {
                    if let Some(col_def) = input_schema.columns.iter().find(|c| &c.name == col_name)
                    {
                        projected_columns.push(col_def.clone());
                    } else {
                        return Err(ExecutorError::ColumnNotFound(format!(
                            "Column '{}' not found in schema for projection",
                            col_name
                        )));
                    }
                }
                Expr::Star => {
                    // Expand star to all columns
                    projected_columns.extend(input_schema.columns.clone());
                }
                Expr::Aggregate { func, arg } => {
                    let agg_col_name = Self::get_aggregate_name(expr);
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
                    // For aliases, use the alias name but try to preserve the type
                    let inner_type = if let Expr::Column(col_name) = expr.as_ref() {
                        input_schema
                            .columns
                            .iter()
                            .find(|c| &c.name == col_name)
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
                    // For other expressions (functions, binary ops, etc.), use a generic type
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

    /// Get a name for an aggregate expression
    fn get_aggregate_name(expr: &Expr) -> String {
        match expr {
            Expr::Aggregate { func, arg } => {
                if let Expr::Column(col_name) = arg.as_ref() {
                    format!("{:?}({})", func, col_name).to_lowercase()
                } else {
                    format!("{:?}(expr)", func).to_lowercase()
                }
            }
            Expr::Alias { alias, .. } => alias.clone(),
            _ => format!("{:?}", expr),
        }
    }
}
