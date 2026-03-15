use crate::catalog::Value;
use crate::catalog::{Catalog, TableSchema};
use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};
use crate::executor::volcano::{
    DistinctExecutor, FilterExecutor, HashAggExecutor, JoinExecutor, JoinType, LimitExecutor,
    ProjectExecutor, SeqScanExecutor, SortExecutor, SubqueryScanExecutor,
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
                // Build combined schema from all tables in the view (base + joins)
                let mut combined_schema = cat
                    .get_table(&view_stmt.from)
                    .ok_or_else(|| {
                        ExecutorError::InternalError(format!(
                            "Table '{}' not found for view",
                            view_stmt.from
                        ))
                    })?
                    .clone();
                // Add columns from joined tables
                for join in &view_stmt.joins {
                    if let Some(join_table) = cat.get_table(&join.table) {
                        combined_schema.columns.extend(join_table.columns.clone());
                    }
                }
                // Project the schema based on view's SELECT columns
                current_schema =
                    Self::derive_projection_schema(&combined_schema, &view_stmt.columns)?;
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

        // Handle JOINs if present
        if !stmt.joins.is_empty() {
            if let Some(cat) = catalog {
                // Build plans for each joined table and chain the joins
                for join in &stmt.joins {
                    let right_schema = cat
                        .get_table(&join.table)
                        .ok_or_else(|| {
                            ExecutorError::InternalError(format!(
                                "Table '{}' not found for join",
                                join.table
                            ))
                        })?
                        .clone();

                    let right_plan: Box<dyn Executor> = Box::new(SeqScanExecutor::new(
                        join.table.clone(),
                        right_schema.clone(),
                        Arc::clone(&cat.data),
                        Arc::clone(&cat.txn_mgr),
                    ));

                    // Create join condition - evaluate against combined tuple
                    let join_condition = join.on.clone();

                    // Build combined schema for this join step
                    let mut combined_schema_for_join = current_schema.clone();
                    combined_schema_for_join.columns.extend(right_schema.columns.clone());

                    let condition = move |left: &Tuple, right: &Tuple| -> bool {
                        // Combine left and right tuples for predicate evaluation
                        let mut combined = left.clone();
                        for (k, v) in right.iter() {
                            combined.insert(k.clone(), v.clone());
                        }

                        match crate::executor::Eval::eval_expr(&join_condition, &combined) {
                            Ok(Value::Bool(b)) => b,
                            _ => false,
                        }
                    };

                    let join_executor = JoinExecutor::new(
                        plan,
                        right_plan,
                        match join.join_type {
                            crate::parser::ast::JoinType::Inner => JoinType::Inner,
                            crate::parser::ast::JoinType::Left => JoinType::Left,
                            crate::parser::ast::JoinType::Right => JoinType::Right,
                            crate::parser::ast::JoinType::Full => JoinType::Full,
                        },
                        Box::new(condition),
                    );
                    plan = Box::new(join_executor);

                    // Update current schema to include right table columns
                    current_schema.columns.extend(right_schema.columns.clone());
                }
            }
        }

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
        log::debug!("Planner: Processing {} SELECT columns", stmt.columns.len());

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
                log::debug!("Planner: Processing column expression: {:?}", expr);
                if let Expr::Star = expr {
                    // Expand star to all columns from current schema
                    for col in &current_schema.columns {
                        final_projection_exprs.push(Expr::Column(col.name.clone()));
                    }
                } else {
                    // Convert QualifiedColumn to simple Column since all columns are merged
                    match expr {
                        Expr::QualifiedColumn { column, .. } => {
                            log::debug!(
                                "Planner: Converting QualifiedColumn to Column '{}'",
                                column
                            );
                            final_projection_exprs.push(Expr::Column(column.clone()));
                        }
                        _ => {
                            final_projection_exprs.push(expr.clone());
                        }
                    }
                }
            }
        }

        // Update the schema to reflect the projection
        let projected_schema =
            Self::derive_projection_schema(&current_schema, &final_projection_exprs)?;
        current_schema = projected_schema;

        // Validate projection before creating executor (catches bugs early)
        Self::validate_projection_schema(&final_projection_exprs, &current_schema)?;

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
    pub fn derive_projection_schema(
        input_schema: &TableSchema,
        projection_exprs: &[Expr],
    ) -> Result<TableSchema, ExecutorError> {
        let mut projected_columns = Vec::new();

        for expr in projection_exprs {
            match expr {
                Expr::Column(col_name) => {
                    // Handle table-prefixed column names (e.g., "o.total" -> "total")
                    let lookup_name = if let Some(dot_pos) = col_name.find('.') {
                        &col_name[dot_pos + 1..]
                    } else {
                        col_name.as_str()
                    };

                    if let Some(col_def) =
                        input_schema.columns.iter().find(|c| c.name == lookup_name)
                    {
                        projected_columns.push(col_def.clone());
                    } else {
                        return Err(ExecutorError::ColumnNotFound(format!(
                            "Column '{}' not found in schema for projection",
                            col_name
                        )));
                    }
                }
                Expr::QualifiedColumn { column, .. } => {
                    // For qualified columns, just use the column name
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
                        // Handle table-prefixed column names (e.g., "o.id" -> "id")
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

    /// Validate that all projected columns exist in the input schema
    /// This catches bugs early (e.g., malformed column names from Debug formatting)
    fn validate_projection_schema(
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
                    // Aliases create new column names, so they're always valid
                    // But check for obviously malformed names
                    if alias.contains('{') || alias.contains("QualifiedColumn") {
                        return Err(ExecutorError::InternalError(format!(
                            "Malformed alias detected: '{}'. This may indicate a bug in expression handling.",
                            alias
                        )));
                    }
                }
                Expr::FunctionCall { name, .. } => {
                    // Function result columns are named after the function
                    if name.contains('{') || name.contains("Expr::") {
                        return Err(ExecutorError::InternalError(format!(
                            "Malformed function column name detected: '{}'. This may indicate a bug.",
                            name
                        )));
                    }
                }
                Expr::Aggregate { .. } => {
                    // Aggregate columns are handled separately
                }
                // Other expression types (BinaryOp, etc.) use generated names
                // which should be validated by the caller
                _ => {}
            }
        }

        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Catalog, TableSchema};
    use crate::parser::ast::{
        BinaryOperator, ColumnDef, DataType, Expr, JoinClause, JoinType, SelectStmt,
    };
    use std::sync::Arc;

    fn create_test_catalog() -> Arc<Catalog> {
        let catalog = Catalog::new();

        // Create customers table
        let customers_columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int,
                is_primary_key: true,
                is_unique: true,
                is_auto_increment: false,
                is_not_null: false,
                default_value: None,
                foreign_key: None,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
                is_primary_key: false,
                is_unique: false,
                is_auto_increment: false,
                is_not_null: false,
                default_value: None,
                foreign_key: None,
            },
            ColumnDef {
                name: "email".to_string(),
                data_type: DataType::Text,
                is_primary_key: false,
                is_unique: false,
                is_auto_increment: false,
                is_not_null: false,
                default_value: None,
                foreign_key: None,
            },
        ];
        catalog.create_table("customers".to_string(), customers_columns).unwrap();

        // Create orders table
        let orders_columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int,
                is_primary_key: true,
                is_unique: true,
                is_auto_increment: false,
                is_not_null: false,
                default_value: None,
                foreign_key: None,
            },
            ColumnDef {
                name: "customer_id".to_string(),
                data_type: DataType::Int,
                is_primary_key: false,
                is_unique: false,
                is_auto_increment: false,
                is_not_null: false,
                default_value: None,
                foreign_key: None,
            },
            ColumnDef {
                name: "total".to_string(),
                data_type: DataType::Int,
                is_primary_key: false,
                is_unique: false,
                is_auto_increment: false,
                is_not_null: false,
                default_value: None,
                foreign_key: None,
            },
        ];
        catalog.create_table("orders".to_string(), orders_columns).unwrap();

        // Create items table
        let items_columns = vec![
            ColumnDef {
                name: "item_id".to_string(),
                data_type: DataType::Int,
                is_primary_key: true,
                is_unique: true,
                is_auto_increment: false,
                is_not_null: false,
                default_value: None,
                foreign_key: None,
            },
            ColumnDef {
                name: "sku".to_string(),
                data_type: DataType::Text,
                is_primary_key: false,
                is_unique: false,
                is_auto_increment: false,
                is_not_null: false,
                default_value: None,
                foreign_key: None,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
                is_primary_key: false,
                is_unique: false,
                is_auto_increment: false,
                is_not_null: false,
                default_value: None,
                foreign_key: None,
            },
            ColumnDef {
                name: "price".to_string(),
                data_type: DataType::Int,
                is_primary_key: false,
                is_unique: false,
                is_auto_increment: false,
                is_not_null: false,
                default_value: None,
                foreign_key: None,
            },
        ];
        catalog.create_table("items".to_string(), items_columns).unwrap();

        Arc::new(catalog)
    }

    #[test]
    fn test_derive_projection_schema_simple_columns() {
        let schema = TableSchema::new(
            "test".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                    is_primary_key: true,
                    is_unique: true,
                    is_auto_increment: false,
                    is_not_null: false,
                    default_value: None,
                    foreign_key: None,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    is_primary_key: false,
                    is_unique: false,
                    is_auto_increment: false,
                    is_not_null: false,
                    default_value: None,
                    foreign_key: None,
                },
            ],
        );

        let projection = vec![Expr::Column("id".to_string()), Expr::Column("name".to_string())];

        let result = Planner::derive_projection_schema(&schema, &projection).unwrap();
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[0].data_type, DataType::Int);
        assert_eq!(result.columns[1].name, "name");
        assert_eq!(result.columns[1].data_type, DataType::Text);
    }

    #[test]
    fn test_derive_projection_schema_with_table_prefix() {
        let schema = TableSchema::new(
            "test".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                    is_primary_key: true,
                    is_unique: true,
                    is_auto_increment: false,
                    is_not_null: false,
                    default_value: None,
                    foreign_key: None,
                },
                ColumnDef {
                    name: "total".to_string(),
                    data_type: DataType::Int,
                    is_primary_key: false,
                    is_unique: false,
                    is_auto_increment: false,
                    is_not_null: false,
                    default_value: None,
                    foreign_key: None,
                },
            ],
        );

        // Test with table-prefixed column names (e.g., "o.total")
        let projection =
            vec![Expr::Column("o.id".to_string()), Expr::Column("o.total".to_string())];

        let result = Planner::derive_projection_schema(&schema, &projection).unwrap();
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[0].data_type, DataType::Int);
        assert_eq!(result.columns[1].name, "total");
        assert_eq!(result.columns[1].data_type, DataType::Int);
    }

    #[test]
    fn test_derive_projection_schema_with_alias() {
        let schema = TableSchema::new(
            "test".to_string(),
            vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int,
                is_primary_key: true,
                is_unique: true,
                is_auto_increment: false,
                is_not_null: false,
                default_value: None,
                foreign_key: None,
            }],
        );

        let projection = vec![Expr::Alias {
            alias: "order_id".to_string(),
            expr: Box::new(Expr::Column("o.id".to_string())),
        }];

        let result = Planner::derive_projection_schema(&schema, &projection).unwrap();
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "order_id");
        assert_eq!(result.columns[0].data_type, DataType::Int);
    }

    #[test]
    fn test_derive_projection_schema_star() {
        let schema = TableSchema::new(
            "test".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                    is_primary_key: true,
                    is_unique: true,
                    is_auto_increment: false,
                    is_not_null: false,
                    default_value: None,
                    foreign_key: None,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    is_primary_key: false,
                    is_unique: false,
                    is_auto_increment: false,
                    is_not_null: false,
                    default_value: None,
                    foreign_key: None,
                },
            ],
        );

        let projection = vec![Expr::Star];

        let result = Planner::derive_projection_schema(&schema, &projection).unwrap();
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[1].name, "name");
    }

    #[test]
    fn test_view_schema_derivation_simple() {
        let catalog = create_test_catalog();
        let planner = Planner::new(Some(catalog.clone()));

        // Create a simple view: SELECT id, name FROM customers
        let view_stmt = SelectStmt {
            distinct: false,
            columns: vec![Expr::Column("id".to_string()), Expr::Column("name".to_string())],
            from: "customers".to_string(),
            table_alias: None,
            joins: vec![],
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        let result = planner.plan(&view_stmt);
        assert!(result.is_ok());
    }

    #[test]
    fn test_view_schema_derivation_with_join() {
        // Note: This test verifies that the view schema derivation handles prefixed columns
        // The actual JOIN execution is handled separately
        let catalog = create_test_catalog();

        // Test that derive_projection_schema handles prefixed columns correctly
        let orders_schema = catalog.get_table("orders").unwrap();
        let customers_schema = catalog.get_table("customers").unwrap();

        // Create a combined schema (as would be done for a JOIN view)
        let mut combined_schema = customers_schema.clone();
        combined_schema.columns.extend(orders_schema.columns.clone());

        let projection = vec![
            Expr::Column("c.name".to_string()),
            Expr::Alias {
                alias: "order_id".to_string(),
                expr: Box::new(Expr::Column("o.id".to_string())),
            },
            Expr::Column("o.total".to_string()),
        ];

        let result = Planner::derive_projection_schema(&combined_schema, &projection);
        assert!(
            result.is_ok(),
            "Schema derivation with prefixed columns should succeed: {:?}",
            result.err()
        );

        let schema = result.unwrap();
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns[0].name, "name");
        assert_eq!(schema.columns[1].name, "order_id");
        assert_eq!(schema.columns[2].name, "total");
    }

    #[test]
    fn test_view_schema_derivation_with_prefixed_columns() {
        let catalog = create_test_catalog();

        // Test that derive_projection_schema handles prefixed columns correctly
        let orders_schema = catalog.get_table("orders").unwrap();
        let customers_schema = catalog.get_table("customers").unwrap();

        // Create a combined schema (as would be done for a JOIN view)
        let mut combined_schema = customers_schema.clone();
        combined_schema.columns.extend(orders_schema.columns.clone());

        let projection =
            vec![Expr::Column("c.name".to_string()), Expr::Column("o.total".to_string())];

        let result = Planner::derive_projection_schema(&combined_schema, &projection);
        assert!(
            result.is_ok(),
            "Schema derivation with prefixed columns should succeed: {:?}",
            result.err()
        );

        let schema = result.unwrap();
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].name, "name");
        assert_eq!(schema.columns[1].name, "total");
    }
}
