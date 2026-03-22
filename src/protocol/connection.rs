use super::message::{Message, ProtocolError, Response};
use super::result_set::{ColumnMetadata, ResultSet, Row};
use super::type_mapping::{serialize_value, value_to_pg_type};
use crate::catalog::{Catalog, Value};
use crate::parser::Expr;
use crate::parser::{Parser, Statement};
use crate::planner::planner::Planner;
use std::io::{Read, Write};
use std::sync::Arc;

pub enum ExecutionResult {
    CommandComplete(String),
    ResultSet(ResultSet),
}

pub struct Connection<S: Read + Write> {
    stream: S,
    authenticated: bool,
    catalog: Arc<Catalog>,
}

impl<S: Read + Write> Connection<S> {
    pub fn new(stream: S, catalog: Arc<Catalog>) -> Self {
        Self { stream, authenticated: false, catalog }
    }

    pub fn handle_startup(&mut self) -> Result<(), ProtocolError> {
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf)?;
        let len = i32::from_be_bytes(len_buf) as usize;

        let mut data = vec![0u8; len - 4];
        self.stream.read_exact(&mut data)?;

        let msg = Message::parse(0, &data)?;
        log::debug!("Startup message: {:?}", msg);
        self.authenticated = true;

        Response::AuthenticationOk.write(&mut self.stream)?;
        Response::ReadyForQuery.write(&mut self.stream)?;
        self.stream.flush()?;
        Ok(())
    }

    pub fn handle_query(&mut self, sql: &str) -> Result<(), ProtocolError> {
        log::info!("Query: {}", sql);
        match Parser::new(sql) {
            Ok(mut parser) => match parser.parse() {
                Ok(stmt) => {
                    log::debug!("Parsed statement: {:?}", stmt);
                    match self.execute_statement(stmt) {
                        Ok(ExecutionResult::CommandComplete(tag)) => {
                            Response::CommandComplete { tag }.write(&mut self.stream)?;
                            Response::ReadyForQuery.write(&mut self.stream)?;
                        }
                        Ok(ExecutionResult::ResultSet(result_set)) => {
                            // Send RowDescription
                            Response::RowDescriptionDetailed {
                                columns: result_set.columns.clone(),
                            }
                            .write(&mut self.stream)?;

                            // Send DataRow for each row
                            for row in &result_set.rows {
                                Response::DataRowDetailed { fields: row.fields.clone() }
                                    .write(&mut self.stream)?;
                            }

                            // Send CommandComplete
                            Response::CommandComplete {
                                tag: format!("SELECT {}", result_set.row_count()),
                            }
                            .write(&mut self.stream)?;
                            Response::ReadyForQuery.write(&mut self.stream)?;
                        }
                        Err(e) => {
                            log::warn!("Execution error: {}", e);
                            Response::ErrorResponse { message: format!("Execution error: {}", e) }
                                .write(&mut self.stream)?;
                            Response::ReadyForQuery.write(&mut self.stream)?;
                        }
                    }
                }
                Err(e) => {
                    log::warn!("Parse error: {}", e);
                    Response::ErrorResponse { message: format!("Parse error: {}", e) }
                        .write(&mut self.stream)?;
                    Response::ReadyForQuery.write(&mut self.stream)?;
                }
            },
            Err(e) => {
                log::warn!("Lexer error: {}", e);
                Response::ErrorResponse { message: format!("Lexer error: {}", e) }
                    .write(&mut self.stream)?;
                Response::ReadyForQuery.write(&mut self.stream)?;
            }
        }
        self.stream.flush()?;
        Ok(())
    }

    fn build_result_set(
        &self,
        column_names: &[String],
        rows: Vec<Vec<crate::catalog::Value>>,
    ) -> Result<ResultSet, String> {
        // If we have "*", we need to get actual column count from first row
        let actual_column_count = if !rows.is_empty() { rows[0].len() } else { column_names.len() };

        // Build column metadata
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
                    (25, -1) // Default to TEXT
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

        // Convert rows
        for row in rows {
            let fields: Vec<Option<Vec<u8>>> = row.iter().map(serialize_value).collect();
            result_set.add_row(Row::new(fields));
        }

        Ok(result_set)
    }

    fn execute_statement(&self, stmt: Statement) -> Result<ExecutionResult, String> {
        match stmt {
            Statement::CreateTable(create) => {
                self.catalog.create_table_with_constraints(
                    create.table.clone(),
                    create.columns,
                    create.primary_key,
                    create.foreign_keys,
                )?;
                Ok(ExecutionResult::CommandComplete("CREATE TABLE".to_string()))
            }
            Statement::DropTable(drop) => {
                self.catalog.drop_table(&drop.table, drop.if_exists)?;
                Ok(ExecutionResult::CommandComplete("DROP TABLE".to_string()))
            }
            Statement::CreateView(create) => {
                self.catalog.create_view(create.name.clone(), *create.query)?;
                Ok(ExecutionResult::CommandComplete("CREATE VIEW".to_string()))
            }
            Statement::DropView(drop) => {
                self.catalog.drop_view(&drop.name, drop.if_exists)?;
                Ok(ExecutionResult::CommandComplete("DROP VIEW".to_string()))
            }
            Statement::CreateMaterializedView(create) => {
                self.catalog.create_materialized_view(create.name.clone(), *create.query)?;
                Ok(ExecutionResult::CommandComplete("CREATE MATERIALIZED VIEW".to_string()))
            }
            Statement::RefreshMaterializedView(refresh) => {
                self.catalog.refresh_materialized_view(&refresh.name)?;
                Ok(ExecutionResult::CommandComplete("REFRESH MATERIALIZED VIEW".to_string()))
            }
            Statement::DropMaterializedView(drop) => {
                self.catalog.drop_materialized_view(&drop.name, drop.if_exists)?;
                Ok(ExecutionResult::CommandComplete("DROP MATERIALIZED VIEW".to_string()))
            }
            Statement::CreateTrigger(create) => {
                self.catalog.create_trigger(create)?;
                Ok(ExecutionResult::CommandComplete("CREATE TRIGGER".to_string()))
            }
            Statement::DropTrigger(drop) => {
                self.catalog.drop_trigger(&drop.name, drop.if_exists)?;
                Ok(ExecutionResult::CommandComplete("DROP TRIGGER".to_string()))
            }
            Statement::CreateIndex(create) => {
                self.catalog.create_index(create)?;
                Ok(ExecutionResult::CommandComplete("CREATE INDEX".to_string()))
            }
            Statement::DropIndex(drop) => {
                self.catalog.drop_index(&drop.name, drop.if_exists)?;
                Ok(ExecutionResult::CommandComplete("DROP INDEX".to_string()))
            }
            Statement::Describe(desc) => {
                if let Some(schema) = self.catalog.get_table(&desc.table) {
                    let cols: Vec<String> = schema
                        .columns
                        .iter()
                        .map(|c| format!("{}: {:?}", c.name, c.data_type))
                        .collect();
                    Ok(ExecutionResult::CommandComplete(format!(
                        "DESCRIBE
{}",
                        cols.join(
                            "
"
                        )
                    )))
                } else {
                    Err(format!("Table '{}' does not exist", desc.table))
                }
            }
            Statement::Insert(insert) => {
                // Handle multi-row INSERT using batch_insert if there are additional rows
                if insert.batch_values.is_empty() {
                    // Single row INSERT
                    self.catalog.insert(&insert.table, &insert.columns, insert.values)?;
                    Ok(ExecutionResult::CommandComplete("INSERT 0 1".to_string()))
                } else {
                    // Multi-row INSERT: combine first row with batch_values
                    let mut all_rows = vec![insert.values];
                    all_rows.extend(insert.batch_values);
                    let count =
                        self.catalog.batch_insert(&insert.table, &insert.columns, all_rows)?;
                    Ok(ExecutionResult::CommandComplete(format!("INSERT 0 {}", count)))
                }
            }
            Statement::Select(select_stmt) => {
                // Renamed 'select' to 'select_stmt' to avoid shadowing
                log::debug!(
                    "Executing SELECT: from={}, joins={}, has_where={}",
                    select_stmt.from,
                    select_stmt.joins.len(),
                    select_stmt.where_clause.is_some()
                );

                // Check if this is a materialized view first
                if let Some((mv_data, column_names)) =
                    self.catalog.get_materialized_view_with_columns(&select_stmt.from)
                {
                    log::debug!(
                        "Found materialized view: {} with {} columns",
                        select_stmt.from,
                        column_names.len()
                    );
                    // Return the stored data from the materialized view
                    let result_set = self.build_result_set(&column_names, mv_data)?;
                    return Ok(ExecutionResult::ResultSet(result_set));
                }

                if !select_stmt.joins.is_empty() {
                    return self.execute_join_query(select_stmt);
                }

                // Use the planner to build the execution plan
                let planner = Planner::new_with_catalog(self.catalog.clone());
                let mut plan = planner.plan(&select_stmt).map_err(|e| format!("{:?}", e))?;

                // Collect results by calling next() on the root executor
                let mut rows: Vec<Vec<Value>> = Vec::new();
                let mut output_column_names: Option<Vec<String>> = None;

                loop {
                    match plan.next() {
                        Ok(Some(tuple_hashmap)) => {
                            let mut row = Vec::new();

                            // Determine column names from the first tuple
                            if output_column_names.is_none() {
                                output_column_names = Some(tuple_hashmap.keys().cloned().collect());
                            }

                            // Collect values in the order of column names
                            if let Some(ref col_names) = output_column_names {
                                for col_name in col_names {
                                    row.push(
                                        tuple_hashmap.get(col_name).cloned().unwrap_or(Value::Null),
                                    );
                                }
                            }
                            rows.push(row);
                        }
                        Ok(None) => break, // End of data
                        Err(e) => return Err(format!("{:?}", e)),
                    }
                }

                let column_names = output_column_names.unwrap_or_default();

                log::trace!("planner execution returned {} rows", rows.len());

                // Build result set
                let result_set = self.build_result_set(&column_names, rows)?;
                Ok(ExecutionResult::ResultSet(result_set))
            }
            Statement::Update(update) => {
                let count =
                    self.catalog.update(&update.table, update.assignments, update.where_clause)?;
                Ok(ExecutionResult::CommandComplete(format!("UPDATE {}", count)))
            }
            Statement::Delete(delete) => {
                let count = self.catalog.delete(&delete.table, delete.where_clause)?;
                Ok(ExecutionResult::CommandComplete(format!("DELETE {}", count)))
            }
            _ => Ok(ExecutionResult::CommandComplete("SELECT 0".to_string())),
        }
    }

    fn execute_join_query(
        &self,
        select: crate::parser::ast::SelectStmt,
    ) -> Result<ExecutionResult, String> {
        use crate::catalog::predicate::PredicateEvaluator;
        use crate::parser::ast::JoinType;

        log::info!(
            "[JOIN] Executing JOIN query. From: {}, Joins: {}",
            select.from,
            select.joins.len()
        );

        let left_table = &select.from;
        let left_schema = self
            .catalog
            .get_table(left_table)
            .ok_or_else(|| format!("Table '{}' not found", left_table))?;
        let left_alias = select.table_alias.as_ref().unwrap_or(left_table);

        let mut all_schemas = vec![(left_alias.clone(), left_schema.clone())];
        for join in &select.joins {
            let schema = self
                .catalog
                .get_table(&join.table)
                .ok_or_else(|| format!("Table '{}' not found", join.table))?;
            let alias = join.alias.as_ref().unwrap_or(&join.table);
            all_schemas.push((alias.clone(), schema.clone()));
        }

        log::info!(
            "[JOIN] Schema map: {:?}",
            all_schemas
                .iter()
                .map(|(a, s)| (a.clone(), s.name.clone()))
                .collect::<Vec<(String, String)>>() // Explicit type annotation
        );

        let snapshot = self.catalog.txn_mgr.get_snapshot();
        let data = self.catalog.data.read().unwrap();
        let left_tuples =
            data.get(left_table).ok_or_else(|| format!("Table '{}' has no data", left_table))?;

        let mut results = Vec::new();
        for left_tuple in left_tuples {
            if !left_tuple.header.is_visible(&snapshot, &self.catalog.txn_mgr) {
                continue;
            }

            let mut current_row = left_tuple.data.clone();
            let mut matched = true;

            for (join_idx, join) in select.joins.iter().enumerate() {
                let right_tuples = data
                    .get(&join.table)
                    .ok_or_else(|| format!("Table '{}' has no data", join.table))?;

                let mut join_matched = false;
                for right_tuple in right_tuples {
                    if !right_tuple.header.is_visible(&snapshot, &self.catalog.txn_mgr) {
                        continue;
                    }

                    let combined = [current_row.clone(), right_tuple.data.clone()].concat();
                    let combined_schema =
                        self.build_combined_schema(&all_schemas[..=join_idx + 1])?;
                    if PredicateEvaluator::evaluate(&join.on, &combined, &combined_schema)? {
                        current_row.extend_from_slice(&right_tuple.data);
                        join_matched = true;
                        break;
                    }
                }

                if !join_matched && join.join_type == JoinType::Inner {
                    matched = false;
                    break;
                }
            }

            if matched {
                if let Some(ref where_clause) = select.where_clause {
                    let combined_schema = self.build_combined_schema(&all_schemas)?;
                    if !PredicateEvaluator::evaluate(where_clause, &current_row, &combined_schema)?
                    {
                        continue;
                    }
                }
                results.push(current_row);
            }
        }

        let column_names = self.extract_column_names(&select.columns, &all_schemas)?;
        let projected = self.project_columns(&results, &select.columns, &all_schemas)?;
        let result_set = self.build_result_set(&column_names, projected)?;
        Ok(ExecutionResult::ResultSet(result_set))
    }

    fn build_combined_schema(
        &self,
        schemas: &[(String, crate::catalog::TableSchema)],
    ) -> Result<crate::catalog::TableSchema, String> {
        let mut combined_cols = Vec::new();
        for (_, schema) in schemas {
            combined_cols.extend(schema.columns.clone());
        }
        Ok(crate::catalog::TableSchema::new("combined".to_string(), combined_cols))
    }

    fn extract_column_names(
        &self,
        exprs: &[crate::parser::ast::Expr],
        _schemas: &[(String, crate::catalog::TableSchema)],
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

    fn project_columns(
        &self,
        rows: &[Vec<crate::catalog::Value>],
        exprs: &[crate::parser::ast::Expr],
        schemas: &[(String, crate::catalog::TableSchema)],
    ) -> Result<Vec<Vec<crate::catalog::Value>>, String> {
        log::debug!("[PROJ] Projecting {} expressions, {} rows", exprs.len(), rows.len());
        if exprs.is_empty() || (exprs.len() == 1 && matches!(exprs[0], Expr::Star)) {
            return Ok(rows.to_vec());
        }

        let mut result = Vec::new();
        for row in rows {
            let mut projected = Vec::new();
            for expr in exprs {
                log::debug!("[PROJ] Processing expression: {:?}", expr);
                match expr {
                    Expr::QualifiedColumn { table, column } => {
                        let mut offset = 0;
                        let mut found = false;
                        for (tbl_alias, schema) in schemas {
                            log::debug!(
                                "[PROJ] Checking alias '{}' for '{}.{}', schema has {} cols",
                                tbl_alias,
                                table,
                                column,
                                schema.columns.len()
                            );
                            if tbl_alias == table {
                                log::debug!(
                                    "[PROJ] Alias matched! Looking for column '{}' in {:?}",
                                    column,
                                    schema.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
                                );
                                if let Some(idx) =
                                    schema.columns.iter().position(|c| &c.name == column)
                                {
                                    log::debug!("[PROJ] Found at offset {} + idx {}", offset, idx);
                                    projected.push(row[offset + idx].clone());
                                    found = true;
                                    break;
                                }
                            }
                            offset += schema.columns.len();
                        }
                        if !found {
                            return Err(format!("Column '{}.{}' not found", table, column));
                        }
                    }
                    Expr::Column(name) => {
                        // Handle table-prefixed column names (e.g., "o.total" -> "total")
                        let lookup_name = if let Some(dot_pos) = name.find('.') {
                            &name[dot_pos + 1..]
                        } else {
                            name.as_str()
                        };

                        let mut offset = 0;
                        let mut found = false;
                        for (_, schema) in schemas {
                            log::debug!(
                                "[PROJ] Looking for column '{}' (lookup: '{}') in schema with {} cols",
                                name,
                                lookup_name,
                                schema.columns.len()
                            );
                            if let Some(idx) =
                                schema.columns.iter().position(|c| c.name == lookup_name)
                            {
                                log::debug!("[PROJ] Found at offset {} + idx {}", offset, idx);
                                projected.push(row[offset + idx].clone());
                                found = true;
                                break;
                            }
                            offset += schema.columns.len();
                        }
                        if !found {
                            return Err(format!("Column '{}' not found", name));
                        }
                    }
                    Expr::Alias { expr: inner_expr, alias: _ } => {
                        // For aliases, evaluate the inner expression
                        // The alias name is used for column naming, handled separately
                        let value = self.evaluate_expression(inner_expr, row, schemas)?;
                        projected.push(value);
                    }
                    Expr::BinaryOp { left, op, right } => {
                        let left_val = self.evaluate_expression(left, row, schemas)?;
                        let right_val = self.evaluate_expression(right, row, schemas)?;
                        let result = self.evaluate_binary_op(&left_val, *op, &right_val)?;
                        projected.push(result);
                    }
                    Expr::UnaryOp { op, expr } => {
                        let val = self.evaluate_expression(expr, row, schemas)?;
                        let result = self.evaluate_unary_op(&val, *op)?;
                        projected.push(result);
                    }
                    Expr::Number(n) => {
                        projected.push(crate::catalog::Value::Int(*n));
                    }
                    Expr::Float(f) => {
                        projected.push(crate::catalog::Value::Float(*f));
                    }
                    Expr::String(s) => {
                        projected.push(crate::catalog::Value::Text(s.clone()));
                    }
                    Expr::Null => {
                        projected.push(crate::catalog::Value::Null);
                    }
                    Expr::Star => {
                        // Return all columns
                        projected.extend(row.iter().cloned());
                    }
                    _ => {
                        return Err(format!("Unsupported expression in SELECT: {:?}", expr));
                    }
                }
            }
            result.push(projected);
        }
        Ok(result)
    }

    /// Evaluate a single expression against a row
    fn evaluate_expression(
        &self,
        expr: &crate::parser::ast::Expr,
        row: &[crate::catalog::Value],
        schemas: &[(String, crate::catalog::TableSchema)],
    ) -> Result<crate::catalog::Value, String> {
        match expr {
            Expr::Column(name) => {
                let lookup_name = if let Some(dot_pos) = name.find('.') {
                    &name[dot_pos + 1..]
                } else {
                    name.as_str()
                };

                let mut offset = 0;
                for (_, schema) in schemas {
                    if let Some(idx) = schema.columns.iter().position(|c| c.name == lookup_name) {
                        return Ok(row[offset + idx].clone());
                    }
                    offset += schema.columns.len();
                }
                Err(format!("Column '{}' not found", name))
            }
            Expr::QualifiedColumn { table, column } => {
                let mut offset = 0;
                for (tbl_alias, schema) in schemas {
                    if tbl_alias == table {
                        if let Some(idx) = schema.columns.iter().position(|c| &c.name == column) {
                            return Ok(row[offset + idx].clone());
                        }
                    }
                    offset += schema.columns.len();
                }
                Err(format!("Column '{}.{}' not found", table, column))
            }
            Expr::Alias { expr: inner_expr, .. } => {
                self.evaluate_expression(inner_expr, row, schemas)
            }
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_expression(left, row, schemas)?;
                let right_val = self.evaluate_expression(right, row, schemas)?;
                self.evaluate_binary_op(&left_val, *op, &right_val)
            }
            Expr::UnaryOp { op, expr } => {
                let val = self.evaluate_expression(expr, row, schemas)?;
                self.evaluate_unary_op(&val, *op)
            }
            Expr::Number(n) => Ok(crate::catalog::Value::Int(*n)),
            Expr::Float(f) => Ok(crate::catalog::Value::Float(*f)),
            Expr::String(s) => Ok(crate::catalog::Value::Text(s.clone())),
            Expr::Null => Ok(crate::catalog::Value::Null),
            Expr::Star => Err("Cannot evaluate STAR expression".to_string()),
            _ => Err(format!("Unsupported expression: {:?}", expr)),
        }
    }

    /// Evaluate binary operations
    fn evaluate_binary_op(
        &self,
        left: &crate::catalog::Value,
        op: crate::parser::ast::BinaryOperator,
        right: &crate::catalog::Value,
    ) -> Result<crate::catalog::Value, String> {
        use crate::catalog::Value;
        use crate::parser::ast::BinaryOperator;

        match op {
            BinaryOperator::Equals => Ok(Value::Bool(left == right)),
            BinaryOperator::NotEquals => Ok(Value::Bool(left != right)),
            BinaryOperator::LessThan => {
                self.compare_values(left, right, |cmp| cmp == std::cmp::Ordering::Less)
            }
            BinaryOperator::LessThanOrEqual => {
                self.compare_values(left, right, |cmp| cmp != std::cmp::Ordering::Greater)
            }
            BinaryOperator::GreaterThan => {
                self.compare_values(left, right, |cmp| cmp == std::cmp::Ordering::Greater)
            }
            BinaryOperator::GreaterThanOrEqual => {
                self.compare_values(left, right, |cmp| cmp != std::cmp::Ordering::Less)
            }
            BinaryOperator::And => match (left, right) {
                (Value::Bool(l), Value::Bool(r)) => Ok(Value::Bool(*l && *r)),
                _ => Err("AND requires boolean operands".to_string()),
            },
            BinaryOperator::Or => match (left, right) {
                (Value::Bool(l), Value::Bool(r)) => Ok(Value::Bool(*l || *r)),
                _ => Err("OR requires boolean operands".to_string()),
            },
            BinaryOperator::Add => self.arithmetic_op(left, right, |l, r| l + r),
            BinaryOperator::Subtract => self.arithmetic_op(left, right, |l, r| l - r),
            BinaryOperator::Multiply => self.arithmetic_op(left, right, |l, r| l * r),
            BinaryOperator::Divide => self.arithmetic_op(left, right, |l, r| l / r),
            BinaryOperator::Modulo => self.arithmetic_op(left, right, |l, r| l % r),
            _ => Err(format!("Unsupported binary operator: {:?}", op)),
        }
    }

    /// Evaluate unary operations
    fn evaluate_unary_op(
        &self,
        val: &crate::catalog::Value,
        op: crate::parser::ast::UnaryOperator,
    ) -> Result<crate::catalog::Value, String> {
        use crate::catalog::Value;
        use crate::parser::ast::UnaryOperator;

        match op {
            UnaryOperator::Minus => match val {
                Value::Int(n) => Ok(Value::Int(-n)),
                Value::Float(f) => Ok(Value::Float(-f)),
                _ => Err("Unary minus requires numeric operand".to_string()),
            },
            UnaryOperator::Not => match val {
                Value::Bool(b) => Ok(Value::Bool(!b)),
                _ => Err("NOT requires boolean operand".to_string()),
            },
        }
    }

    /// Helper for comparison operations
    fn compare_values<F>(
        &self,
        left: &crate::catalog::Value,
        right: &crate::catalog::Value,
        cmp_fn: F,
    ) -> Result<crate::catalog::Value, String>
    where
        F: Fn(std::cmp::Ordering) -> bool,
    {
        use crate::catalog::Value;

        match (left, right) {
            (Value::Int(l), Value::Int(r)) => Ok(Value::Bool(cmp_fn(l.cmp(r)))),
            (Value::Float(l), Value::Float(r)) => {
                Ok(Value::Bool(l.partial_cmp(r).is_some_and(cmp_fn)))
            }
            (Value::Text(l), Value::Text(r)) => Ok(Value::Bool(cmp_fn(l.cmp(r)))),
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Bool(false)),
            _ => Err("Comparison requires compatible types".to_string()),
        }
    }

    /// Helper for arithmetic operations
    fn arithmetic_op<F>(
        &self,
        left: &crate::catalog::Value,
        right: &crate::catalog::Value,
        op_fn: F,
    ) -> Result<crate::catalog::Value, String>
    where
        F: Fn(i64, i64) -> i64,
    {
        use crate::catalog::Value;

        match (left, right) {
            (Value::Int(l), Value::Int(r)) => Ok(Value::Int(op_fn(*l, *r))),
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            _ => Err("Arithmetic requires integer operands".to_string()),
        }
    }

    pub fn run(&mut self) -> Result<(), ProtocolError> {
        // Handle SSL negotiation request first
        let mut first_bytes = [0u8; 8];
        if self.stream.read_exact(&mut first_bytes).is_err() {
            // Not enough data for SSL or startup, probably just a terminate
            return Ok(());
        }

        let len =
            i32::from_be_bytes([first_bytes[0], first_bytes[1], first_bytes[2], first_bytes[3]]);
        let code =
            i32::from_be_bytes([first_bytes[4], first_bytes[5], first_bytes[6], first_bytes[7]]);

        if len == 8 && code == 80877103 {
            // SSL request
            log::debug!("SSL negotiation rejected");
            self.stream.write_all(b"N")?;
            self.stream.flush()?;
            // After rejecting SSL, a startup message is expected
            self.handle_startup()?;
        } else {
            // Not an SSL request, so it's a startup message.
            // We've already read the first 8 bytes.
            let mut remaining_data = vec![0u8; (len - 8) as usize];
            self.stream.read_exact(&mut remaining_data)?;
            let mut data = first_bytes[4..].to_vec();
            data.extend_from_slice(&remaining_data);

            let msg = Message::parse(0, &data)?;
            log::debug!("Startup message: {:?}", msg);
            self.authenticated = true;

            Response::AuthenticationOk.write(&mut self.stream)?;
            Response::ReadyForQuery.write(&mut self.stream)?;
            self.stream.flush()?;
        }

        loop {
            let mut tag_buf = [0u8; 1];
            if self.stream.read_exact(&mut tag_buf).is_err() {
                break;
            }

            let mut len_buf = [0u8; 4];
            self.stream.read_exact(&mut len_buf)?;
            let len = i32::from_be_bytes(len_buf) as usize;

            let mut data = vec![0u8; len - 4];
            self.stream.read_exact(&mut data)?;

            let msg = Message::parse(tag_buf[0], &data)?;

            match msg {
                Message::Query { sql } => self.handle_query(&sql)?,
                Message::Terminate => break,
                _ => {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
impl Connection<std::io::Cursor<Vec<u8>>> {
    pub fn dummy() -> Self {
        let catalog = Arc::new(Catalog::new());
        let cursor = std::io::Cursor::new(Vec::new());
        Connection::new(cursor, catalog)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Column, DataType, TableSchema};
    use crate::parser::ast::ColumnDef;
    use std::io::{Cursor, Read, Write};
    use std::sync::Arc;

    struct MockStream {
        input: Cursor<Vec<u8>>,
        output: Cursor<Vec<u8>>,
    }

    impl MockStream {
        fn new(input_data: Vec<u8>) -> Self {
            Self { input: Cursor::new(input_data), output: Cursor::new(Vec::new()) }
        }

        fn get_output(&self) -> Vec<u8> {
            self.output.get_ref().clone()
        }
    }

    impl Read for MockStream {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.input.read(buf)
        }
    }

    impl Write for MockStream {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.output.write(buf)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.output.flush()
        }
    }

    // Helper to create a byte vector simulating a message from the client
    fn create_message(msg_type: u8, payload: &[u8]) -> Vec<u8> {
        let len = (payload.len() + 4) as i32; // Length includes itself
        let mut buf = Vec::new();
        buf.push(msg_type); // Message type tag
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(payload);
        buf
    }

    // Helper for startup message without SSL
    fn create_startup_message(payload: &[u8]) -> Vec<u8> {
        let len = (payload.len() + 4) as i32;
        let mut buf = Vec::new();
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(payload);
        buf
    }

    // Helper for SSLRequest
    fn create_ssl_request() -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&8i32.to_be_bytes()); // length
        buf.extend_from_slice(&80877103i32.to_be_bytes()); // SSLRequest code
        buf
    }

    #[test]
    fn test_connection_creation() {
        let mut stream = MockStream::new(vec![]);
        let catalog = Arc::new(Catalog::new());
        let conn = Connection::new(&mut stream, catalog);
        assert!(!conn.authenticated);
    }

    #[test]
    fn test_handle_startup_successful() {
        let payload = b"\0\x03\0\0user\0testuser\0database\0testdb\0\0"; // Protocol version 3.0, user and database
        let input_data = create_startup_message(payload);
        let mut stream = MockStream::new(input_data);
        let catalog = Arc::new(Catalog::new());
        let mut conn = Connection::new(&mut stream, catalog);

        conn.handle_startup().unwrap();
        assert!(conn.authenticated);

        let output = stream.get_output();
        // Expected: AuthenticationOk (R) + ReadyForQuery (Z)
        let expected_output = b"R\0\0\0\x08\0\0\0\0Z\0\0\0\x05I";
        assert_eq!(&output[..], expected_output);
    }

    #[test]
    fn test_handle_startup_ssl_rejected() {
        let ssl_request = create_ssl_request();
        let payload = b"\0\x03\0\0user\0testuser\0database\0testdb\0\0";
        let startup_message = create_startup_message(payload);

        let mut input_stream_data = Vec::new();
        input_stream_data.extend_from_slice(&ssl_request);
        input_stream_data.extend_from_slice(&startup_message);

        let mut stream = MockStream::new(input_stream_data);
        let mut conn = Connection::new(&mut stream, Arc::new(Catalog::new()));

        let _ = conn.run();

        let output = stream.get_output();
        // Expect: 'N' for SSL rejection, then AuthenticationOk (R) and ReadyForQuery (Z)
        let mut expected_output = b"N".to_vec(); // SSL rejection
        expected_output.extend_from_slice(b"R\0\0\0\x08\0\0\0\0Z\0\0\0\x05I"); // AuthOk + ReadyForQuery

        assert!(output.starts_with(&expected_output));
    }

    #[test]
    fn test_handle_startup_malformed_message() {
        // Malformed: length says 100, but payload is only 2 bytes
        let malformed_payload = vec![0x03, 0x00]; // Protocol version 3.0

        let mut input_data = Vec::new();
        input_data.extend_from_slice(&100i32.to_be_bytes()); // Length 100
        input_data.extend_from_slice(&malformed_payload);

        let mut stream = Cursor::new(input_data);
        let catalog = Arc::new(Catalog::new());
        let mut conn = Connection::new(stream, catalog);

        // Expect an error because the message is malformed or incomplete
        let result = conn.handle_startup();
        assert!(result.is_err());
        // For Cursor, read_exact for more than available bytes results in an error
        assert!(format!("{:?}", result.unwrap_err()).contains("failed to fill whole buffer"));
    }

    #[test]
    fn test_handle_query_create_table() {
        let create_table_sql = "CREATE TABLE users (id INT, name TEXT);";
        let mut stream = MockStream::new(Vec::new());
        let mut conn = Connection::new(&mut stream, Arc::new(Catalog::new()));
        conn.authenticated = true;

        conn.handle_query(create_table_sql).unwrap();

        let output = stream.get_output();
        // Expect CommandComplete (C) and ReadyForQuery (Z)
        let expected_output = b"C\0\0\0\x11CREATE TABLE\0Z\0\0\0\x05I";
        assert_eq!(output, expected_output);
    }

    #[test]
    fn test_handle_query_insert_and_select() {
        let catalog = Arc::new(Catalog::new());
        let mut stream = MockStream::new(Vec::new());
        let mut conn = Connection::new(&mut stream, catalog.clone());
        conn.authenticated = true;

        // Create table
        let create_table_sql = "CREATE TABLE items (id INT, name TEXT);";
        conn.handle_query(create_table_sql).unwrap();

        // Insert data
        let insert_sql = "INSERT INTO items VALUES (1, 'apple');";
        conn.handle_query(insert_sql).unwrap();

        // Select data
        let select_sql = "SELECT id, name FROM items;";
        conn.handle_query(select_sql).unwrap();

        let output = stream.get_output();
        // Expected: RowDescription (T), DataRow (D), CommandComplete (C), ReadyForQuery (Z)
        // We check for the SELECT 1 command complete tag, but since we run multiple queries, we can't guarantee the whole buffer
        assert!(output.ends_with(b"C\0\0\0\rSELECT 1\0Z\0\0\0\x05I")); // CommandComplete SELECT 1 + ReadyForQuery
    }

    #[test]
    fn test_handle_query_multi_row_insert_with_nulls() {
        let catalog = Arc::new(Catalog::new());
        let mut stream = MockStream::new(Vec::new());
        let mut conn = Connection::new(&mut stream, catalog.clone());
        conn.authenticated = true;

        // Create table with nullable columns
        let create_table_sql = "CREATE TABLE nullable_test (id INT, value INT, txt TEXT);";
        conn.handle_query(create_table_sql).unwrap();

        // Multi-row INSERT with NULL values
        let insert_sql =
            "INSERT INTO nullable_test VALUES (1, 10, 'hello'), (2, NULL, 'world'), (3, 30, NULL);";
        conn.handle_query(insert_sql).unwrap();

        // Verify 3 rows were inserted
        assert_eq!(catalog.row_count("nullable_test"), 3);

        // Select rows where value IS NULL
        let select_sql = "SELECT * FROM nullable_test WHERE value IS NULL;";
        conn.handle_query(select_sql).unwrap();

        let output = stream.get_output();
        let output_str = String::from_utf8_lossy(&output);
        // Should contain the row with id=2 and txt='world'
        assert!(output_str.contains("world"), "Should find row with NULL value");
    }

    #[test]
    fn test_handle_query_is_not_null() {
        let catalog = Arc::new(Catalog::new());
        let mut stream = MockStream::new(Vec::new());
        let mut conn = Connection::new(&mut stream, catalog.clone());
        conn.authenticated = true;

        // Create table
        let create_table_sql = "CREATE TABLE test_null (id INT, value INT);";
        conn.handle_query(create_table_sql).unwrap();

        // Insert data with NULLs
        let insert_sql = "INSERT INTO test_null VALUES (1, 10), (2, NULL), (3, 30);";
        conn.handle_query(insert_sql).unwrap();

        // Select rows where value IS NOT NULL
        let select_sql = "SELECT * FROM test_null WHERE value IS NOT NULL;";
        conn.handle_query(select_sql).unwrap();

        let output = stream.get_output();
        let output_str = String::from_utf8_lossy(&output);
        // Should contain rows with id=1 and id=3, but not id=2
        assert!(output_str.contains("10"), "Should find row with value 10");
        assert!(output_str.contains("30"), "Should find row with value 30");
    }

    #[test]
    fn test_handle_query_order_by_with_non_selected_column() {
        // Test ORDER BY with column not in SELECT list
        let catalog = Arc::new(Catalog::new());
        let mut stream = MockStream::new(Vec::new());
        let mut conn = Connection::new(&mut stream, catalog.clone());
        conn.authenticated = true;

        // Create table
        let create_table_sql = "CREATE TABLE products (id INT, name TEXT, price INT);";
        conn.handle_query(create_table_sql).unwrap();

        // Insert data
        let insert_sql =
            "INSERT INTO products VALUES (1, 'Apple', 100), (2, 'Banana', 50), (3, 'Cherry', 200);";
        conn.handle_query(insert_sql).unwrap();

        // SELECT name ORDER BY price (price is not in SELECT list)
        let select_sql = "SELECT name FROM products ORDER BY price ASC;";
        conn.handle_query(select_sql).unwrap();

        let output = stream.get_output();
        let output_str = String::from_utf8_lossy(&output);
        // Should be ordered by price: Banana (50), Apple (100), Cherry (200)
        let banana_pos = output_str.find("Banana").unwrap_or(usize::MAX);
        let apple_pos = output_str.find("Apple").unwrap_or(usize::MAX);
        let cherry_pos = output_str.find("Cherry").unwrap_or(usize::MAX);
        assert!(banana_pos < apple_pos && apple_pos < cherry_pos, "Should be ordered by price ASC");
    }

    #[test]
    fn test_handle_query_order_by_limit_offset_with_non_selected_column() {
        // Test ORDER BY + LIMIT + OFFSET with column not in SELECT list
        let catalog = Arc::new(Catalog::new());
        let mut stream = MockStream::new(Vec::new());
        let mut conn = Connection::new(&mut stream, catalog.clone());
        conn.authenticated = true;

        // Create table
        let create_table_sql = "CREATE TABLE items (id INT, name TEXT, price INT);";
        conn.handle_query(create_table_sql).unwrap();

        // Insert data
        let insert_sql =
            "INSERT INTO items VALUES (1, 'Cheap', 10), (2, 'Medium', 50), (3, 'Expensive', 100);";
        conn.handle_query(insert_sql).unwrap();

        // SELECT name ORDER BY price ASC LIMIT 1 OFFSET 1
        // Should return the second cheapest item: "Medium"
        let select_sql = "SELECT name FROM items ORDER BY price ASC LIMIT 1 OFFSET 1;";
        conn.handle_query(select_sql).unwrap();

        let output = stream.get_output();
        let output_str = String::from_utf8_lossy(&output);
        // Should return only "Medium" (the second cheapest)
        assert!(output_str.contains("Medium"), "Should return Medium (second cheapest)");
        assert!(!output_str.contains("Cheap"), "Should not return Cheap (cheapest)");
        assert!(!output_str.contains("Expensive"), "Should not return Expensive (most expensive)");
    }

    #[test]
    fn test_handle_query_select_without_from_clause() {
        // Test SELECT without FROM clause (e.g., SELECT COALESCE(...), SELECT 1+1)
        let catalog = Arc::new(Catalog::new());
        let mut stream = MockStream::new(Vec::new());
        let mut conn = Connection::new(&mut stream, catalog.clone());
        conn.authenticated = true;

        // SELECT COALESCE(NULL, NULL, 'default_value') - no FROM clause
        let select_sql = "SELECT COALESCE(NULL, NULL, 'default_value');";
        conn.handle_query(select_sql).unwrap();

        let output = stream.get_output();
        let output_str = String::from_utf8_lossy(&output);
        // Should contain the default value
        assert!(output_str.contains("default_value"), "COALESCE should return first non-NULL");
    }

    #[test]
    fn test_handle_query_select_expression_without_from() {
        // Test SELECT with expression but no FROM clause
        let catalog = Arc::new(Catalog::new());
        let mut stream = MockStream::new(Vec::new());
        let mut conn = Connection::new(&mut stream, catalog.clone());
        conn.authenticated = true;

        // SELECT 1 + 1 - no FROM clause
        let select_sql = "SELECT 1 + 1;";
        conn.handle_query(select_sql).unwrap();

        let output = stream.get_output();
        // Should return a result (we just check it doesn't error)
        assert!(!output.is_empty(), "Should return a result");
    }

    #[test]
    fn test_handle_query_syntax_error() {
        let invalid_sql = "SELECT FROM users;"; // Missing column list
        let mut stream = MockStream::new(Vec::new());
        let mut conn = Connection::new(&mut stream, Arc::new(Catalog::new()));
        conn.authenticated = true;

        conn.handle_query(invalid_sql).unwrap();

        let output = stream.get_output();
        // Expect ErrorResponse (E) and ReadyForQuery (Z)
        assert!(output.starts_with(b"E\0\0\0"));
        assert!(output.windows(12).any(|window| window == b"Parse error:"));
        assert!(output.ends_with(b"Z\0\0\0\x05I"));
    }

    #[test]
    fn test_build_result_set_simple() {
        let mut stream = MockStream::new(vec![]);
        let catalog = Arc::new(Catalog::new());
        let conn = Connection::new(&mut stream, catalog.clone());

        let column_names = vec!["id".to_string(), "name".to_string()];
        let rows = vec![
            vec![Value::Int(1), Value::Text("Alice".to_string())],
            vec![Value::Int(2), Value::Text("Bob".to_string())],
        ];

        let result_set = conn.build_result_set(&column_names, rows).unwrap();
        assert_eq!(result_set.row_count(), 2);
        assert_eq!(result_set.columns.len(), 2);
        assert_eq!(result_set.columns[0].name, "id");
        assert_eq!(result_set.columns[1].name, "name");
    }

    #[test]
    fn test_build_result_set_star() {
        let mut stream = MockStream::new(vec![]);
        let catalog = Arc::new(Catalog::new());
        let conn = Connection::new(&mut stream, catalog.clone());

        let column_names = vec!["*".to_string()];
        let rows = vec![
            vec![Value::Int(1), Value::Text("Alice".to_string())],
            vec![Value::Int(2), Value::Text("Bob".to_string())],
        ];

        let result_set = conn.build_result_set(&column_names, rows).unwrap();
        assert_eq!(result_set.row_count(), 2);
        assert_eq!(result_set.columns.len(), 2);
        assert_eq!(result_set.columns[0].name, "column1");
        assert_eq!(result_set.columns[1].name, "column2");
    }

    #[test]
    fn test_build_result_set_empty() {
        let mut stream = MockStream::new(vec![]);
        let catalog = Arc::new(Catalog::new());
        let conn = Connection::new(&mut stream, catalog.clone());

        let column_names = vec!["id".to_string(), "name".to_string()];
        let rows: Vec<Vec<Value>> = Vec::new();

        let result_set = conn.build_result_set(&column_names, rows).unwrap();
        assert_eq!(result_set.row_count(), 0);
        assert_eq!(result_set.columns.len(), 2); // Still expect column metadata for schema
        assert_eq!(result_set.columns[0].name, "id");
        assert_eq!(result_set.columns[1].name, "name");
    }

    #[test]
    fn test_project_columns_select_star() {
        let mut stream = MockStream::new(vec![]);
        let catalog = Arc::new(Catalog::new());
        let conn = Connection::new(&mut stream, catalog.clone());

        let rows = vec![
            vec![Value::Int(1), Value::Text("Alice".to_string())],
            vec![Value::Int(2), Value::Text("Bob".to_string())],
        ];
        let exprs = vec![crate::parser::Expr::Star];
        let schemas = vec![(
            "users".to_string(),
            TableSchema::new(
                "users".to_string(),
                vec![
                    Column::new("id".to_string(), DataType::Int),
                    Column::new("name".to_string(), DataType::Text),
                ],
            ),
        )];

        let projected_rows = conn.project_columns(&rows, &exprs, &schemas).unwrap();
        assert_eq!(projected_rows.len(), 2);
        assert_eq!(projected_rows[0].len(), 2);
        assert_eq!(projected_rows[0][0], Value::Int(1));
    }

    #[test]
    fn test_project_columns_select_specific() {
        let mut stream = MockStream::new(vec![]);
        let catalog = Arc::new(Catalog::new());
        let conn = Connection::new(&mut stream, catalog.clone());

        let rows = vec![
            vec![Value::Int(1), Value::Text("Alice".to_string())],
            vec![Value::Int(2), Value::Text("Bob".to_string())],
        ];
        let exprs = vec![crate::parser::Expr::Column("name".to_string())];
        let schemas = vec![(
            "users".to_string(),
            TableSchema::new(
                "users".to_string(),
                vec![
                    Column::new("id".to_string(), DataType::Int),
                    Column::new("name".to_string(), DataType::Text),
                ],
            ),
        )];

        let projected_rows = conn.project_columns(&rows, &exprs, &schemas).unwrap();
        assert_eq!(projected_rows.len(), 2);
        assert_eq!(projected_rows[0].len(), 1);
        assert_eq!(projected_rows[0][0], Value::Text("Alice".to_string()));
    }

    #[test]
    fn test_project_columns_qualified() {
        let mut stream = MockStream::new(vec![]);
        let catalog = Arc::new(Catalog::new());
        let conn = Connection::new(&mut stream, catalog.clone());

        let rows = vec![
            vec![Value::Int(1), Value::Text("Alice".to_string())],
            vec![Value::Int(2), Value::Text("Bob".to_string())],
        ];
        let exprs = vec![crate::parser::Expr::QualifiedColumn {
            table: "u".to_string(),
            column: "name".to_string(),
        }];
        let schemas = vec![(
            "u".to_string(),
            TableSchema::new(
                "users".to_string(),
                vec![
                    Column::new("id".to_string(), DataType::Int),
                    Column::new("name".to_string(), DataType::Text),
                ],
            ),
        )];

        let projected_rows = conn.project_columns(&rows, &exprs, &schemas).unwrap();
        assert_eq!(projected_rows.len(), 2);
        assert_eq!(projected_rows[0].len(), 1);
        assert_eq!(projected_rows[0][0], Value::Text("Alice".to_string()));
    }

    #[test]
    fn test_project_columns_not_found() {
        let mut stream = MockStream::new(vec![]);
        let catalog = Arc::new(Catalog::new());
        let conn = Connection::new(&mut stream, catalog.clone());

        let rows = vec![vec![Value::Int(1), Value::Text("Alice".to_string())]];
        let exprs = vec![crate::parser::Expr::Column("nonexistent".to_string())];
        let schemas = vec![(
            "users".to_string(),
            TableSchema::new(
                "users".to_string(),
                vec![
                    Column::new("id".to_string(), DataType::Int),
                    Column::new("name".to_string(), DataType::Text),
                ],
            ),
        )];

        let result = conn.project_columns(&rows, &exprs, &schemas);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Column 'nonexistent' not found"));
    }

    #[test]
    fn test_run_query_then_terminate() {
        let catalog = Arc::new(Catalog::new());

        // Setup: Create table and insert data using the catalog directly
        let create_table_sql = "CREATE TABLE test_data (id INT);";
        let create_table_stmt_enum = Parser::new(create_table_sql).unwrap().parse().unwrap();
        if let Statement::CreateTable(create_table_stmt) = create_table_stmt_enum {
            catalog
                .create_table_with_constraints(
                    create_table_stmt.table,
                    create_table_stmt.columns,
                    None,
                    vec![],
                )
                .unwrap();
        } else {
            panic!("Expected CreateTable statement");
        }
        let insert_sql = "INSERT INTO test_data VALUES (1);";
        let insert_stmt_enum = Parser::new(insert_sql).unwrap().parse().unwrap();
        if let Statement::Insert(insert_stmt) = insert_stmt_enum {
            catalog.insert(&insert_stmt.table, &insert_stmt.columns, insert_stmt.values).unwrap();
        } else {
            panic!("Expected Insert statement");
        }

        let startup_payload = b"\0\x03\0\0user\0testuser\0database\0testdb\0\0";
        let startup_message = create_startup_message(startup_payload);
        let query_message = create_message(b'Q', b"SELECT id FROM test_data;");
        let terminate_message = create_message(b'X', b""); // Terminate message (empty payload)

        let mut input_stream_data = Vec::new();
        input_stream_data.extend_from_slice(&startup_message);
        input_stream_data.extend_from_slice(&query_message);
        input_stream_data.extend_from_slice(&terminate_message);

        let mut stream = MockStream::new(input_stream_data);
        let mut conn = Connection::new(&mut stream, catalog.clone());

        conn.run().unwrap(); // Run the connection loop

        let output = stream.get_output();
        // Check for AuthenticationOk, ReadyForQuery, RowDescription, DataRow, CommandComplete, ReadyForQuery
        assert!(output.starts_with(b"R\0\0\0\x08\0\0\0\0Z\0\0\0\x05I")); // AuthOk + ReadyForQuery
        assert!(output.windows(4).any(|window| window == b"T\0\0\0")); // RowDescription
        assert!(output.windows(4).any(|window| window == b"D\0\0\0")); // DataRow
        assert!(output.ends_with(b"C\0\0\0\rSELECT 1\0Z\0\0\0\x05I")); // CommandComplete + ReadyForQuery
    }

    #[test]
    fn test_run_ssl_startup_query_terminate() {
        let catalog = Arc::new(Catalog::new());

        // Setup: Create table and insert data using the catalog directly
        let create_table_sql = "CREATE TABLE products (pid INT, pname TEXT);";
        let create_table_stmt_enum = Parser::new(create_table_sql).unwrap().parse().unwrap();
        if let Statement::CreateTable(create_table_stmt) = create_table_stmt_enum {
            catalog
                .create_table_with_constraints(
                    create_table_stmt.table,
                    create_table_stmt.columns,
                    None,
                    vec![],
                )
                .unwrap();
        } else {
            panic!("Expected CreateTable statement");
        }
        let insert_sql = "INSERT INTO products VALUES (101, 'Laptop');";
        let insert_stmt_enum = Parser::new(insert_sql).unwrap().parse().unwrap();
        if let Statement::Insert(insert_stmt) = insert_stmt_enum {
            catalog.insert(&insert_stmt.table, &insert_stmt.columns, insert_stmt.values).unwrap();
        } else {
            panic!("Expected Insert statement");
        }

        let ssl_request = create_ssl_request();
        let startup_payload = b"\0\x03\0\0user\0testuser\0database\0testdb\0\0";
        let startup_message = create_startup_message(startup_payload);
        let query_message = create_message(b'Q', b"SELECT pid, pname FROM products;");
        let terminate_message = create_message(b'X', b"");

        let mut input_stream_data = Vec::new();
        input_stream_data.extend_from_slice(&ssl_request);
        input_stream_data.extend_from_slice(&startup_message);
        input_stream_data.extend_from_slice(&query_message);
        input_stream_data.extend_from_slice(&terminate_message);

        let mut stream = MockStream::new(input_stream_data);
        let mut conn = Connection::new(&mut stream, catalog.clone());

        conn.run().unwrap();

        let output = stream.get_output();
        // Expect: 'N' for SSL rejection, then AuthOk, ReadyForQuery, RowDescription, DataRow, CommandComplete, ReadyForQuery
        let mut expected_output_prefix = b"N".to_vec();
        expected_output_prefix.extend_from_slice(b"R\0\0\0\x08\0\0\0\0Z\0\0\0\x05I");

        assert!(output.starts_with(&expected_output_prefix));
        assert!(output.windows(4).any(|window| window == b"T\0\0\0")); // RowDescription
        assert!(output.windows(4).any(|window| window == b"D\0\0\0")); // DataRow
        assert!(output.ends_with(b"C\0\0\0\rSELECT 1\0Z\0\0\0\x05I")); // CommandComplete + ReadyForQuery
    }
}

#[cfg(test)]
mod expression_tests {
    use super::*;
    use crate::catalog::{Column, TableSchema, Value};
    use crate::parser::ast::{BinaryOperator, Expr, UnaryOperator};

    fn create_test_schema() -> (String, TableSchema) {
        let columns = vec![
            Column::new("id".to_string(), crate::parser::ast::DataType::Int),
            Column::new("name".to_string(), crate::parser::ast::DataType::Text),
            Column::new("price".to_string(), crate::parser::ast::DataType::Int),
            Column::new("quantity".to_string(), crate::parser::ast::DataType::Int),
        ];
        let schema = TableSchema::new("test".to_string(), columns);
        ("test".to_string(), schema)
    }

    fn create_test_row() -> Vec<Value> {
        vec![Value::Int(1), Value::Text("Laptop".to_string()), Value::Int(1000), Value::Int(5)]
    }

    #[test]
    fn test_evaluate_column() {
        let conn: Connection<std::io::Cursor<Vec<u8>>> = Connection::dummy();
        let schema = create_test_schema();
        let row = create_test_row();

        let result =
            conn.evaluate_expression(&Expr::Column("id".to_string()), &row, &[schema.clone()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int(1));

        let result =
            conn.evaluate_expression(&Expr::Column("name".to_string()), &row, &[schema.clone()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Text("Laptop".to_string()));
    }

    #[test]
    fn test_evaluate_qualified_column() {
        let conn: Connection<std::io::Cursor<Vec<u8>>> = Connection::dummy();
        let schema = create_test_schema();
        let row = create_test_row();

        let result = conn.evaluate_expression(
            &Expr::QualifiedColumn { table: "test".to_string(), column: "price".to_string() },
            &row,
            &[schema.clone()],
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int(1000));
    }

    #[test]
    fn test_evaluate_column_not_found() {
        let conn: Connection<std::io::Cursor<Vec<u8>>> = Connection::dummy();
        let schema = create_test_schema();
        let row = create_test_row();

        let result = conn.evaluate_expression(
            &Expr::Column("nonexistent".to_string()),
            &row,
            &[schema.clone()],
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    #[test]
    fn test_evaluate_alias() {
        let conn: Connection<std::io::Cursor<Vec<u8>>> = Connection::dummy();
        let schema = create_test_schema();
        let row = create_test_row();

        let alias_expr = Expr::Alias {
            expr: Box::new(Expr::Column("price".to_string())),
            alias: "item_price".to_string(),
        };

        let result = conn.evaluate_expression(&alias_expr, &row, &[schema.clone()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int(1000));
    }

    #[test]
    fn test_evaluate_binary_arithmetic() {
        let conn: Connection<std::io::Cursor<Vec<u8>>> = Connection::dummy();
        let schema = create_test_schema();
        let row = create_test_row();

        // Test addition
        let add_expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("price".to_string())),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Number(100)),
        };
        let result = conn.evaluate_expression(&add_expr, &row, &[schema.clone()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int(1100));

        // Test subtraction
        let sub_expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("price".to_string())),
            op: BinaryOperator::Subtract,
            right: Box::new(Expr::Number(200)),
        };
        let result = conn.evaluate_expression(&sub_expr, &row, &[schema.clone()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int(800));

        // Test multiplication
        let mul_expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("price".to_string())),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Number(2)),
        };
        let result = conn.evaluate_expression(&mul_expr, &row, &[schema.clone()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int(2000));

        // Test division
        let div_expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("price".to_string())),
            op: BinaryOperator::Divide,
            right: Box::new(Expr::Number(10)),
        };
        let result = conn.evaluate_expression(&div_expr, &row, &[schema.clone()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int(100));

        // Test modulo
        let mod_expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("price".to_string())),
            op: BinaryOperator::Modulo,
            right: Box::new(Expr::Number(3)),
        };
        let result = conn.evaluate_expression(&mod_expr, &row, &[schema.clone()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int(1));
    }

    #[test]
    fn test_evaluate_binary_comparison() {
        let conn: Connection<std::io::Cursor<Vec<u8>>> = Connection::dummy();
        let schema = create_test_schema();
        let row = create_test_row();

        // Test equals
        let eq_expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(1)),
        };
        let result = conn.evaluate_expression(&eq_expr, &row, &[schema.clone()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Bool(true));

        // Test greater than
        let gt_expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("price".to_string())),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expr::Number(500)),
        };
        let result = conn.evaluate_expression(&gt_expr, &row, &[schema.clone()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Bool(true));

        // Test less than
        let lt_expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("price".to_string())),
            op: BinaryOperator::LessThan,
            right: Box::new(Expr::Number(500)),
        };
        let result = conn.evaluate_expression(&lt_expr, &row, &[schema.clone()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Bool(false));
    }

    #[test]
    fn test_evaluate_binary_logical() {
        let conn: Connection<std::io::Cursor<Vec<u8>>> = Connection::dummy();
        let schema = create_test_schema();
        let row = create_test_row();

        // Test AND
        let and_expr = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::Number(1)),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("price".to_string())),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expr::Number(500)),
            }),
        };
        let result = conn.evaluate_expression(&and_expr, &row, &[schema.clone()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Bool(true));

        // Test OR
        let or_expr = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::Number(2)),
            }),
            op: BinaryOperator::Or,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("price".to_string())),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expr::Number(500)),
            }),
        };
        let result = conn.evaluate_expression(&or_expr, &row, &[schema.clone()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Bool(true));
    }

    #[test]
    fn test_evaluate_unary_minus() {
        let conn: Connection<std::io::Cursor<Vec<u8>>> = Connection::dummy();
        let schema = create_test_schema();
        let row = create_test_row();

        let neg_expr = Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: Box::new(Expr::Column("price".to_string())),
        };
        let result = conn.evaluate_expression(&neg_expr, &row, &[schema.clone()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int(-1000));
    }

    #[test]
    fn test_evaluate_unary_not() {
        let conn: Connection<std::io::Cursor<Vec<u8>>> = Connection::dummy();
        let schema = create_test_schema();
        let row = create_test_row();

        let not_expr = Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::Number(2)),
            }),
        };
        let result = conn.evaluate_expression(&not_expr, &row, &[schema.clone()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Bool(true));
    }

    #[test]
    fn test_evaluate_literals() {
        let conn: Connection<std::io::Cursor<Vec<u8>>> = Connection::dummy();
        let schema = create_test_schema();
        let row = create_test_row();

        // Test number
        let result = conn.evaluate_expression(&Expr::Number(42), &row, &[schema.clone()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int(42));

        // Test float
        let result = conn.evaluate_expression(&Expr::Float(3.14), &row, &[schema.clone()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Float(3.14));

        // Test string
        let result =
            conn.evaluate_expression(&Expr::String("hello".to_string()), &row, &[schema.clone()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Text("hello".to_string()));

        // Test null
        let result = conn.evaluate_expression(&Expr::Null, &row, &[schema.clone()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Null);
    }

    #[test]
    fn test_evaluate_star_error() {
        let conn: Connection<std::io::Cursor<Vec<u8>>> = Connection::dummy();
        let schema = create_test_schema();
        let row = create_test_row();

        let result = conn.evaluate_expression(&Expr::Star, &row, &[schema.clone()]);
        assert!(result.is_err());
    }

    #[test]
    fn test_evaluate_binary_op_type_mismatch() {
        let conn: Connection<std::io::Cursor<Vec<u8>>> = Connection::dummy();
        let schema = create_test_schema();
        let row = create_test_row();

        // Adding text and int should fail
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("name".to_string())),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Number(100)),
        };
        let result = conn.evaluate_expression(&expr, &row, &[schema.clone()]);
        assert!(result.is_err());
    }

    #[test]
    fn test_evaluate_binary_op_null_handling() {
        let conn: Connection<std::io::Cursor<Vec<u8>>> = Connection::dummy();
        let schema = create_test_schema();
        let row = create_test_row();

        // Any arithmetic with null should return null
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Null),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Number(100)),
        };
        let result = conn.evaluate_expression(&expr, &row, &[schema.clone()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Null);
    }

    #[test]
    fn test_compare_values_text() {
        let conn: Connection<std::io::Cursor<Vec<u8>>> = Connection::dummy();
        let schema = create_test_schema();
        let row = create_test_row();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("name".to_string())),
            op: BinaryOperator::LessThan,
            right: Box::new(Expr::String("Mouse".to_string())),
        };
        let result = conn.evaluate_expression(&expr, &row, &[schema.clone()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Bool(true)); // "Laptop" < "Mouse" is true (L < M)

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("name".to_string())),
            op: BinaryOperator::LessThan,
            right: Box::new(Expr::String("Apple".to_string())),
        };
        let result = conn.evaluate_expression(&expr, &row, &[schema.clone()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Bool(false)); // "Laptop" < "Apple" is false (L > A)
    }

    #[test]
    fn test_nested_expressions() {
        let conn: Connection<std::io::Cursor<Vec<u8>>> = Connection::dummy();
        let schema = create_test_schema();
        let row = create_test_row();

        // (price + 100) * 2
        let nested_expr = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("price".to_string())),
                op: BinaryOperator::Add,
                right: Box::new(Expr::Number(100)),
            }),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Number(2)),
        };
        let result = conn.evaluate_expression(&nested_expr, &row, &[schema.clone()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int(2200));
    }

    #[test]
    fn test_project_columns_with_alias() {
        let conn: Connection<std::io::Cursor<Vec<u8>>> = Connection::dummy();
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

        let result = conn.project_columns(&rows, &exprs, &[schema.clone()]);
        assert!(result.is_ok());
        let projected = result.unwrap();
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].len(), 2);
        assert_eq!(projected[0][0], Value::Int(1000));
        assert_eq!(projected[0][1], Value::Text("Laptop".to_string()));
    }

    #[test]
    fn test_project_columns_with_arithmetic() {
        let conn: Connection<std::io::Cursor<Vec<u8>>> = Connection::dummy();
        let schema = create_test_schema();
        let row = create_test_row();
        let rows = vec![row.clone()];

        let exprs = vec![
            Expr::Column("name".to_string()),
            Expr::BinaryOp {
                left: Box::new(Expr::Column("price".to_string())),
                op: BinaryOperator::Multiply,
                right: Box::new(Expr::Number(2)),
            },
        ];

        let result = conn.project_columns(&rows, &exprs, &[schema.clone()]);
        assert!(result.is_ok());
        let projected = result.unwrap();
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].len(), 2);
        assert_eq!(projected[0][0], Value::Text("Laptop".to_string()));
        assert_eq!(projected[0][1], Value::Int(2000));
    }

    #[test]
    fn test_project_columns_with_mixed_expressions() {
        let conn: Connection<std::io::Cursor<Vec<u8>>> = Connection::dummy();
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
                op: BinaryOperator::Add,
                right: Box::new(Expr::Number(100)),
            },
            Expr::Number(42),
        ];

        let result = conn.project_columns(&rows, &exprs, &[schema.clone()]);
        assert!(result.is_ok());
        let projected = result.unwrap();
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].len(), 4);
        assert_eq!(projected[0][0], Value::Int(1));
        assert_eq!(projected[0][1], Value::Text("Laptop".to_string()));
        assert_eq!(projected[0][2], Value::Int(1100));
        assert_eq!(projected[0][3], Value::Int(42));
    }
}
