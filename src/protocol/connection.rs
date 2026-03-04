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
                (&*self.catalog).create_table_with_constraints(
                    create.table.clone(),
                    create.columns,
                    create.primary_key,
                    create.foreign_keys,
                )?;
                Ok(ExecutionResult::CommandComplete("CREATE TABLE".to_string()))
            }
            Statement::DropTable(drop) => {
                (&*self.catalog).drop_table(&drop.table, drop.if_exists)?;
                Ok(ExecutionResult::CommandComplete("DROP TABLE".to_string()))
            }
            Statement::CreateView(create) => {
                (&*self.catalog).create_view(create.name.clone(), *create.query)?;
                Ok(ExecutionResult::CommandComplete("CREATE VIEW".to_string()))
            }
            Statement::DropView(drop) => {
                (&*self.catalog).drop_view(&drop.name, drop.if_exists)?;
                Ok(ExecutionResult::CommandComplete("DROP VIEW".to_string()))
            }
            Statement::CreateMaterializedView(create) => {
                (&*self.catalog).create_materialized_view(create.name.clone(), *create.query)?;
                Ok(ExecutionResult::CommandComplete("CREATE MATERIALIZED VIEW".to_string()))
            }
            Statement::RefreshMaterializedView(refresh) => {
                (&*self.catalog).refresh_materialized_view(&refresh.name)?;
                Ok(ExecutionResult::CommandComplete("REFRESH MATERIALIZED VIEW".to_string()))
            }
            Statement::DropMaterializedView(drop) => {
                (&*self.catalog).drop_materialized_view(&drop.name, drop.if_exists)?;
                Ok(ExecutionResult::CommandComplete("DROP MATERIALIZED VIEW".to_string()))
            }
            Statement::CreateTrigger(create) => {
                (&*self.catalog).create_trigger(create)?;
                Ok(ExecutionResult::CommandComplete("CREATE TRIGGER".to_string()))
            }
            Statement::DropTrigger(drop) => {
                (&*self.catalog).drop_trigger(&drop.name, drop.if_exists)?;
                Ok(ExecutionResult::CommandComplete("DROP TRIGGER".to_string()))
            }
            Statement::CreateIndex(create) => {
                (&*self.catalog).create_index(create)?;
                Ok(ExecutionResult::CommandComplete("CREATE INDEX".to_string()))
            }
            Statement::DropIndex(drop) => {
                (&*self.catalog).drop_index(&drop.name, drop.if_exists)?;
                Ok(ExecutionResult::CommandComplete("DROP INDEX".to_string()))
            }
            Statement::Describe(desc) => {
                if let Some(schema) = (&*self.catalog).get_table(&desc.table) {
                    let cols: Vec<String> = schema
                        .columns
                        .iter()
                        .map(|c| format!("{}: {:?}", c.name, c.data_type))
                        .collect();
                    Ok(ExecutionResult::CommandComplete(format!("DESCRIBE\n{}", cols.join("\n"))))
                } else {
                    Err(format!("Table '{}' does not exist", desc.table))
                }
            }
            Statement::Insert(insert) => {
                (&*self.catalog).insert(&insert.table, insert.values)?;
                Ok(ExecutionResult::CommandComplete("INSERT 0 1".to_string()))
            }
            Statement::Select(select_stmt) => {
                // Renamed 'select' to 'select_stmt' to avoid shadowing
                log::debug!(
                    "Executing SELECT: from={}, joins={}, has_where={}",
                    select_stmt.from,
                    select_stmt.joins.len(),
                    select_stmt.where_clause.is_some()
                );
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

                let column_names = output_column_names.unwrap_or_else(Vec::new);

                log::trace!("planner execution returned {} rows", rows.len());

                // Build result set
                let result_set = self.build_result_set(&column_names, rows)?;
                Ok(ExecutionResult::ResultSet(result_set))
            }
            Statement::Update(update) => {
                let count = (&*self.catalog).update(
                    &update.table,
                    update.assignments,
                    update.where_clause,
                )?;
                Ok(ExecutionResult::CommandComplete(format!("UPDATE {}", count)))
            }
            Statement::Delete(delete) => {
                let count = (&*self.catalog).delete(&delete.table, delete.where_clause)?;
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
        let left_schema = (&*self.catalog)
            .get_table(left_table)
            .ok_or_else(|| format!("Table '{}' not found", left_table))?;
        let left_alias = select.table_alias.as_ref().unwrap_or(left_table);

        let mut all_schemas = vec![(left_alias.clone(), left_schema.clone())];
        for join in &select.joins {
            let schema = (&*self.catalog)
                .get_table(&join.table)
                .ok_or_else(|| format!("Table '{}' not found", join.table))?;
            let alias = join.alias.as_ref().unwrap_or(&join.table);
            all_schemas.push((alias.clone(), schema));
        }

        log::info!(
            "[JOIN] Schema map: {:?}",
            all_schemas
                .iter()
                .map(|(a, s)| (a.clone(), s.name.clone()))
                .collect::<Vec<(String, String)>>() // Explicit type annotation
        );

        let snapshot = (&*self.catalog).txn_mgr.get_snapshot();
        let data = (&*self.catalog).data.read().unwrap();
        let left_tuples =
            data.get(left_table).ok_or_else(|| format!("Table '{}' has no data", left_table))?;

        let mut results = Vec::new();
        for left_tuple in left_tuples {
            if !left_tuple.header.is_visible(&snapshot, &(&*self.catalog).txn_mgr) {
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
                    if !right_tuple.header.is_visible(&snapshot, &(&*self.catalog).txn_mgr) {
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
        if exprs.is_empty() || (exprs.len() == 1 && matches!(exprs[0], Expr::Star)) {
            return Ok(rows.to_vec());
        }

        let mut result = Vec::new();
        for row in rows {
            let mut projected = Vec::new();
            for expr in exprs {
                match expr {
                    Expr::QualifiedColumn { table, column } => {
                        let mut offset = 0;
                        let mut found = false;
                        for (tbl_alias, schema) in schemas {
                            log::info!(
                                "[PROJ] Checking alias '{}' for '{}.{}', schema has {} cols",
                                tbl_alias,
                                table,
                                column,
                                schema.columns.len()
                            );
                            if tbl_alias == table {
                                log::info!(
                                    "[PROJ] Alias matched! Looking for column '{}' in {:?}",
                                    column,
                                    schema.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
                                );
                                if let Some(idx) =
                                    schema.columns.iter().position(|c| &c.name == column)
                                {
                                    log::info!("[PROJ] Found at offset {} + idx {}", offset, idx);
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
                        let mut offset = 0;
                        let mut found = false;
                        for (_, schema) in schemas {
                            if let Some(idx) = schema.columns.iter().position(|c| &c.name == name) {
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
                    _ => return Err("Unsupported expression in SELECT".to_string()),
                }
            }
            result.push(projected);
        }
        Ok(result)
    }

    pub fn run(&mut self) -> Result<(), ProtocolError> {
        // Handle SSL negotiation request
        let mut ssl_buf = [0u8; 8];
        if self.stream.read_exact(&mut ssl_buf).is_ok() {
            // Check for SSL request (length=8, code=80877103)
            let len = i32::from_be_bytes([ssl_buf[0], ssl_buf[1], ssl_buf[2], ssl_buf[3]]);
            let code = i32::from_be_bytes([ssl_buf[4], ssl_buf[5], ssl_buf[6], ssl_buf[7]]);

            if len == 8 && code == 80877103 {
                // Reject SSL with 'N'
                log::debug!("SSL negotiation rejected");
                self.stream.write_all(b"N")?;
                self.stream.flush()?;
            } else {
                // Not SSL request, this is startup message
                // Read remaining startup data
                let mut data = vec![0u8; (len - 8) as usize];
                self.stream.read_exact(&mut data)?;

                let mut full_data = ssl_buf[4..8].to_vec();
                full_data.extend_from_slice(&data);

                let msg = Message::parse(0, &full_data)?;
                log::debug!("Startup message: {:?}", msg);
                self.authenticated = true;

                Response::AuthenticationOk.write(&mut self.stream)?;
                Response::ReadyForQuery.write(&mut self.stream)?;
                self.stream.flush()?;
            }
        }

        // If SSL was rejected, now handle actual startup
        if !self.authenticated {
            self.handle_startup()?;
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
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_connection_creation() {
        use crate::catalog::Catalog;
        use std::sync::Arc;
        let stream = Cursor::new(Vec::new());
        let catalog = Arc::new(Catalog::new());
        let conn = Connection::new(stream, catalog);
        assert!(!conn.authenticated);
    }
}
