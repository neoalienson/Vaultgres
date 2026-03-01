use super::message::{Message, Response, ProtocolError};
use crate::parser::{Parser, Statement};
use crate::catalog::Catalog;
use std::io::{Read, Write};
use std::sync::Arc;

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
            Ok(mut parser) => {
                match parser.parse() {
                    Ok(stmt) => {
                        log::debug!("Parsed statement: {:?}", stmt);
                        match self.execute_statement(stmt) {
                            Ok(tag) => {
                                Response::CommandComplete { tag }.write(&mut self.stream)?;
                                Response::ReadyForQuery.write(&mut self.stream)?;
                            }
                            Err(e) => {
                                log::warn!("Execution error: {}", e);
                                Response::ErrorResponse { message: format!("Execution error: {}", e) }.write(&mut self.stream)?;
                                Response::ReadyForQuery.write(&mut self.stream)?;
                            }
                        }
                    }
                    Err(e) => {
                        log::warn!("Parse error: {}", e);
                        Response::ErrorResponse { message: format!("Parse error: {}", e) }.write(&mut self.stream)?;
                        Response::ReadyForQuery.write(&mut self.stream)?;
                    }
                }
            }
            Err(e) => {
                log::warn!("Lexer error: {}", e);
                Response::ErrorResponse { message: format!("Lexer error: {}", e) }.write(&mut self.stream)?;
                Response::ReadyForQuery.write(&mut self.stream)?;
            }
        }
        self.stream.flush()?;
        Ok(())
    }
    
    fn execute_statement(&self, stmt: Statement) -> Result<String, String> {
        use crate::parser::ast::Expr;
        
        match stmt {
            Statement::CreateTable(create) => {
                self.catalog.create_table(create.table.clone(), create.columns)?;
                Ok("CREATE TABLE".to_string())
            }
            Statement::DropTable(drop) => {
                self.catalog.drop_table(&drop.table, drop.if_exists)?;
                Ok("DROP TABLE".to_string())
            }
            Statement::CreateView(create) => {
                self.catalog.create_view(create.name.clone(), *create.query)?;
                Ok("CREATE VIEW".to_string())
            }
            Statement::DropView(drop) => {
                self.catalog.drop_view(&drop.name, drop.if_exists)?;
                Ok("DROP VIEW".to_string())
            }
            Statement::CreateMaterializedView(create) => {
                self.catalog.create_materialized_view(create.name.clone(), *create.query)?;
                Ok("CREATE MATERIALIZED VIEW".to_string())
            }
            Statement::RefreshMaterializedView(refresh) => {
                self.catalog.refresh_materialized_view(&refresh.name)?;
                Ok("REFRESH MATERIALIZED VIEW".to_string())
            }
            Statement::DropMaterializedView(drop) => {
                self.catalog.drop_materialized_view(&drop.name, drop.if_exists)?;
                Ok("DROP MATERIALIZED VIEW".to_string())
            }
            Statement::CreateTrigger(create) => {
                self.catalog.create_trigger(create)?;
                Ok("CREATE TRIGGER".to_string())
            }
            Statement::DropTrigger(drop) => {
                self.catalog.drop_trigger(&drop.name, drop.if_exists)?;
                Ok("DROP TRIGGER".to_string())
            }
            Statement::CreateIndex(create) => {
                self.catalog.create_index(create)?;
                Ok("CREATE INDEX".to_string())
            }
            Statement::DropIndex(drop) => {
                self.catalog.drop_index(&drop.name, drop.if_exists)?;
                Ok("DROP INDEX".to_string())
            }
            Statement::Describe(desc) => {
                if let Some(schema) = self.catalog.get_table(&desc.table) {
                    let cols: Vec<String> = schema.columns.iter()
                        .map(|c| format!("{}: {:?}", c.name, c.data_type))
                        .collect();
                    Ok(format!("DESCRIBE\n{}", cols.join("\n")))
                } else {
                    Err(format!("Table '{}' does not exist", desc.table))
                }
            }
            Statement::Insert(insert) => {
                self.catalog.insert(&insert.table, insert.values)?;
                Ok("INSERT 0 1".to_string())
            }
            Statement::Select(select) => {
                let columns: Vec<String> = select.columns.iter()
                    .map(|expr| match expr {
                        Expr::Star => "*".to_string(),
                        Expr::Column(name) => name.clone(),
                        Expr::Aggregate { func, arg } => {
                            let func_name = match func {
                                crate::parser::ast::AggregateFunc::Count => "COUNT",
                                crate::parser::ast::AggregateFunc::Sum => "SUM",
                                crate::parser::ast::AggregateFunc::Avg => "AVG",
                                crate::parser::ast::AggregateFunc::Min => "MIN",
                                crate::parser::ast::AggregateFunc::Max => "MAX",
                            };
                            let col = match **arg {
                                Expr::Star => "*",
                                Expr::Column(ref name) => name.as_str(),
                                _ => "?",
                            };
                            format!("AGG:{}:{}", func_name, col)
                        }
                        _ => "?".to_string(),
                    })
                    .collect();
                
                let rows = self.catalog.select(&select.from, select.distinct, columns, select.where_clause, select.group_by, select.having, select.order_by, select.limit, select.offset)?;
                Ok(format!("SELECT {}", rows.len()))
            }
            Statement::Update(update) => {
                let count = self.catalog.update(&update.table, update.assignments, update.where_clause)?;
                Ok(format!("UPDATE {}", count))
            }
            Statement::Delete(delete) => {
                let count = self.catalog.delete(&delete.table, delete.where_clause)?;
                Ok(format!("DELETE {}", count))
            }
            _ => Ok("SELECT 0".to_string()),
        }
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
