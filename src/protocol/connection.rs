use super::message::{Message, Response, ProtocolError};
use crate::parser::Parser;
use crate::executor::Executor;
use std::io::{Read, Write};

pub struct Connection<S: Read + Write> {
    stream: S,
    authenticated: bool,
}

impl<S: Read + Write> Connection<S> {
    pub fn new(stream: S) -> Self {
        Self { stream, authenticated: false }
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
                        Response::CommandComplete { tag: "SELECT 0".to_string() }.write(&mut self.stream)?;
                        Response::ReadyForQuery.write(&mut self.stream)?;
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
        let stream = Cursor::new(Vec::new());
        let conn = Connection::new(stream);
        assert!(!conn.authenticated);
    }
}
