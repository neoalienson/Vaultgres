//! Cursor DDL parsing
//!
//! Handles DECLARE CURSOR, FETCH, and CLOSE CURSOR statements.

use super::super::Parser;
use crate::parser::ast::{
    CloseCursorStmt, DeclareCursorStmt, FetchCursorStmt, FetchDirection, Statement,
};
use crate::parser::error::Result;
use crate::parser::lexer::Token;

pub fn parse_declare_cursor(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Declare)?;
    let name = parser.expect_identifier()?;
    parser.expect(Token::Cursor)?;
    parser.expect(Token::For)?;
    let query = super::super::select::parse_select_stmt(parser)?;
    Ok(Statement::DeclareCursor(DeclareCursorStmt { name, query: Box::new(query) }))
}

pub fn parse_fetch_cursor(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Fetch)?;

    let (direction, count) = match parser.current_token() {
        Token::Next => {
            parser.advance();
            (FetchDirection::Next, None)
        }
        Token::Prior => {
            parser.advance();
            (FetchDirection::Prior, None)
        }
        Token::First => {
            parser.advance();
            (FetchDirection::First, None)
        }
        Token::Last => {
            parser.advance();
            (FetchDirection::Last, None)
        }
        Token::Absolute => {
            parser.advance();
            let count = if let Token::Number(n) = parser.current_token() {
                let num = *n;
                parser.advance();
                Some(num)
            } else {
                None
            };
            (FetchDirection::Absolute, count)
        }
        Token::Relative => {
            parser.advance();
            let count = if let Token::Number(n) = parser.current_token() {
                let num = *n;
                parser.advance();
                Some(num)
            } else {
                None
            };
            (FetchDirection::Relative, count)
        }
        Token::Forward => {
            parser.advance();
            (FetchDirection::Forward, None)
        }
        Token::Backward => {
            parser.advance();
            (FetchDirection::Backward, None)
        }
        _ => (FetchDirection::Next, None),
    };

    parser.expect(Token::From)?;
    let name = parser.expect_identifier()?;

    Ok(Statement::FetchCursor(FetchCursorStmt { name, direction, count }))
}

pub fn parse_close_cursor(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Close)?;
    let name = parser.expect_identifier()?;
    Ok(Statement::CloseCursor(CloseCursorStmt { name }))
}
