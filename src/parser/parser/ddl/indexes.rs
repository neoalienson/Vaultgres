//! Index DDL parsing
//!
//! Handles CREATE INDEX statements.

use super::super::Parser;
use crate::parser::ast::{CreateIndexStmt, Expr, Statement};
use crate::parser::error::Result;
use crate::parser::lexer::Token;

enum IndexColumn {
    Name(String),
    Expr(Expr),
}

pub fn parse_index_column(parser: &mut Parser) -> Result<IndexColumn> {
    if matches!(parser.current_token(), Token::Identifier(_)) {
        Ok(IndexColumn::Name(parser.expect_identifier()?))
    } else {
        Ok(IndexColumn::Expr(super::super::expr::parse_expr(parser)?))
    }
}

pub fn parse_create_index(parser: &mut Parser, unique: bool) -> Result<Statement> {
    parser.expect(Token::Index)?;
    let name = parser.expect_identifier()?;
    parser.expect(Token::On)?;
    let table = parser.expect_identifier()?;
    parser.expect(Token::LeftParen)?;

    let mut columns = Vec::new();
    let mut expressions = Vec::new();

    loop {
        match parse_index_column(parser)? {
            IndexColumn::Name(col) => columns.push(col),
            IndexColumn::Expr(expr) => expressions.push(expr),
        }
        if parser.current_token() != &Token::Comma {
            break;
        }
        parser.advance();
    }

    parser.expect(Token::RightParen)?;
    let where_clause = if parser.current_token() == &Token::Where {
        parser.advance();
        Some(super::super::expr::parse_expr(parser)?)
    } else {
        None
    };

    Ok(Statement::CreateIndex(CreateIndexStmt {
        name,
        table,
        columns,
        expressions,
        unique,
        where_clause,
    }))
}
