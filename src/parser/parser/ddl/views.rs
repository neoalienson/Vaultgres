//! View DDL parsing
//!
//! Handles CREATE VIEW and CREATE MATERIALIZED VIEW statements.

use super::super::Parser;
use crate::parser::ast::{CreateMaterializedViewStmt, CreateViewStmt, Statement};
use crate::parser::error::Result;

pub fn parse_create_view(parser: &mut Parser) -> Result<Statement> {
    parser.expect(crate::parser::lexer::Token::View)?;

    let name = parser.expect_identifier()?;

    parser.expect(crate::parser::lexer::Token::As)?;

    let query = super::super::select::parse_select_stmt(parser)?;

    Ok(Statement::CreateView(CreateViewStmt { name, query: Box::new(query) }))
}

pub fn parse_create_materialized_view(parser: &mut Parser) -> Result<Statement> {
    parser.expect(crate::parser::lexer::Token::Materialized)?;
    parser.expect(crate::parser::lexer::Token::View)?;

    let name = parser.expect_identifier()?;

    parser.expect(crate::parser::lexer::Token::As)?;

    let query = super::super::select::parse_select_stmt(parser)?;

    Ok(Statement::CreateMaterializedView(CreateMaterializedViewStmt {
        name,
        query: Box::new(query),
    }))
}
