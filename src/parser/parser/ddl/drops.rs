//! DROP statement parsing
//!
//! Handles DROP statements for tables, views, materialized views, triggers, indexes, types,
//! functions, and aggregates.

use super::super::Parser;
use crate::parser::ast::{
    DropAggregateStmt, DropIndexStmt, DropMaterializedViewStmt, DropTriggerStmt, DropViewStmt,
    Statement,
};
use crate::parser::error::Result;
use crate::parser::lexer::Token;

pub fn parse_drop(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Drop)?;

    match parser.current_token() {
        Token::Table => super::tables::parse_drop_table(parser),
        Token::View => parse_drop_view(parser),
        Token::Materialized => parse_drop_materialized_view(parser),
        Token::Trigger => parse_drop_trigger(parser),
        Token::Index => parse_drop_index(parser),
        Token::Type => super::types::parse_drop_type(parser),
        Token::Function | Token::Procedure => super::functions::parse_drop_function(parser),
        Token::Aggregate => super::functions::parse_drop_aggregate(parser),
        _ => Err(crate::parser::error::ParseError::UnexpectedToken(format!(
            "{:?}",
            parser.current_token()
        ))),
    }
}

pub fn parse_drop_view(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::View)?;
    let if_exists = super::tables::parse_if_exists(parser)?;
    let name = parser.expect_identifier()?;
    Ok(Statement::DropView(DropViewStmt { name, if_exists }))
}

pub fn parse_drop_materialized_view(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Materialized)?;
    parser.expect(Token::View)?;
    let if_exists = super::tables::parse_if_exists(parser)?;
    let name = parser.expect_identifier()?;
    Ok(Statement::DropMaterializedView(DropMaterializedViewStmt { name, if_exists }))
}

pub fn parse_drop_trigger(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Trigger)?;
    let if_exists = super::tables::parse_if_exists(parser)?;
    let name = parser.expect_identifier()?;
    Ok(Statement::DropTrigger(DropTriggerStmt { name, if_exists }))
}

pub fn parse_drop_index(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Index)?;
    let if_exists = super::tables::parse_if_exists(parser)?;
    let name = parser.expect_identifier()?;
    Ok(Statement::DropIndex(DropIndexStmt { name, if_exists }))
}
