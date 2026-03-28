//! Trigger DDL parsing
//!
//! Handles CREATE TRIGGER statements.

use super::super::Parser;
use crate::parser::ast::{CreateTriggerStmt, Statement, TriggerEvent, TriggerFor, TriggerTiming};
use crate::parser::error::{ParseError, Result};
use crate::parser::lexer::Token;

pub fn parse_trigger_timing(parser: &mut Parser) -> Result<TriggerTiming> {
    let timing = match parser.current_token() {
        Token::Before => TriggerTiming::Before,
        Token::After => TriggerTiming::After,
        _ => return Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    };
    parser.advance();
    Ok(timing)
}

pub fn parse_trigger_event(parser: &mut Parser) -> Result<TriggerEvent> {
    let event = match parser.current_token() {
        Token::Insert => TriggerEvent::Insert,
        Token::Update => TriggerEvent::Update,
        Token::Delete => TriggerEvent::Delete,
        _ => return Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    };
    parser.advance();
    Ok(event)
}

pub fn parse_trigger_for(parser: &mut Parser) -> Result<TriggerFor> {
    let for_each = match parser.current_token() {
        Token::Row => TriggerFor::EachRow,
        Token::Statement => TriggerFor::EachStatement,
        _ => return Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    };
    parser.advance();
    Ok(for_each)
}

pub fn parse_create_trigger(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Trigger)?;
    let name = parser.expect_identifier()?;
    let timing = parse_trigger_timing(parser)?;
    let event = parse_trigger_event(parser)?;
    parser.expect(Token::On)?;
    let table = parser.expect_identifier()?;
    parser.expect(Token::For)?;
    parser.expect(Token::Each)?;
    let for_each = parse_trigger_for(parser)?;

    let when = if parser.current_token() == &Token::When {
        parser.advance();
        parser.expect(Token::LeftParen)?;
        let expr = super::super::expr::parse_expr(parser)?;
        parser.expect(Token::RightParen)?;
        Some(expr)
    } else {
        None
    };

    parser.expect(Token::Begin)?;
    let mut body = Vec::new();
    while parser.current_token() != &Token::End {
        body.push(parser.parse()?);
        if parser.current_token() == &Token::Semicolon {
            parser.advance();
        }
    }
    parser.expect(Token::End)?;

    Ok(Statement::CreateTrigger(CreateTriggerStmt {
        name,
        timing,
        event,
        table,
        for_each,
        when,
        body,
    }))
}
