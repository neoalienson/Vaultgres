//! Type DDL parsing
//!
//! Handles CREATE TYPE, ALTER TYPE, and related operations.

use super::super::Parser;
use crate::parser::ast::{
    AlterTypeStmt, AttachPartitionStmt, ColumnDef, CreateTypeStmt, DetachPartitionStmt,
    DropTypeStmt, PartitionBoundSpec, Statement, TypeKind,
};
use crate::parser::error::{ParseError, Result};
use crate::parser::lexer::Token;

pub fn parse_create_type(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Type)?;
    let type_name = parser.expect_identifier()?;
    parser.expect(Token::As)?;

    if parser.current_token() == &Token::Enum {
        parser.advance();
        parser.expect(Token::LeftParen)?;

        let mut labels = Vec::new();
        loop {
            match parser.current_token() {
                Token::String(s) => {
                    labels.push(s.clone());
                    parser.advance();
                }
                _ => {
                    return Err(ParseError::UnexpectedToken(format!(
                        "Expected enum label, got {:?}",
                        parser.current_token()
                    )));
                }
            }
            if parser.current_token() == &Token::Comma {
                parser.advance();
                continue;
            }
            break;
        }

        parser.expect(Token::RightParen)?;

        Ok(Statement::CreateType(CreateTypeStmt { type_name, kind: TypeKind::Enum(labels) }))
    } else if parser.current_token() == &Token::LeftParen {
        parser.advance();

        let mut columns = Vec::new();
        loop {
            if parser.current_token() == &Token::RightParen {
                break;
            }

            let field_name = parser.expect_identifier()?;
            let field_type = super::tables::parse_data_type(parser)?;
            columns.push(ColumnDef::new(field_name, field_type));

            if parser.current_token() == &Token::Comma {
                parser.advance();
            } else if parser.current_token() != &Token::RightParen {
                return Err(ParseError::UnexpectedToken(format!(
                    "Expected ',' or ')', got {:?}",
                    parser.current_token()
                )));
            }
        }

        parser.expect(Token::RightParen)?;

        if columns.is_empty() {
            return Err(ParseError::UnexpectedToken(
                "Composite type must have at least one field".to_string(),
            ));
        }

        let mut seen_names: std::collections::HashSet<&String> = std::collections::HashSet::new();
        for col in &columns {
            if !seen_names.insert(&col.name) {
                return Err(ParseError::UnexpectedToken(
                    "Composite type cannot have duplicate field names".to_string(),
                ));
            }
        }

        Ok(Statement::CreateType(CreateTypeStmt { type_name, kind: TypeKind::Composite(columns) }))
    } else {
        Err(ParseError::UnexpectedToken(format!(
            "Expected ENUM or '(', got {:?}",
            parser.current_token()
        )))
    }
}

pub fn parse_drop_type(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Type)?;
    let if_exists = super::tables::parse_if_exists(parser)?;
    let type_name = parser.expect_identifier()?;
    let cascade = if parser.current_token() == &Token::Cascade {
        parser.advance();
        true
    } else if parser.current_token() == &Token::Restrict {
        parser.advance();
        false
    } else {
        false
    };
    Ok(Statement::DropType(DropTypeStmt { type_name, if_exists, cascade }))
}

pub fn parse_alter_type(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Type)?;
    let type_name = parser.expect_identifier()?;
    parser.expect(Token::Add)?;
    let new_label = match parser.current_token() {
        Token::String(s) => {
            let label = s.clone();
            parser.advance();
            label
        }
        _ => {
            return Err(ParseError::UnexpectedToken(format!(
                "Expected enum label value, got {:?}",
                parser.current_token()
            )));
        }
    };

    let after_label = if parser.current_token() == &Token::After {
        parser.advance();
        Some(parser.expect_identifier()?)
    } else if parser.current_token() == &Token::Before {
        parser.advance();
        Some(parser.expect_identifier()?)
    } else {
        None
    };

    Ok(Statement::AlterType(AlterTypeStmt { type_name, new_label, after_label }))
}

pub fn parse_alter_table(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Table)?;
    let table = parser.expect_identifier()?;

    match parser.current_token() {
        Token::Attach => parse_attach_partition(parser, table),
        Token::Detach => parse_detach_partition(parser, table),
        _ => Err(ParseError::UnexpectedToken(format!(
            "ALTER TABLE not supported for {:?}",
            parser.current_token()
        ))),
    }
}

pub fn parse_attach_partition(parser: &mut Parser, table: String) -> Result<Statement> {
    parser.expect(Token::Attach)?;
    parser.expect(Token::Partition)?;
    let partition_name = parser.expect_identifier()?;

    let bound = if parser.current_token() == &Token::For {
        Some(super::tables::parse_partition_bound(parser)?)
    } else {
        None
    };

    Ok(Statement::AttachPartition(AttachPartitionStmt {
        parent_table: table,
        partition_name,
        bound: bound.unwrap_or(PartitionBoundSpec::Default),
    }))
}

pub fn parse_detach_partition(parser: &mut Parser, table: String) -> Result<Statement> {
    parser.expect(Token::Detach)?;
    parser.expect(Token::Partition)?;
    let partition_name = parser.expect_identifier()?;

    Ok(Statement::DetachPartition(DetachPartitionStmt { parent_table: table, partition_name }))
}

pub fn parse_alter(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Alter)?;

    match parser.current_token() {
        Token::Type => parse_alter_type(parser),
        Token::Table => parse_alter_table(parser),
        _ => Err(ParseError::UnexpectedToken(format!(
            "ALTER not supported for {:?}",
            parser.current_token()
        ))),
    }
}
