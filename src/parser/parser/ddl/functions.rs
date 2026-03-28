//! Function and aggregate DDL parsing
//!
//! Handles CREATE FUNCTION, CREATE AGGREGATE, and their DROP counterparts.

use super::super::Parser;
use crate::parser::ast::{
    CreateAggregateStmt, CreateFunctionStmt, DropAggregateStmt, DropFunctionStmt,
    FunctionParameter, FunctionReturnType, FunctionVolatility, ParameterMode, Statement,
};
use crate::parser::error::{ParseError, Result};
use crate::parser::lexer::Token;

pub fn parse_type_name(parser: &mut Parser) -> Result<String> {
    let type_name = match parser.current_token() {
        Token::Int => "INT",
        Token::Text => "TEXT",
        Token::Avg => "FLOAT",
        Token::Sql => "SQL",
        Token::PlPgSql => "PLPGSQL",
        Token::Identifier(s) => {
            let name = s.clone();
            parser.advance();
            return Ok(name);
        }
        _ => return Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    };
    parser.advance();
    Ok(type_name.to_string())
}

pub fn parse_param_mode(parser: &mut Parser) -> ParameterMode {
    let mode = match parser.current_token() {
        Token::Out => ParameterMode::Out,
        Token::Inout => ParameterMode::InOut,
        Token::Variadic => ParameterMode::Variadic,
        _ => return ParameterMode::In,
    };
    parser.advance();
    mode
}

pub fn parse_default_value(parser: &mut Parser) -> Option<String> {
    if parser.current_token() != &Token::Equals {
        return None;
    }
    parser.advance();
    let val = match parser.current_token() {
        Token::String(s) => format!("'{}'", s),
        Token::Number(n) => n.to_string(),
        Token::Identifier(s) => s.clone(),
        _ => return None,
    };
    parser.advance();
    Some(val)
}

pub fn parse_function_parameter(parser: &mut Parser) -> Result<FunctionParameter> {
    let mode = parse_param_mode(parser);
    let name = parser.expect_identifier()?;
    let data_type = parse_type_name(parser)?;
    let default = parse_default_value(parser);
    Ok(FunctionParameter { name, data_type, mode, default })
}

pub fn parse_table_columns(parser: &mut Parser) -> Result<Vec<(String, String)>> {
    let mut cols = Vec::new();
    loop {
        let col_name = parser.expect_identifier()?;
        let col_type = parse_type_name(parser)?;
        cols.push((col_name, col_type));
        if parser.current_token() != &Token::Comma {
            break;
        }
        parser.advance();
    }
    Ok(cols)
}

pub fn parse_return_type(parser: &mut Parser) -> Result<FunctionReturnType> {
    match parser.current_token() {
        Token::Table => {
            parser.advance();
            parser.expect(Token::LeftParen)?;
            let cols = parse_table_columns(parser)?;
            parser.expect(Token::RightParen)?;
            Ok(FunctionReturnType::Table(cols))
        }
        Token::Setof => {
            parser.advance();
            Ok(FunctionReturnType::Setof(parse_type_name(parser)?))
        }
        _ => Ok(FunctionReturnType::Type(parse_type_name(parser)?)),
    }
}

pub fn parse_create_function(parser: &mut Parser) -> Result<Statement> {
    parser.advance();
    let name = parser.expect_identifier()?;
    parser.expect(Token::LeftParen)?;

    let mut parameters = Vec::new();
    if parser.current_token() != &Token::RightParen {
        loop {
            parameters.push(parse_function_parameter(parser)?);
            if parser.current_token() != &Token::Comma {
                break;
            }
            parser.advance();
        }
    }
    parser.expect(Token::RightParen)?;

    parser.expect(Token::Returns)?;
    let return_type = parse_return_type(parser)?;
    parser.expect(Token::Language)?;
    let language = parse_type_name(parser)?;

    let volatility = match parser.current_token() {
        Token::Immutable => {
            parser.advance();
            Some(FunctionVolatility::Immutable)
        }
        Token::Stable => {
            parser.advance();
            Some(FunctionVolatility::Stable)
        }
        Token::Volatile => {
            parser.advance();
            Some(FunctionVolatility::Volatile)
        }
        _ => None,
    };

    let cost = if parser.current_token() == &Token::Cost {
        parser.advance();
        if let Token::Number(n) = parser.current_token() {
            let c = *n as f64;
            parser.advance();
            Some(c)
        } else {
            None
        }
    } else {
        None
    };

    let rows = if parser.current_token() == &Token::Rows {
        parser.advance();
        if let Token::Number(n) = parser.current_token() {
            let r = *n as u64;
            parser.advance();
            Some(r)
        } else {
            None
        }
    } else {
        None
    };

    parser.expect(Token::As)?;
    let body = if let Token::String(s) = parser.current_token().clone() {
        parser.advance();
        s
    } else {
        return Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token())));
    };

    Ok(Statement::CreateFunction(CreateFunctionStmt {
        name,
        parameters,
        return_type,
        language,
        body,
        volatility,
        cost,
        rows,
    }))
}

pub fn parse_drop_function(parser: &mut Parser) -> Result<Statement> {
    parser.advance();
    let if_exists = super::tables::parse_if_exists(parser)?;
    let name = parser.expect_identifier()?;
    Ok(Statement::DropFunction(DropFunctionStmt { name, if_exists }))
}

pub fn parse_create_aggregate(parser: &mut Parser) -> Result<Statement> {
    parser.advance();
    let name = parser.expect_identifier()?;
    parser.expect(Token::LeftParen)?;
    let input_type = parse_type_name(parser)?;
    parser.expect(Token::RightParen)?;
    parser.expect(Token::LeftParen)?;

    let mut sfunc = None;
    let mut stype = None;
    let mut finalfunc = None;
    let mut initcond = None;

    while parser.current_token() != &Token::RightParen {
        match parser.current_token() {
            Token::SFunc => {
                parser.advance();
                parser.expect(Token::Equals)?;
                sfunc = Some(parser.expect_identifier()?);
            }
            Token::SType => {
                parser.advance();
                parser.expect(Token::Equals)?;
                stype = Some(parse_type_name(parser)?);
            }
            Token::FinalFunc => {
                parser.advance();
                parser.expect(Token::Equals)?;
                finalfunc = Some(parser.expect_identifier()?);
            }
            Token::InitCond => {
                parser.advance();
                parser.expect(Token::Equals)?;
                initcond = Some(match parser.current_token() {
                    Token::String(s) => {
                        let val = s.clone();
                        parser.advance();
                        val
                    }
                    Token::Number(n) => {
                        let val = n.to_string();
                        parser.advance();
                        val
                    }
                    _ => {
                        return Err(ParseError::UnexpectedToken(format!(
                            "Expected string or number for INITCOND, got {:?}",
                            parser.current_token()
                        )));
                    }
                });
            }
            Token::Cost => {
                parser.advance();
                parser.expect(Token::Equals)?;
                if let Token::Number(n) = parser.current_token() {
                    parser.advance();
                }
            }
            Token::Rows => {
                parser.advance();
                parser.expect(Token::Equals)?;
                if let Token::Number(n) = parser.current_token() {
                    parser.advance();
                }
            }
            Token::Identifier(_) => {
                parser.advance();
            }
            _ => {
                return Err(ParseError::UnexpectedToken(format!(
                    "Unexpected token in aggregate definition: {:?}",
                    parser.current_token()
                )));
            }
        }
        if parser.current_token() == &Token::Comma {
            parser.advance();
        }
    }

    parser.expect(Token::RightParen)?;

    let sfunc = sfunc.ok_or_else(|| {
        ParseError::UnexpectedToken("SFUNC is required for CREATE AGGREGATE".to_string())
    })?;
    let stype = stype.ok_or_else(|| {
        ParseError::UnexpectedToken("STYPE is required for CREATE AGGREGATE".to_string())
    })?;

    let volatility = match parser.current_token() {
        Token::Immutable => {
            parser.advance();
            Some(FunctionVolatility::Immutable)
        }
        Token::Stable => {
            parser.advance();
            Some(FunctionVolatility::Stable)
        }
        Token::Volatile => {
            parser.advance();
            Some(FunctionVolatility::Volatile)
        }
        _ => None,
    };

    let cost = if parser.current_token() == &Token::Cost {
        parser.advance();
        if let Token::Number(n) = parser.current_token() {
            let c = *n as f64;
            parser.advance();
            Some(c)
        } else {
            None
        }
    } else {
        None
    };

    Ok(Statement::CreateAggregate(CreateAggregateStmt {
        name,
        input_type,
        sfunc,
        stype,
        finalfunc,
        initcond,
        volatility,
        cost,
    }))
}

pub fn parse_drop_aggregate(parser: &mut Parser) -> Result<Statement> {
    parser.advance();
    let if_exists = super::tables::parse_if_exists(parser)?;
    let name = parser.expect_identifier()?;
    Ok(Statement::DropAggregate(DropAggregateStmt { name, if_exists }))
}
