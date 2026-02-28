use super::Parser;
use crate::parser::error::{Result, ParseError};
use crate::parser::lexer::Token;
use crate::parser::ast::{Statement, CreateTableStmt, DropTableStmt, DescribeStmt, ColumnDef, DataType};

pub fn parse_create(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Create)?;
    parser.expect(Token::Table)?;
    
    let table = parser.expect_identifier()?;
    
    parser.expect(Token::LeftParen)?;
    
    let columns = parse_column_defs(parser)?;
    
    parser.expect(Token::RightParen)?;
    
    Ok(Statement::CreateTable(CreateTableStmt {
        table,
        columns,
    }))
}

pub fn parse_drop(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Drop)?;
    parser.expect(Token::Table)?;
    
    let if_exists = if parser.current_token() == &Token::If {
        parser.advance();
        parser.expect(Token::Exists)?;
        true
    } else {
        false
    };
    
    let table = parser.expect_identifier()?;
    
    Ok(Statement::DropTable(DropTableStmt { table, if_exists }))
}

pub fn parse_describe(parser: &mut Parser) -> Result<Statement> {
    if parser.current_token() == &Token::Describe || parser.current_token() == &Token::Desc {
        parser.advance();
    }
    
    let table = parser.expect_identifier()?;
    
    Ok(Statement::Describe(DescribeStmt { table }))
}

fn parse_column_defs(parser: &mut Parser) -> Result<Vec<ColumnDef>> {
    let mut columns = vec![parse_column_def(parser)?];
    
    while parser.current_token() == &Token::Comma {
        parser.advance();
        columns.push(parse_column_def(parser)?);
    }
    
    Ok(columns)
}

fn parse_column_def(parser: &mut Parser) -> Result<ColumnDef> {
    let name = parser.expect_identifier()?;
    let data_type = parse_data_type(parser)?;
    
    Ok(ColumnDef { name, data_type })
}

fn parse_data_type(parser: &mut Parser) -> Result<DataType> {
    match parser.current_token() {
        Token::Int => {
            parser.advance();
            Ok(DataType::Int)
        }
        Token::Text => {
            parser.advance();
            Ok(DataType::Text)
        }
        Token::Varchar => {
            parser.advance();
            if parser.current_token() == &Token::LeftParen {
                parser.advance();
                if let Token::Number(n) = parser.current_token() {
                    let size = *n as u32;
                    parser.advance();
                    parser.expect(Token::RightParen)?;
                    Ok(DataType::Varchar(size))
                } else {
                    Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token())))
                }
            } else {
                Ok(DataType::Varchar(255))
            }
        }
        _ => Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    }
}
