use super::Parser;
use crate::parser::ast::{DeleteStmt, Expr, InsertStmt, Statement, UpdateStmt};
use crate::parser::error::Result;
use crate::parser::lexer::Token;

pub fn parse_insert(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Insert)?;
    parser.expect(Token::Into)?;

    let table = parser.expect_identifier()?;

    let columns = if parser.current_token() == &Token::LeftParen {
        parser.advance();
        let cols = super::expr::parse_ident_list(parser)?;
        parser.expect(Token::RightParen)?;
        cols
    } else {
        Vec::new()
    };

    parser.expect(Token::Values)?;
    parser.expect(Token::LeftParen)?;

    let values = super::expr::parse_expr_list(parser)?;

    parser.expect(Token::RightParen)?;

    let mut batch_values = Vec::new();
    while parser.current_token() == &Token::Comma {
        parser.advance();
        parser.expect(Token::LeftParen)?;
        batch_values.push(super::expr::parse_expr_list(parser)?);
        parser.expect(Token::RightParen)?;
    }

    Ok(Statement::Insert(InsertStmt { table, columns, values, batch_values }))
}

pub fn parse_update(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Update)?;

    let table = parser.expect_identifier()?;

    parser.expect(Token::Set)?;

    let assignments = parse_assignments(parser)?;

    let where_clause = if parser.current_token() == &Token::Where {
        parser.advance();
        Some(super::expr::parse_expr(parser)?)
    } else {
        None
    };

    Ok(Statement::Update(UpdateStmt { table, assignments, where_clause }))
}

pub fn parse_delete(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Delete)?;
    parser.expect(Token::From)?;

    let table = parser.expect_identifier()?;

    let where_clause = if parser.current_token() == &Token::Where {
        parser.advance();
        Some(super::expr::parse_expr(parser)?)
    } else {
        None
    };

    Ok(Statement::Delete(DeleteStmt { table, where_clause }))
}

fn parse_assignments(parser: &mut Parser) -> Result<Vec<(String, Expr)>> {
    let mut assignments = Vec::new();

    loop {
        let column = parser.expect_identifier()?;
        parser.expect(Token::Equals)?;
        let value = super::expr::parse_expr(parser)?;

        assignments.push((column, value));

        if parser.current_token() != &Token::Comma {
            break;
        }
        parser.advance();
    }

    Ok(assignments)
}
