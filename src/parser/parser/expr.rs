use super::Parser;
use crate::parser::error::{Result, ParseError};
use crate::parser::lexer::Token;
use crate::parser::ast::{Expr, BinaryOperator};

pub fn parse_expr(parser: &mut Parser) -> Result<Expr> {
    let left = parse_primary(parser)?;
    
    let op = match parser.current_token() {
        Token::Equals => BinaryOperator::Equals,
        Token::NotEquals => BinaryOperator::NotEquals,
        Token::LessThan => BinaryOperator::LessThan,
        Token::LessThanOrEqual => BinaryOperator::LessThanOrEqual,
        Token::GreaterThan => BinaryOperator::GreaterThan,
        Token::GreaterThanOrEqual => BinaryOperator::GreaterThanOrEqual,
        _ => return Ok(left),
    };
    
    parser.advance();
    let right = parse_primary(parser)?;
    Ok(Expr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}

pub fn parse_primary(parser: &mut Parser) -> Result<Expr> {
    match parser.current_token().clone() {
        Token::Identifier(name) => {
            parser.advance();
            Ok(Expr::Column(name))
        }
        Token::Number(n) => {
            parser.advance();
            Ok(Expr::Number(n))
        }
        Token::String(s) => {
            parser.advance();
            Ok(Expr::String(s))
        }
        Token::Star => {
            parser.advance();
            Ok(Expr::Star)
        }
        _ => Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    }
}

pub fn parse_expr_list(parser: &mut Parser) -> Result<Vec<Expr>> {
    let mut exprs = vec![parse_expr(parser)?];
    
    while parser.current_token() == &Token::Comma {
        parser.advance();
        exprs.push(parse_expr(parser)?);
    }
    
    Ok(exprs)
}
