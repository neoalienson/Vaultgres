use super::Parser;
use crate::parser::error::{Result, ParseError};
use crate::parser::lexer::Token;
use crate::parser::ast::{Expr, BinaryOperator};

pub fn parse_expr(parser: &mut Parser) -> Result<Expr> {
    parse_or(parser)
}

fn parse_or(parser: &mut Parser) -> Result<Expr> {
    let mut left = parse_and(parser)?;
    
    while parser.current_token() == &Token::Or {
        parser.advance();
        let right = parse_and(parser)?;
        left = Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Or,
            right: Box::new(right),
        };
    }
    
    Ok(left)
}

fn parse_and(parser: &mut Parser) -> Result<Expr> {
    let mut left = parse_comparison(parser)?;
    
    while parser.current_token() == &Token::And {
        parser.advance();
        let right = parse_comparison(parser)?;
        left = Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::And,
            right: Box::new(right),
        };
    }
    
    Ok(left)
}

fn parse_comparison(parser: &mut Parser) -> Result<Expr> {
    let left = parse_primary(parser)?;
    
    if parser.current_token() == &Token::In {
        parser.advance();
        parser.expect(Token::LeftParen)?;
        let mut values = vec![parse_primary(parser)?];
        while parser.current_token() == &Token::Comma {
            parser.advance();
            values.push(parse_primary(parser)?);
        }
        parser.expect(Token::RightParen)?;
        return Ok(Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::In,
            right: Box::new(Expr::List(values)),
        });
    }
    
    if parser.current_token() == &Token::Between {
        parser.advance();
        let lower = parse_primary(parser)?;
        parser.expect(Token::And)?;
        let upper = parse_primary(parser)?;
        return Ok(Expr::BinaryOp {
            left: Box::new(left.clone()),
            op: BinaryOperator::Between,
            right: Box::new(Expr::List(vec![lower, upper])),
        });
    }
    
    let op = match parser.current_token() {
        Token::Equals => BinaryOperator::Equals,
        Token::NotEquals => BinaryOperator::NotEquals,
        Token::LessThan => BinaryOperator::LessThan,
        Token::LessThanOrEqual => BinaryOperator::LessThanOrEqual,
        Token::GreaterThan => BinaryOperator::GreaterThan,
        Token::GreaterThanOrEqual => BinaryOperator::GreaterThanOrEqual,
        Token::Like => BinaryOperator::Like,
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
        Token::Count | Token::Sum | Token::Avg | Token::Min | Token::Max => {
            parse_aggregate(parser)
        }
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

fn parse_aggregate(parser: &mut Parser) -> Result<Expr> {
    use crate::parser::ast::AggregateFunc;
    
    let func = match parser.current_token() {
        Token::Count => AggregateFunc::Count,
        Token::Sum => AggregateFunc::Sum,
        Token::Avg => AggregateFunc::Avg,
        Token::Min => AggregateFunc::Min,
        Token::Max => AggregateFunc::Max,
        _ => return Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    };
    
    parser.advance();
    parser.expect(Token::LeftParen)?;
    
    let arg = if parser.current_token() == &Token::Star {
        parser.advance();
        Box::new(Expr::Star)
    } else {
        Box::new(parse_expr(parser)?)
    };
    
    parser.expect(Token::RightParen)?;
    
    Ok(Expr::Aggregate { func, arg })
}

pub fn parse_expr_list(parser: &mut Parser) -> Result<Vec<Expr>> {
    let mut exprs = vec![parse_expr(parser)?];
    
    while parser.current_token() == &Token::Comma {
        parser.advance();
        exprs.push(parse_expr(parser)?);
    }
    
    Ok(exprs)
}
