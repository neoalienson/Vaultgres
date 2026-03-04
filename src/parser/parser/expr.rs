use super::Parser;
use crate::parser::ast::{BinaryOperator, Expr};
use crate::parser::error::{ParseError, Result};
use crate::parser::lexer::Token;

pub fn parse_expr(parser: &mut Parser) -> Result<Expr> {
    parse_or(parser)
}

fn parse_or(parser: &mut Parser) -> Result<Expr> {
    let mut left = parse_and(parser)?;

    while parser.current_token() == &Token::Or {
        parser.advance();
        let right = parse_and(parser)?;
        left =
            Expr::BinaryOp { left: Box::new(left), op: BinaryOperator::Or, right: Box::new(right) };
    }

    Ok(left)
}

fn parse_and(parser: &mut Parser) -> Result<Expr> {
    let mut left = parse_not(parser)?;

    while parser.current_token() == &Token::And {
        parser.advance();
        let right = parse_not(parser)?;
        left = Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::And,
            right: Box::new(right),
        };
    }

    Ok(left)
}

fn parse_not(parser: &mut Parser) -> Result<Expr> {
    if parser.current_token() == &Token::Not {
        parser.advance();
        let expr = parse_comparison(parser)?;
        return Ok(Expr::UnaryOp {
            op: crate::parser::ast::UnaryOperator::Not,
            expr: Box::new(expr),
        });
    }
    parse_comparison(parser)
}

fn parse_comparison(parser: &mut Parser) -> Result<Expr> {
    let left = parse_primary(parser)?;

    // Handle IS NULL and IS NOT NULL
    if parser.current_token() == &Token::Is {
        parser.advance();
        let is_not = if parser.current_token() == &Token::Not {
            parser.advance();
            true
        } else {
            false
        };
        parser.expect(Token::Null)?;
        return Ok(if is_not {
            Expr::IsNotNull(Box::new(left))
        } else {
            Expr::IsNull(Box::new(left))
        });
    }

    if parser.current_token() == &Token::In {
        parser.advance();
        parser.expect(Token::LeftParen)?;

        // Check if it's a subquery
        if parser.current_token() == &Token::Select {
            let subquery = crate::parser::parser::select::parse_select_stmt(parser)?;
            parser.expect(Token::RightParen)?;
            return Ok(Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::In,
                right: Box::new(Expr::Subquery(Box::new(subquery))),
            });
        }

        // Otherwise, parse list of values
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
        // Convert BETWEEN to: left >= lower AND left <= upper
        return Ok(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(left.clone()),
                op: BinaryOperator::GreaterThanOrEqual,
                right: Box::new(lower),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::LessThanOrEqual,
                right: Box::new(upper),
            }),
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
    Ok(Expr::BinaryOp { left: Box::new(left), op, right: Box::new(right) })
}

pub fn parse_primary(parser: &mut Parser) -> Result<Expr> {
    match parser.current_token().clone() {
        Token::Case => parse_case(parser),
        Token::Count | Token::Sum | Token::Avg | Token::Min | Token::Max => parse_aggregate(parser),
        Token::RowNumber | Token::Rank | Token::DenseRank | Token::Lag | Token::Lead => {
            parse_window(parser)
        }
        Token::Parameter(n) => {
            parser.advance();
            Ok(Expr::Parameter(n))
        }
        Token::LeftParen => {
            parser.advance();
            if parser.current_token() == &Token::Select {
                let subquery = super::select::parse_select_stmt(parser)?;
                parser.expect(Token::RightParen)?;
                Ok(Expr::Subquery(Box::new(subquery)))
            } else {
                let expr = parse_expr(parser)?;
                parser.expect(Token::RightParen)?;
                Ok(expr)
            }
        }
        Token::Identifier(name) => {
            parser.advance();
            if matches!(parser.current_token(), Token::Dot) {
                parser.advance();
                let column = parser.expect_identifier()?;
                Ok(Expr::QualifiedColumn { table: name, column })
            } else if matches!(parser.current_token(), Token::LeftParen) {
                parser.advance();
                let args = if parser.current_token() != &Token::RightParen {
                    parse_expr_list(parser)?
                } else {
                    vec![]
                };
                parser.expect(Token::RightParen)?;
                Ok(Expr::FunctionCall { name, args })
            } else {
                Ok(Expr::Column(name))
            }
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

fn parse_window(parser: &mut Parser) -> Result<Expr> {
    use crate::parser::ast::{OrderByExpr, WindowFunc};

    let func = match parser.current_token() {
        Token::RowNumber => WindowFunc::RowNumber,
        Token::Rank => WindowFunc::Rank,
        Token::DenseRank => WindowFunc::DenseRank,
        Token::Lag => WindowFunc::Lag,
        Token::Lead => WindowFunc::Lead,
        _ => return Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    };

    parser.advance();
    parser.expect(Token::LeftParen)?;

    let arg = if parser.current_token() == &Token::RightParen {
        Box::new(Expr::Star)
    } else {
        Box::new(parse_expr(parser)?)
    };

    parser.expect(Token::RightParen)?;
    parser.expect(Token::Over)?;
    parser.expect(Token::LeftParen)?;

    let mut partition_by = Vec::new();
    if parser.current_token() == &Token::Partition {
        parser.advance();
        parser.expect(Token::By)?;
        loop {
            partition_by.push(parser.expect_identifier()?);
            if parser.current_token() != &Token::Comma {
                break;
            }
            parser.advance();
        }
    }

    let mut order_by = Vec::new();
    if parser.current_token() == &Token::Order {
        parser.advance();
        parser.expect(Token::By)?;
        loop {
            let column = parser.expect_identifier()?;
            let ascending = match parser.current_token() {
                Token::Descending => {
                    parser.advance();
                    false
                }
                Token::Asc => {
                    parser.advance();
                    true
                }
                _ => true,
            };
            order_by.push(OrderByExpr { column, ascending });
            if parser.current_token() != &Token::Comma {
                break;
            }
            parser.advance();
        }
    }

    parser.expect(Token::RightParen)?;

    Ok(Expr::Window { func, arg, partition_by, order_by })
}

fn parse_case(parser: &mut Parser) -> Result<Expr> {
    parser.expect(Token::Case)?;

    let mut conditions = Vec::new();

    while parser.current_token() == &Token::When {
        parser.advance();
        let condition = parse_expr(parser)?;
        parser.expect(Token::Then)?;
        let result = parse_expr(parser)?;
        conditions.push((condition, result));
    }

    let else_expr = if parser.current_token() == &Token::Else {
        parser.advance();
        Some(Box::new(parse_expr(parser)?))
    } else {
        None
    };

    parser.expect(Token::End)?;

    Ok(Expr::Case { conditions, else_expr })
}

pub fn parse_expr_list(parser: &mut Parser) -> Result<Vec<Expr>> {
    let mut exprs = vec![];

    loop {
        let expr = parse_expr(parser)?;

        let final_expr = if parser.current_token() == &Token::As {
            parser.advance();
            let alias = parser.expect_identifier()?;
            Expr::Alias { expr: Box::new(expr), alias }
        } else {
            expr
        };

        exprs.push(final_expr);

        if parser.current_token() != &Token::Comma {
            break;
        }
        parser.advance();
    }

    Ok(exprs)
}
