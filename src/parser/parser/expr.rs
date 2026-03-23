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

/// Parse JSON operators: ->, ->>, #>, #>>, ?, ?|, ?&
/// Lower precedence than additive, higher than comparison
fn parse_json_operators(parser: &mut Parser) -> Result<Expr> {
    let left = parse_additive(parser)?;

    loop {
        let op = match parser.current_token() {
            Token::Arrow => BinaryOperator::JsonExtract,
            Token::DoubleArrow => BinaryOperator::JsonExtractText,
            Token::HashArrow => BinaryOperator::JsonPath,
            Token::HashDoubleArrow => BinaryOperator::JsonPathText,
            Token::Question => BinaryOperator::JsonExists,
            Token::QuestionBar => BinaryOperator::JsonExistsAny,
            Token::QuestionAmpersand => BinaryOperator::JsonExistsAll,
            _ => break,
        };

        parser.advance();
        let right = parse_additive(parser)?;
        return Ok(Expr::BinaryOp { left: Box::new(left), op, right: Box::new(right) });
    }

    Ok(left)
}

fn parse_comparison(parser: &mut Parser) -> Result<Expr> {
    let left = parse_json_operators(parser)?;

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
        let lower = parse_additive(parser)?;
        parser.expect(Token::And)?;
        let upper = parse_additive(parser)?;
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
    let right = parse_additive(parser)?;
    Ok(Expr::BinaryOp { left: Box::new(left), op, right: Box::new(right) })
}

/// Parse additive expressions: + and -
/// Lower precedence than comparison, higher than multiplicative
fn parse_additive(parser: &mut Parser) -> Result<Expr> {
    let mut left = parse_multiplicative(parser)?;

    loop {
        let op = match parser.current_token() {
            Token::Plus => BinaryOperator::Add,
            Token::Minus => BinaryOperator::Subtract,
            _ => break,
        };
        parser.advance();
        let right = parse_multiplicative(parser)?;
        left = Expr::BinaryOp { left: Box::new(left), op, right: Box::new(right) };
    }

    Ok(left)
}

/// Parse multiplicative expressions: *, / and %
/// Higher precedence than additive
fn parse_multiplicative(parser: &mut Parser) -> Result<Expr> {
    let mut left = parse_unary(parser)?;

    loop {
        let op = match parser.current_token() {
            Token::Star => BinaryOperator::Multiply,
            Token::Slash => BinaryOperator::Divide,
            Token::Percent => BinaryOperator::Modulo,
            _ => break,
        };
        parser.advance();
        let right = parse_unary(parser)?;
        left = Expr::BinaryOp { left: Box::new(left), op, right: Box::new(right) };
    }

    Ok(left)
}

/// Parse unary expressions: unary minus
/// Higher precedence than multiplicative
fn parse_unary(parser: &mut Parser) -> Result<Expr> {
    if parser.current_token() == &Token::Minus {
        parser.advance();
        let expr = parse_unary(parser)?;
        return Ok(Expr::UnaryOp {
            op: crate::parser::ast::UnaryOperator::Minus,
            expr: Box::new(expr),
        });
    }
    parse_primary(parser)
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
        Token::Null => {
            parser.advance();
            Ok(Expr::Null)
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

pub fn parse_ident_list(parser: &mut Parser) -> Result<Vec<String>> {
    let mut idents = vec![];

    loop {
        let ident = parser.expect_identifier()?;
        idents.push(ident);

        if parser.current_token() != &Token::Comma {
            break;
        }
        parser.advance();
    }

    Ok(idents)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{BinaryOperator, Expr, UnaryOperator};

    fn parse(sql: &str) -> Result<Expr> {
        let mut parser = Parser::new(sql)?;
        parse_expr(&mut parser)
    }

    #[test]
    fn test_parse_addition() {
        let expr = parse("1 + 2").unwrap();
        assert!(matches!(expr, Expr::BinaryOp { op: BinaryOperator::Add, .. }));
    }

    #[test]
    fn test_parse_subtraction() {
        let expr = parse("10 - 5").unwrap();
        assert!(matches!(expr, Expr::BinaryOp { op: BinaryOperator::Subtract, .. }));
    }

    #[test]
    fn test_parse_multiplication() {
        let expr = parse("3 * 4").unwrap();
        assert!(matches!(expr, Expr::BinaryOp { op: BinaryOperator::Multiply, .. }));
    }

    #[test]
    fn test_parse_division() {
        let expr = parse("100 / 5").unwrap();
        assert!(matches!(expr, Expr::BinaryOp { op: BinaryOperator::Divide, .. }));
    }

    #[test]
    fn test_parse_modulo() {
        let expr = parse("10 % 3").unwrap();
        assert!(matches!(expr, Expr::BinaryOp { op: BinaryOperator::Modulo, .. }));
    }

    #[test]
    fn test_parse_column_arithmetic() {
        let expr = parse("price * 2").unwrap();
        match expr {
            Expr::BinaryOp { op: BinaryOperator::Multiply, left, right } => {
                assert!(matches!(*left, Expr::Column(ref name) if name == "price"));
                assert!(matches!(*right, Expr::Number(2)));
            }
            _ => panic!("Expected BinaryOp::Multiply"),
        }
    }

    #[test]
    fn test_parse_complex_arithmetic() {
        let expr = parse("price + 100 * 2").unwrap();
        // Multiplication has higher precedence: price + (100 * 2)
        match expr {
            Expr::BinaryOp { op: BinaryOperator::Add, right, .. } => {
                assert!(matches!(*right, Expr::BinaryOp { op: BinaryOperator::Multiply, .. }));
            }
            _ => panic!("Expected addition with multiplication on right"),
        }
    }

    #[test]
    fn test_parse_parentheses_override_precedence() {
        let expr = parse("(price + 100) * 2").unwrap();
        // Parentheses override: (price + 100) * 2
        match expr {
            Expr::BinaryOp { op: BinaryOperator::Multiply, left, right } => {
                assert!(matches!(*left, Expr::BinaryOp { op: BinaryOperator::Add, .. }));
                assert!(matches!(*right, Expr::Number(2)));
            }
            _ => panic!("Expected multiplication"),
        }
    }

    #[test]
    fn test_parse_unary_minus() {
        let expr = parse("-price").unwrap();
        match expr {
            Expr::UnaryOp { op: UnaryOperator::Minus, expr: inner } => {
                assert!(matches!(*inner, Expr::Column(ref name) if name == "price"));
            }
            _ => panic!("Expected UnaryOp::Minus"),
        }
    }

    #[test]
    fn test_parse_double_unary_minus() {
        let expr = parse("--price").unwrap();
        match expr {
            Expr::UnaryOp { op: UnaryOperator::Minus, expr: inner } => {
                assert!(matches!(*inner, Expr::UnaryOp { op: UnaryOperator::Minus, .. }));
            }
            _ => panic!("Expected nested UnaryOp::Minus"),
        }
    }

    #[test]
    fn test_parse_chained_addition() {
        let expr = parse("1 + 2 + 3").unwrap();
        // Left associative: (1 + 2) + 3
        match expr {
            Expr::BinaryOp { op: BinaryOperator::Add, left, .. } => {
                assert!(matches!(*left, Expr::BinaryOp { op: BinaryOperator::Add, .. }));
            }
            _ => panic!("Expected chained addition"),
        }
    }

    #[test]
    fn test_parse_mixed_arithmetic() {
        let expr = parse("a * b + c / d - e % f").unwrap();
        // Should parse as: ((a * b) + (c / d)) - (e % f)
        assert!(matches!(expr, Expr::BinaryOp { op: BinaryOperator::Subtract, .. }));
    }

    #[test]
    fn test_parse_arithmetic_with_comparison() {
        let expr = parse("price * 2 > 100").unwrap();
        // Comparison has lower precedence: (price * 2) > 100
        match expr {
            Expr::BinaryOp { op: BinaryOperator::GreaterThan, left, right } => {
                assert!(matches!(*left, Expr::BinaryOp { op: BinaryOperator::Multiply, .. }));
                assert!(matches!(*right, Expr::Number(100)));
            }
            _ => panic!("Expected GreaterThan"),
        }
    }

    #[test]
    fn test_parse_arithmetic_in_select() {
        use crate::parser::parser::select::parse_select_stmt;

        let mut parser = Parser::new("SELECT price * 2 FROM products").unwrap();
        let stmt = parse_select_stmt(&mut parser).unwrap();

        assert_eq!(stmt.columns.len(), 1);
        match &stmt.columns[0] {
            Expr::BinaryOp { op: BinaryOperator::Multiply, left, right } => {
                assert!(matches!(*left.as_ref(), Expr::Column(ref name) if name == "price"));
                assert!(matches!(*right.as_ref(), Expr::Number(2)));
            }
            _ => panic!("Expected BinaryOp::Multiply"),
        }
    }

    #[test]
    fn test_parse_arithmetic_with_alias() {
        use crate::parser::parser::select::parse_select_stmt;

        let mut parser = Parser::new("SELECT price * 2 AS double_price FROM products").unwrap();
        let stmt = parse_select_stmt(&mut parser).unwrap();

        assert_eq!(stmt.columns.len(), 1);
        match &stmt.columns[0] {
            Expr::Alias { alias, expr } => {
                assert_eq!(alias, "double_price");
                assert!(matches!(
                    expr.as_ref(),
                    Expr::BinaryOp { op: BinaryOperator::Multiply, .. }
                ));
            }
            _ => panic!("Expected Alias"),
        }
    }

    #[test]
    fn test_parse_complex_select_expression() {
        use crate::parser::parser::select::parse_select_stmt;

        let mut parser = Parser::new(
            "SELECT name, price, price * 2 AS double_price, price + 100 AS increased FROM products",
        )
        .unwrap();
        let stmt = parse_select_stmt(&mut parser).unwrap();

        assert_eq!(stmt.columns.len(), 4);

        // First column: name
        assert!(matches!(&stmt.columns[0], Expr::Column(name) if name == "name"));

        // Second column: price
        assert!(matches!(&stmt.columns[1], Expr::Column(name) if name == "price"));

        // Third column: price * 2 AS double_price
        match &stmt.columns[2] {
            Expr::Alias { alias, expr } => {
                assert_eq!(alias, "double_price");
                match expr.as_ref() {
                    Expr::BinaryOp { op: BinaryOperator::Multiply, left, right } => {
                        assert!(matches!(left.as_ref(), Expr::Column(col) if col == "price"));
                        assert!(matches!(right.as_ref(), Expr::Number(2)));
                    }
                    _ => panic!("Expected Multiply"),
                }
            }
            _ => panic!("Expected Alias"),
        }

        // Fourth column: price + 100 AS increased
        match &stmt.columns[3] {
            Expr::Alias { alias, expr } => {
                assert_eq!(alias, "increased");
                match expr.as_ref() {
                    Expr::BinaryOp { op: BinaryOperator::Add, left, right } => {
                        assert!(matches!(left.as_ref(), Expr::Column(col) if col == "price"));
                        assert!(matches!(right.as_ref(), Expr::Number(100)));
                    }
                    _ => panic!("Expected Add"),
                }
            }
            _ => panic!("Expected Alias"),
        }
    }

    #[test]
    fn test_parse_nested_arithmetic() {
        let expr = parse("(a + b) * (c - d)").unwrap();
        match expr {
            Expr::BinaryOp { op: BinaryOperator::Multiply, left, right } => {
                assert!(matches!(*left, Expr::BinaryOp { op: BinaryOperator::Add, .. }));
                assert!(matches!(*right, Expr::BinaryOp { op: BinaryOperator::Subtract, .. }));
            }
            _ => panic!("Expected Multiply"),
        }
    }

    #[test]
    fn test_parse_arithmetic_with_null() {
        let expr = parse("price + NULL").unwrap();
        match expr {
            Expr::BinaryOp { op: BinaryOperator::Add, right, .. } => {
                assert!(matches!(*right, Expr::Null));
            }
            _ => panic!("Expected Add with Null"),
        }
    }

    #[test]
    fn test_parse_arithmetic_with_string() {
        let expr = parse("name + 'suffix'").unwrap();
        match expr {
            Expr::BinaryOp { op: BinaryOperator::Add, right, .. } => {
                assert!(matches!(*right, Expr::String(ref s) if s == "suffix"));
            }
            _ => panic!("Expected Add with String"),
        }
    }
}
