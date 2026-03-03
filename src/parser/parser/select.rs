use super::Parser;
use crate::parser::ast::{JoinClause, JoinType, OrderByExpr, SelectStmt, Statement};
use crate::parser::error::{ParseError, Result};
use crate::parser::lexer::Token;

pub fn parse_select(parser: &mut Parser) -> Result<Statement> {
    let stmt = parse_select_stmt(parser)?;
    Ok(Statement::Select(stmt))
}

pub fn parse_select_stmt(parser: &mut Parser) -> Result<SelectStmt> {
    parser.expect(Token::Select)?;

    let distinct = if parser.current_token() == &Token::Distinct {
        parser.advance();
        true
    } else {
        false
    };

    let columns = parse_select_list(parser)?;

    let from = if parser.current_token() == &Token::From {
        parser.advance();
        parser.expect_identifier()?
    } else {
        String::new()
    };

    let table_alias = if parser.current_token() == &Token::As {
        parser.advance();
        Some(parser.expect_identifier()?)
    } else {
        None
    };

    let mut joins = Vec::new();
    while matches!(
        parser.current_token(),
        Token::Join | Token::Lateral | Token::Inner | Token::Left | Token::Right | Token::Full
    ) {
        joins.push(parse_join(parser)?);
    }

    let where_clause = if parser.current_token() == &Token::Where {
        parser.advance();
        Some(super::expr::parse_expr(parser)?)
    } else {
        None
    };

    let group_by = if parser.current_token() == &Token::Group {
        parser.advance();
        parser.expect(Token::By)?;
        Some(parse_group_by_list(parser)?)
    } else {
        None
    };

    let having = if parser.current_token() == &Token::Having {
        parser.advance();
        Some(super::expr::parse_expr(parser)?)
    } else {
        None
    };

    let order_by = if parser.current_token() == &Token::Order {
        parser.advance();
        parser.expect(Token::By)?;
        Some(parse_order_by_list(parser)?)
    } else {
        None
    };

    let limit = if parser.current_token() == &Token::Limit {
        parser.advance();
        if let Token::Number(n) = parser.current_token() {
            let limit_val = *n as usize;
            parser.advance();
            Some(limit_val)
        } else {
            return Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token())));
        }
    } else {
        None
    };

    let offset = if parser.current_token() == &Token::Offset {
        parser.advance();
        if let Token::Number(n) = parser.current_token() {
            let offset_val = *n as usize;
            parser.advance();
            Some(offset_val)
        } else {
            return Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token())));
        }
    } else {
        None
    };

    Ok(SelectStmt {
        distinct,
        columns,
        from,
        table_alias,
        joins,
        where_clause,
        group_by,
        having,
        order_by,
        limit,
        offset,
    })
}

fn parse_join(parser: &mut Parser) -> Result<JoinClause> {
    let lateral = if parser.current_token() == &Token::Lateral {
        parser.advance();
        true
    } else {
        false
    };

    let join_type = match parser.current_token() {
        Token::Inner => {
            parser.advance();
            parser.expect(Token::Join)?;
            JoinType::Inner
        }
        Token::Left => {
            parser.advance();
            parser.expect(Token::Join)?;
            JoinType::Left
        }
        Token::Right => {
            parser.advance();
            parser.expect(Token::Join)?;
            JoinType::Right
        }
        Token::Full => {
            parser.advance();
            parser.expect(Token::Join)?;
            JoinType::Full
        }
        Token::Join => {
            parser.advance();
            JoinType::Inner
        }
        _ => return Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    };

    let table = parser.expect_identifier()?;

    let alias = if parser.current_token() == &Token::As {
        parser.advance();
        Some(parser.expect_identifier()?)
    } else {
        None
    };

    parser.expect(Token::On)?;
    let on = super::expr::parse_expr(parser)?;

    Ok(JoinClause { join_type, lateral, table, alias, on })
}

fn parse_select_list(parser: &mut Parser) -> Result<Vec<crate::parser::ast::Expr>> {
    use crate::parser::ast::Expr;

    if parser.current_token() == &Token::Star {
        parser.advance();
        return Ok(vec![Expr::Star]);
    }

    super::expr::parse_expr_list(parser)
}

fn parse_order_by_list(parser: &mut Parser) -> Result<Vec<OrderByExpr>> {
    let mut order_by = Vec::new();

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

    Ok(order_by)
}

fn parse_group_by_list(parser: &mut Parser) -> Result<Vec<String>> {
    let mut columns = Vec::new();

    loop {
        columns.push(parser.expect_identifier()?);

        if parser.current_token() != &Token::Comma {
            break;
        }
        parser.advance();
    }

    Ok(columns)
}
