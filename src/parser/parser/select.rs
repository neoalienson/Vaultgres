use super::Parser;
use crate::parser::error::{Result, ParseError};
use crate::parser::lexer::Token;
use crate::parser::ast::{Statement, SelectStmt, OrderByExpr};

pub fn parse_select(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Select)?;
    
    let columns = parse_select_list(parser)?;
    
    let from = if parser.current_token() == &Token::From {
        parser.advance();
        parser.expect_identifier()?
    } else {
        String::new()
    };
    
    let where_clause = if parser.current_token() == &Token::Where {
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
    
    Ok(Statement::Select(SelectStmt {
        columns,
        from,
        where_clause,
        order_by,
        limit,
        offset,
    }))
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
