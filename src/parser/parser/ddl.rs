use super::select;
use super::Parser;
use crate::parser::ast::{
    CloseCursorStmt, ColumnDef, CreateFunctionStmt, CreateIndexStmt, CreateMaterializedViewStmt,
    CreateTableStmt, CreateTriggerStmt, CreateViewStmt, DataType, DeclareCursorStmt, DescribeStmt,
    DropFunctionStmt, DropIndexStmt, DropMaterializedViewStmt, DropTableStmt, DropTriggerStmt,
    DropViewStmt, FetchCursorStmt, FetchDirection, ForeignKeyAction, ForeignKeyDef, ForeignKeyRef,
    FunctionParameter, FunctionReturnType, FunctionVolatility, ParameterMode, Statement,
    TriggerEvent, TriggerFor, TriggerTiming,
};
use crate::parser::error::{ParseError, Result};
use crate::parser::lexer::Token;

pub fn parse_create(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Create)?;

    let unique = if parser.current_token() == &Token::Unique {
        parser.advance();
        true
    } else {
        false
    };

    match parser.current_token() {
        Token::Table => parse_create_table(parser),
        Token::View => parse_create_view(parser),
        Token::Materialized => parse_create_materialized_view(parser),
        Token::Trigger => parse_create_trigger(parser),
        Token::Index => parse_create_index(parser, unique),
        Token::Function | Token::Procedure => parse_create_function(parser),
        _ => Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    }
}

fn parse_identifier_list(parser: &mut Parser) -> Result<Vec<String>> {
    let mut list = vec![parser.expect_identifier()?];
    while parser.current_token() == &Token::Comma {
        parser.advance();
        list.push(parser.expect_identifier()?);
    }
    Ok(list)
}

fn parse_primary_key_constraint(parser: &mut Parser) -> Result<Vec<String>> {
    parser.advance();
    parser.expect(Token::Key)?;
    parser.expect(Token::LeftParen)?;
    let cols = parse_identifier_list(parser)?;
    parser.expect(Token::RightParen)?;
    Ok(cols)
}

fn parse_foreign_key_constraint(parser: &mut Parser) -> Result<ForeignKeyDef> {
    parser.advance();
    parser.expect(Token::Key)?;
    parser.expect(Token::LeftParen)?;
    let fk_cols = parse_identifier_list(parser)?;
    parser.expect(Token::RightParen)?;
    parser.expect(Token::References)?;
    let ref_table = parser.expect_identifier()?;
    parser.expect(Token::LeftParen)?;
    let ref_cols = parse_identifier_list(parser)?;
    parser.expect(Token::RightParen)?;
    Ok(ForeignKeyDef {
        columns: fk_cols,
        ref_table,
        ref_columns: ref_cols,
        on_delete: ForeignKeyAction::Restrict,
        on_update: ForeignKeyAction::Restrict,
    })
}

fn parse_create_table(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Table)?;
    let table = parser.expect_identifier()?;
    parser.expect(Token::LeftParen)?;

    let mut columns = Vec::new();
    let mut primary_key = None;
    let mut foreign_keys = Vec::new();

    loop {
        if parser.current_token() == &Token::Primary {
            primary_key = Some(parse_primary_key_constraint(parser)?);
        } else if parser.current_token() == &Token::Foreign {
            foreign_keys.push(parse_foreign_key_constraint(parser)?);
        } else {
            columns.push(parse_column_def(parser)?);
        }

        if parser.current_token() != &Token::Comma {
            break;
        }
        parser.advance();
    }

    parser.expect(Token::RightParen)?;

    Ok(Statement::CreateTable(CreateTableStmt {
        table,
        columns,
        primary_key,
        foreign_keys,
        check_constraints: Vec::new(),
        unique_constraints: Vec::new(),
    }))
}

fn parse_create_view(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::View)?;

    let name = parser.expect_identifier()?;

    parser.expect(Token::As)?;

    let query = select::parse_select_stmt(parser)?;

    Ok(Statement::CreateView(CreateViewStmt { name, query: Box::new(query) }))
}

fn parse_create_materialized_view(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Materialized)?;
    parser.expect(Token::View)?;

    let name = parser.expect_identifier()?;

    parser.expect(Token::As)?;

    let query = select::parse_select_stmt(parser)?;

    Ok(Statement::CreateMaterializedView(CreateMaterializedViewStmt {
        name,
        query: Box::new(query),
    }))
}

fn parse_trigger_timing(parser: &mut Parser) -> Result<TriggerTiming> {
    let timing = match parser.current_token() {
        Token::Before => TriggerTiming::Before,
        Token::After => TriggerTiming::After,
        _ => return Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    };
    parser.advance();
    Ok(timing)
}

fn parse_trigger_event(parser: &mut Parser) -> Result<TriggerEvent> {
    let event = match parser.current_token() {
        Token::Insert => TriggerEvent::Insert,
        Token::Update => TriggerEvent::Update,
        Token::Delete => TriggerEvent::Delete,
        _ => return Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    };
    parser.advance();
    Ok(event)
}

fn parse_trigger_for(parser: &mut Parser) -> Result<TriggerFor> {
    let for_each = match parser.current_token() {
        Token::Row => TriggerFor::EachRow,
        Token::Statement => TriggerFor::EachStatement,
        _ => return Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    };
    parser.advance();
    Ok(for_each)
}

fn parse_create_trigger(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Trigger)?;
    let name = parser.expect_identifier()?;
    let timing = parse_trigger_timing(parser)?;
    let event = parse_trigger_event(parser)?;
    parser.expect(Token::On)?;
    let table = parser.expect_identifier()?;
    parser.expect(Token::For)?;
    parser.expect(Token::Each)?;
    let for_each = parse_trigger_for(parser)?;

    let when = if parser.current_token() == &Token::When {
        parser.advance();
        parser.expect(Token::LeftParen)?;
        let expr = super::expr::parse_expr(parser)?;
        parser.expect(Token::RightParen)?;
        Some(expr)
    } else {
        None
    };

    parser.expect(Token::Begin)?;
    let mut body = Vec::new();
    while parser.current_token() != &Token::End {
        body.push(parser.parse()?);
        if parser.current_token() == &Token::Semicolon {
            parser.advance();
        }
    }
    parser.expect(Token::End)?;

    Ok(Statement::CreateTrigger(CreateTriggerStmt {
        name,
        timing,
        event,
        table,
        for_each,
        when,
        body,
    }))
}

fn parse_create_index(parser: &mut Parser, unique: bool) -> Result<Statement> {
    parser.expect(Token::Index)?;

    let name = parser.expect_identifier()?;

    parser.expect(Token::On)?;
    let table = parser.expect_identifier()?;

    parser.expect(Token::LeftParen)?;

    let mut columns = Vec::new();
    let mut expressions = Vec::new();

    loop {
        if matches!(parser.current_token(), Token::Identifier(_)) {
            let col = parser.expect_identifier()?;
            columns.push(col);
        } else {
            expressions.push(super::expr::parse_expr(parser)?);
        }

        if parser.current_token() != &Token::Comma {
            break;
        }
        parser.advance();
    }

    parser.expect(Token::RightParen)?;

    let where_clause = if parser.current_token() == &Token::Where {
        parser.advance();
        Some(super::expr::parse_expr(parser)?)
    } else {
        None
    };

    Ok(Statement::CreateIndex(CreateIndexStmt {
        name,
        table,
        columns,
        expressions,
        unique,
        where_clause,
    }))
}

pub fn parse_drop(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Drop)?;

    match parser.current_token() {
        Token::Table => parse_drop_table(parser),
        Token::View => parse_drop_view(parser),
        Token::Materialized => parse_drop_materialized_view(parser),
        Token::Trigger => parse_drop_trigger(parser),
        Token::Index => parse_drop_index(parser),
        Token::Function | Token::Procedure => parse_drop_function(parser),
        _ => Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    }
}

fn parse_if_exists(parser: &mut Parser) -> Result<bool> {
    Ok(if parser.current_token() == &Token::If {
        parser.advance();
        parser.expect(Token::Exists)?;
        true
    } else {
        false
    })
}

fn parse_drop_table(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Table)?;
    let if_exists = parse_if_exists(parser)?;
    let table = parser.expect_identifier()?;
    Ok(Statement::DropTable(DropTableStmt { table, if_exists }))
}

fn parse_drop_view(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::View)?;
    let if_exists = parse_if_exists(parser)?;
    let name = parser.expect_identifier()?;
    Ok(Statement::DropView(DropViewStmt { name, if_exists }))
}

fn parse_drop_materialized_view(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Materialized)?;
    parser.expect(Token::View)?;
    let if_exists = parse_if_exists(parser)?;
    let name = parser.expect_identifier()?;
    Ok(Statement::DropMaterializedView(DropMaterializedViewStmt { name, if_exists }))
}

fn parse_drop_trigger(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Trigger)?;
    let if_exists = parse_if_exists(parser)?;
    let name = parser.expect_identifier()?;
    Ok(Statement::DropTrigger(DropTriggerStmt { name, if_exists }))
}

fn parse_drop_index(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Index)?;
    let if_exists = parse_if_exists(parser)?;
    let name = parser.expect_identifier()?;
    Ok(Statement::DropIndex(DropIndexStmt { name, if_exists }))
}

pub fn parse_describe(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Describe)?;

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

    let mut is_primary_key = false;
    let is_unique = false;
    let mut is_auto_increment = data_type == DataType::Serial;
    let mut default_value = None;
    let mut foreign_key = None;

    // Check for AUTO_INCREMENT
    if parser.current_token() == &Token::AutoIncrement {
        parser.advance();
        is_auto_increment = true;
    }

    // Check for column-level PRIMARY KEY
    if parser.current_token() == &Token::Primary {
        parser.advance();
        parser.expect(Token::Key)?;
        is_primary_key = true;
    }

    // Check for DEFAULT
    if parser.current_token() == &Token::Default {
        parser.advance();
        default_value = Some(super::expr::parse_expr(parser)?);
    }

    // Check for column-level REFERENCES
    if parser.current_token() == &Token::References {
        parser.advance();
        let ref_table = parser.expect_identifier()?;
        parser.expect(Token::LeftParen)?;
        let ref_column = parser.expect_identifier()?;
        parser.expect(Token::RightParen)?;
        foreign_key = Some(ForeignKeyRef { table: ref_table, column: ref_column });
    }

    Ok(ColumnDef {
        name,
        data_type,
        is_primary_key,
        is_unique,
        is_auto_increment,
        is_not_null: false,
        default_value,
        foreign_key,
    })
}

fn parse_data_type(parser: &mut Parser) -> Result<DataType> {
    let dtype = match parser.current_token() {
        Token::Int => DataType::Int,
        Token::Serial => DataType::Serial,
        Token::Text => DataType::Text,
        Token::Boolean => DataType::Boolean,
        Token::Date => DataType::Date,
        Token::Time => DataType::Time,
        Token::Timestamp => DataType::Timestamp,
        Token::Bytea | Token::Blob => DataType::Bytea,
        Token::Varchar => return parse_varchar(parser),
        Token::Decimal | Token::Numeric => return parse_decimal(parser),
        _ => return Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    };
    parser.advance();
    Ok(dtype)
}

fn parse_varchar(parser: &mut Parser) -> Result<DataType> {
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

fn parse_decimal(parser: &mut Parser) -> Result<DataType> {
    parser.advance();
    if parser.current_token() == &Token::LeftParen {
        parser.advance();
        if let Token::Number(p) = parser.current_token() {
            let precision = *p as u8;
            parser.advance();
            if parser.current_token() == &Token::Comma {
                parser.advance();
                if let Token::Number(s) = parser.current_token() {
                    let scale = *s as u8;
                    parser.advance();
                    parser.expect(Token::RightParen)?;
                    Ok(DataType::Decimal(precision, scale))
                } else {
                    Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token())))
                }
            } else {
                parser.expect(Token::RightParen)?;
                Ok(DataType::Decimal(precision, 0))
            }
        } else {
            Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token())))
        }
    } else {
        Ok(DataType::Decimal(10, 0))
    }
}

fn parse_create_function(parser: &mut Parser) -> Result<Statement> {
    parser.advance(); // FUNCTION or PROCEDURE
    let name = parser.expect_identifier()?;
    parser.expect(Token::LeftParen)?;

    let mut parameters = Vec::new();
    if parser.current_token() != &Token::RightParen {
        loop {
            let mode = match parser.current_token() {
                Token::Out => {
                    parser.advance();
                    ParameterMode::Out
                }
                Token::Inout => {
                    parser.advance();
                    ParameterMode::InOut
                }
                Token::Variadic => {
                    parser.advance();
                    ParameterMode::Variadic
                }
                _ => ParameterMode::In,
            };

            let param_name = parser.expect_identifier()?;
            let data_type = match parser.current_token() {
                Token::Int => {
                    parser.advance();
                    "INT".to_string()
                }
                Token::Text => {
                    parser.advance();
                    "TEXT".to_string()
                }
                Token::Avg => {
                    parser.advance();
                    "FLOAT".to_string()
                }
                Token::Identifier(s) => {
                    let t = s.clone();
                    parser.advance();
                    t
                }
                _ => {
                    return Err(ParseError::UnexpectedToken(format!(
                        "{:?}",
                        parser.current_token()
                    )))
                }
            };

            let default = if parser.current_token() == &Token::Equals {
                parser.advance();
                match parser.current_token() {
                    Token::String(s) => {
                        let val = format!("'{}'", s);
                        parser.advance();
                        Some(val)
                    }
                    Token::Number(n) => {
                        let val = n.to_string();
                        parser.advance();
                        Some(val)
                    }
                    Token::Identifier(s) => {
                        let val = s.clone();
                        parser.advance();
                        Some(val)
                    }
                    _ => None,
                }
            } else {
                None
            };

            parameters.push(FunctionParameter { name: param_name, data_type, mode, default });

            if parser.current_token() != &Token::Comma {
                break;
            }
            parser.advance();
        }
    }
    parser.expect(Token::RightParen)?;

    parser.expect(Token::Returns)?;
    let return_type = if parser.current_token() == &Token::Table {
        parser.advance();
        parser.expect(Token::LeftParen)?;
        let mut cols = Vec::new();
        loop {
            let col_name = parser.expect_identifier()?;
            let col_type = match parser.current_token() {
                Token::Int => {
                    parser.advance();
                    "INT".to_string()
                }
                Token::Text => {
                    parser.advance();
                    "TEXT".to_string()
                }
                Token::Identifier(s) => {
                    let t = s.clone();
                    parser.advance();
                    t
                }
                _ => {
                    return Err(ParseError::UnexpectedToken(format!(
                        "{:?}",
                        parser.current_token()
                    )))
                }
            };
            cols.push((col_name, col_type));
            if parser.current_token() != &Token::Comma {
                break;
            }
            parser.advance();
        }
        parser.expect(Token::RightParen)?;
        FunctionReturnType::Table(cols)
    } else if parser.current_token() == &Token::Setof {
        parser.advance();
        let type_name = match parser.current_token() {
            Token::Int => {
                parser.advance();
                "INT".to_string()
            }
            Token::Text => {
                parser.advance();
                "TEXT".to_string()
            }
            Token::Identifier(s) => {
                let t = s.clone();
                parser.advance();
                t
            }
            _ => return Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
        };
        FunctionReturnType::Setof(type_name)
    } else {
        let type_name = match parser.current_token() {
            Token::Int => {
                parser.advance();
                "INT".to_string()
            }
            Token::Text => {
                parser.advance();
                "TEXT".to_string()
            }
            Token::Identifier(s) => {
                let t = s.clone();
                parser.advance();
                t
            }
            _ => return Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
        };
        FunctionReturnType::Type(type_name)
    };

    parser.expect(Token::Language)?;
    let language = match parser.current_token() {
        Token::Sql => {
            parser.advance();
            "SQL".to_string()
        }
        Token::PlPgSql => {
            parser.advance();
            "PLPGSQL".to_string()
        }
        Token::Identifier(s) => {
            let l = s.clone();
            parser.advance();
            l
        }
        _ => return Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    };

    // Optional volatility
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

    // Optional cost hint
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

    // Optional rows hint
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

fn parse_drop_function(parser: &mut Parser) -> Result<Statement> {
    parser.advance(); // FUNCTION or PROCEDURE
    let if_exists = parse_if_exists(parser)?;
    let name = parser.expect_identifier()?;
    Ok(Statement::DropFunction(DropFunctionStmt { name, if_exists }))
}

pub fn parse_declare_cursor(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Declare)?;
    let name = parser.expect_identifier()?;
    parser.expect(Token::Cursor)?;
    parser.expect(Token::For)?;
    let query = select::parse_select_stmt(parser)?;
    Ok(Statement::DeclareCursor(DeclareCursorStmt { name, query: Box::new(query) }))
}

pub fn parse_fetch_cursor(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Fetch)?;

    let (direction, count) = match parser.current_token() {
        Token::Next => {
            parser.advance();
            (FetchDirection::Next, None)
        }
        Token::Prior => {
            parser.advance();
            (FetchDirection::Prior, None)
        }
        Token::First => {
            parser.advance();
            (FetchDirection::First, None)
        }
        Token::Last => {
            parser.advance();
            (FetchDirection::Last, None)
        }
        Token::Absolute => {
            parser.advance();
            let count = if let Token::Number(n) = parser.current_token() {
                let num = *n;
                parser.advance();
                Some(num)
            } else {
                None
            };
            (FetchDirection::Absolute, count)
        }
        Token::Relative => {
            parser.advance();
            let count = if let Token::Number(n) = parser.current_token() {
                let num = *n;
                parser.advance();
                Some(num)
            } else {
                None
            };
            (FetchDirection::Relative, count)
        }
        Token::Forward => {
            parser.advance();
            (FetchDirection::Forward, None)
        }
        Token::Backward => {
            parser.advance();
            (FetchDirection::Backward, None)
        }
        _ => (FetchDirection::Next, None),
    };

    parser.expect(Token::From)?;
    let name = parser.expect_identifier()?;

    Ok(Statement::FetchCursor(FetchCursorStmt { name, direction, count }))
}

pub fn parse_close_cursor(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Close)?;
    let name = parser.expect_identifier()?;
    Ok(Statement::CloseCursor(CloseCursorStmt { name }))
}
