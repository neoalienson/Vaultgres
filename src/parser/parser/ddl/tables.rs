//! Table DDL parsing
//!
//! Handles CREATE TABLE, partitioning, column definitions, and data types.

use super::super::Parser;
use crate::parser::ast::{
    ColumnDef, CreateTableStmt, DataType, Expr, ForeignKeyAction, ForeignKeyDef, ForeignKeyRef,
    PartitionBoundSpec, PartitionDef, PartitionHashBound, PartitionKey, PartitionListBound,
    PartitionMethod, PartitionRangeBound, Statement,
};
use crate::parser::error::{ParseError, Result};
use crate::parser::lexer::Token;

enum TableElement {
    Column(ColumnDef),
    PrimaryKey(Vec<String>),
    ForeignKey(ForeignKeyDef),
}

pub fn parse_identifier_list(parser: &mut Parser) -> Result<Vec<String>> {
    let mut list = vec![parser.expect_identifier()?];
    while parser.current_token() == &Token::Comma {
        parser.advance();
        list.push(parser.expect_identifier()?);
    }
    Ok(list)
}

pub fn parse_primary_key_constraint(parser: &mut Parser) -> Result<Vec<String>> {
    parser.advance();
    parser.expect(Token::Key)?;
    parser.expect(Token::LeftParen)?;
    let cols = parse_identifier_list(parser)?;
    parser.expect(Token::RightParen)?;
    Ok(cols)
}

pub fn parse_foreign_key_constraint(parser: &mut Parser) -> Result<ForeignKeyDef> {
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

pub fn parse_table_element(parser: &mut Parser) -> Result<TableElement> {
    match parser.current_token() {
        Token::Primary => Ok(TableElement::PrimaryKey(parse_primary_key_constraint(parser)?)),
        Token::Foreign => Ok(TableElement::ForeignKey(parse_foreign_key_constraint(parser)?)),
        _ => Ok(TableElement::Column(parse_column_def(parser)?)),
    }
}

pub fn parse_create_table(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Table)?;
    let table = parser.expect_identifier()?;

    if parser.current_token() == &Token::Partition {
        return parse_create_table_as_partition(parser, table);
    }

    parser.expect(Token::LeftParen)?;

    let mut columns = Vec::new();
    let mut primary_key = None;
    let mut foreign_keys = Vec::new();

    loop {
        match parse_table_element(parser)? {
            TableElement::Column(col) => columns.push(col),
            TableElement::PrimaryKey(pk) => primary_key = Some(pk),
            TableElement::ForeignKey(fk) => foreign_keys.push(fk),
        }
        if parser.current_token() != &Token::Comma {
            break;
        }
        parser.advance();
    }

    parser.expect(Token::RightParen)?;

    let partition_by = if parser.current_token() == &Token::Partition {
        parser.advance();
        parser.expect(Token::By)?;
        Some(parse_partition_method(parser)?)
    } else {
        None
    };

    let partitions = Vec::new();
    let is_partition = false;
    let parent_table = None;

    Ok(Statement::CreateTable(CreateTableStmt {
        table,
        columns,
        primary_key,
        foreign_keys,
        check_constraints: Vec::new(),
        unique_constraints: Vec::new(),
        partition_by,
        partitions,
        is_partition,
        parent_table,
        partition_bound: None,
    }))
}

pub fn parse_create_table_as_partition(parser: &mut Parser, table: String) -> Result<Statement> {
    parser.expect(Token::Partition)?;
    parser.expect(Token::Of)?;
    let parent_table = parser.expect_identifier()?;

    let bound = parse_partition_bound(parser)?;

    Ok(Statement::CreateTable(CreateTableStmt {
        table,
        columns: Vec::new(),
        primary_key: None,
        foreign_keys: Vec::new(),
        check_constraints: Vec::new(),
        unique_constraints: Vec::new(),
        partition_by: None,
        partitions: Vec::new(),
        is_partition: true,
        parent_table: Some(parent_table),
        partition_bound: Some(bound),
    }))
}

pub fn parse_partition_method(parser: &mut Parser) -> Result<(PartitionMethod, Vec<PartitionKey>)> {
    let method = match parser.current_token() {
        Token::Range => {
            parser.advance();
            PartitionMethod::Range
        }
        Token::List => {
            parser.advance();
            PartitionMethod::List
        }
        Token::Hash => {
            parser.advance();
            PartitionMethod::Hash
        }
        _ => {
            return Err(ParseError::UnexpectedToken(format!(
                "Expected RANGE, LIST, or HASH, got {:?}",
                parser.current_token()
            )));
        }
    };

    parser.expect(Token::LeftParen)?;
    let mut keys = Vec::new();
    keys.push(PartitionKey { column: parser.expect_identifier()?, opclass: None });
    while parser.current_token() == &Token::Comma {
        parser.advance();
        keys.push(PartitionKey { column: parser.expect_identifier()?, opclass: None });
    }
    parser.expect(Token::RightParen)?;

    Ok((method, keys))
}

pub fn parse_partition_bound(parser: &mut Parser) -> Result<PartitionBoundSpec> {
    if parser.current_token() == &Token::Default {
        parser.advance();
        return Ok(PartitionBoundSpec::Default);
    }

    parser.expect(Token::For)?;
    parser.expect(Token::Values)?;

    if parser.current_token() == &Token::With {
        parser.advance();
        parser.expect(Token::LeftParen)?;
        parser.expect(Token::Modulus)?;
        let modulus = if let Token::Number(n) = parser.current_token() {
            *n as u64
        } else {
            return Err(ParseError::UnexpectedToken(format!(
                "Expected number for MODULUS, got {:?}",
                parser.current_token()
            )));
        };
        parser.advance();
        parser.expect(Token::Comma)?;
        parser.expect(Token::Remainder)?;
        let remainder = if let Token::Number(n) = parser.current_token() {
            *n as u64
        } else {
            return Err(ParseError::UnexpectedToken(format!(
                "Expected number for REMAINDER, got {:?}",
                parser.current_token()
            )));
        };
        parser.advance();
        parser.expect(Token::RightParen)?;
        return Ok(PartitionBoundSpec::Hash(PartitionHashBound { modulus, remainder }));
    }

    if parser.current_token() == &Token::In {
        parser.advance();
        let values = parse_partition_values_list(parser)?;
        return Ok(PartitionBoundSpec::List(PartitionListBound { values }));
    }

    parser.expect(Token::From)?;
    let from_values = parse_partition_values_list(parser)?;
    parser.expect(Token::To)?;
    let to_values = parse_partition_values_list(parser)?;
    Ok(PartitionBoundSpec::Range(PartitionRangeBound { from_values, to_values }))
}

pub fn parse_partition_values_list(parser: &mut Parser) -> Result<Vec<Expr>> {
    parser.expect(Token::LeftParen)?;
    let mut values = Vec::new();
    loop {
        match parser.current_token() {
            Token::String(s) => {
                values.push(Expr::String(s.clone()));
                parser.advance();
            }
            Token::Number(n) => {
                values.push(Expr::Number(*n));
                parser.advance();
            }
            Token::Identifier(id) => {
                values.push(Expr::Column(id.clone()));
                parser.advance();
            }
            Token::Minus => {
                parser.advance();
                if let Token::Number(n) = parser.current_token() {
                    values.push(Expr::Number(-*n));
                    parser.advance();
                } else {
                    return Err(ParseError::UnexpectedToken(format!(
                        "Expected number after '-', got {:?}",
                        parser.current_token()
                    )));
                }
            }
            _ => {
                return Err(ParseError::UnexpectedToken(format!(
                    "Expected value in partition bound, got {:?}",
                    parser.current_token()
                )));
            }
        }
        if parser.current_token() != &Token::Comma {
            break;
        }
        parser.advance();
    }
    parser.expect(Token::RightParen)?;
    Ok(values)
}

pub fn parse_column_defs(parser: &mut Parser) -> Result<Vec<ColumnDef>> {
    let mut columns = vec![parse_column_def(parser)?];

    while parser.current_token() == &Token::Comma {
        parser.advance();
        columns.push(parse_column_def(parser)?);
    }

    Ok(columns)
}

pub fn parse_column_constraint(parser: &mut Parser, col_def: &mut ColumnDef) -> Result<()> {
    match parser.current_token() {
        Token::AutoIncrement => {
            parser.advance();
            col_def.is_auto_increment = true;
        }
        Token::Primary => {
            parser.advance();
            parser.expect(Token::Key)?;
            col_def.is_primary_key = true;
        }
        Token::Default => {
            parser.advance();
            col_def.default_value = Some(super::super::expr::parse_expr(parser)?);
        }
        Token::References => {
            parser.advance();
            let ref_table = parser.expect_identifier()?;
            parser.expect(Token::LeftParen)?;
            let ref_column = parser.expect_identifier()?;
            parser.expect(Token::RightParen)?;
            col_def.foreign_key = Some(ForeignKeyRef { table: ref_table, column: ref_column });
        }
        _ => return Ok(()),
    }
    Ok(())
}

pub fn parse_column_def(parser: &mut Parser) -> Result<ColumnDef> {
    let name = parser.expect_identifier()?;
    let data_type = parse_data_type(parser)?;
    let mut col_def = ColumnDef {
        name,
        data_type: data_type.clone(),
        is_primary_key: false,
        is_unique: false,
        is_auto_increment: data_type == DataType::Serial,
        is_not_null: false,
        default_value: None,
        foreign_key: None,
    };

    while matches!(
        parser.current_token(),
        Token::AutoIncrement | Token::Primary | Token::Default | Token::References
    ) {
        parse_column_constraint(parser, &mut col_def)?;
    }

    Ok(col_def)
}

pub fn parse_data_type(parser: &mut Parser) -> Result<DataType> {
    let base_dtype = match parser.current_token() {
        Token::Int => DataType::Int,
        Token::Serial => DataType::Serial,
        Token::Text => DataType::Text,
        Token::Boolean => DataType::Boolean,
        Token::Date => DataType::Date,
        Token::Time => DataType::Time,
        Token::Timestamp => DataType::Timestamp,
        Token::Bytea | Token::Blob => DataType::Bytea,
        Token::Json => DataType::Json,
        Token::Jsonb => DataType::Jsonb,
        Token::Enum => {
            parser.advance();
            if let Token::Identifier(type_name) = parser.current_token() {
                let type_name = type_name.clone();
                parser.advance();
                return Ok(DataType::Enum(type_name));
            } else {
                return Err(ParseError::UnexpectedToken(format!(
                    "Expected enum type name, got {:?}",
                    parser.current_token()
                )));
            }
        }
        Token::Array => {
            parser.advance();
            parser.expect(Token::LeftBracket)?;
            parser.expect(Token::RightBracket)?;
            let inner = parse_data_type(parser)?;
            return Ok(DataType::Array(Box::new(inner)));
        }
        Token::Int4Range => DataType::Int4Range,
        Token::Int8Range => DataType::Int8Range,
        Token::NumRange => DataType::NumRange,
        Token::DateRange => DataType::DateRange,
        Token::TsRange => DataType::TsRange,
        Token::TsTzRange => DataType::TsTzRange,
        Token::Varchar => return parse_varchar(parser),
        Token::Decimal | Token::Numeric => return parse_decimal(parser),
        Token::Identifier(type_name) => {
            let type_name = type_name.clone();
            parser.advance();
            return Ok(DataType::Composite(type_name));
        }
        _ => return Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    };
    parser.advance();

    if parser.current_token() == &Token::LeftBracket {
        parser.advance();
        parser.expect(Token::RightBracket)?;
        return Ok(DataType::Array(Box::new(base_dtype)));
    }

    Ok(base_dtype)
}

pub fn parse_varchar(parser: &mut Parser) -> Result<DataType> {
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

pub fn parse_decimal(parser: &mut Parser) -> Result<DataType> {
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

pub fn parse_drop_table(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Table)?;
    let if_exists = parse_if_exists(parser)?;
    let table = parser.expect_identifier()?;
    Ok(crate::parser::ast::Statement::DropTable(crate::parser::ast::DropTableStmt {
        table,
        if_exists,
    }))
}

pub fn parse_if_exists(parser: &mut Parser) -> Result<bool> {
    Ok(if parser.current_token() == &Token::If {
        parser.advance();
        parser.expect(Token::Exists)?;
        true
    } else {
        false
    })
}
