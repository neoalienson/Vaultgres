//! DDL (Data Definition Language) parser module
//!
//! This module provides parsers for DDL statements including:
//! - CREATE/DROP/ALTER for tables, views, indexes, triggers, functions, types
//! - Cursor operations (DECLARE, FETCH, CLOSE)
//!
//! The module is organized into focused submodules:
//! - `tables.rs` - Table creation, partitioning, columns, data types
//! - `views.rs` - View & materialized view creation
//! - `triggers.rs` - Trigger creation
//! - `indexes.rs` - Index creation
//! - `functions.rs` - Function & aggregate creation
//! - `types.rs` - Type creation & ALTER operations
//! - `cursors.rs` - Cursor operations
//! - `drops.rs` - DROP statements

pub mod cursors;
pub mod drops;
pub mod functions;
pub mod indexes;
pub mod tables;
pub mod triggers;
pub mod types;
pub mod views;

pub use crate::parser::lexer::Token;

pub use self::cursors::{parse_close_cursor, parse_declare_cursor, parse_fetch_cursor};
pub use self::drops::parse_drop;
pub use self::functions::{
    parse_create_aggregate, parse_create_function, parse_drop_aggregate, parse_drop_function,
};
pub use self::indexes::parse_create_index;
pub use self::tables::{
    parse_column_constraint, parse_column_def, parse_column_defs, parse_create_table,
    parse_data_type, parse_decimal, parse_drop_table, parse_foreign_key_constraint,
    parse_identifier_list, parse_partition_bound, parse_partition_method,
    parse_partition_values_list, parse_primary_key_constraint, parse_table_element, parse_varchar,
};
pub use self::triggers::parse_create_trigger;
pub use self::types::{
    parse_alter, parse_alter_table, parse_alter_type, parse_attach_partition, parse_create_type,
    parse_detach_partition, parse_drop_type,
};
pub use self::views::{parse_create_materialized_view, parse_create_view};

pub use crate::parser::error::{ParseError, Result};

pub fn parse_create(parser: &mut crate::parser::Parser) -> Result<Statement> {
    parser.expect(Token::Create)?;

    let unique = if parser.current_token() == &Token::Unique {
        parser.advance();
        true
    } else {
        false
    };

    match parser.current_token() {
        Token::Table => tables::parse_create_table(parser),
        Token::View => views::parse_create_view(parser),
        Token::Materialized => views::parse_create_materialized_view(parser),
        Token::Trigger => triggers::parse_create_trigger(parser),
        Token::Index => indexes::parse_create_index(parser, unique),
        Token::Type => types::parse_create_type(parser),
        Token::Function | Token::Procedure => functions::parse_create_function(parser),
        Token::Aggregate => functions::parse_create_aggregate(parser),
        _ => Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    }
}

pub fn parse_describe(parser: &mut crate::parser::Parser) -> Result<Statement> {
    parser.expect(Token::Describe)?;
    let table = parser.expect_identifier()?;
    Ok(Statement::Describe(crate::parser::ast::DescribeStmt { table }))
}

use crate::parser::ast::Statement;
