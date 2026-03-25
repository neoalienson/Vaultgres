//! SQL Parser.
//!
//! This module provides:
//! - Lexer/tokenizer for SQL
//! - Parser for SELECT, INSERT, UPDATE, DELETE
//! - AST node definitions
//! - Basic semantic analysis

#![allow(clippy::module_inception)]

pub mod ast;
pub mod error;
pub mod lexer;
pub mod parser;
pub mod plpgsql_ast;

#[cfg(test)]
mod parser_edge_tests;

#[cfg(test)]
mod view_tests;

#[cfg(test)]
mod view_edge_tests;

#[cfg(test)]
mod materialized_view_tests;

#[cfg(test)]
mod materialized_view_edge_tests;

#[cfg(test)]
mod trigger_tests;

#[cfg(test)]
mod trigger_edge_tests;

#[cfg(test)]
mod index_tests;

#[cfg(test)]
mod index_edge_tests;

#[cfg(test)]
mod aggregate_tests;

#[cfg(test)]
mod partition_tests;

pub use ast::*;
pub use error::{ParseError, Result};
pub use lexer::{Lexer, Token};
pub use parser::Parser;

/// Parses a SQL statement
pub fn parse(sql: &str) -> Result<Statement> {
    let mut parser = Parser::new(sql)?;
    parser.parse()
}
