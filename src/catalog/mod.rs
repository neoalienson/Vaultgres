#![allow(clippy::module_inception)]

mod aggregation;
mod catalog;
mod check;
mod function;
mod persistence;
mod predicate;
mod schema;
mod tuple;
mod unique;
mod value;

#[cfg(test)]
mod tests;

// Re-export public types
pub use catalog::Catalog;
pub use check::CheckValidator;
pub use function::{Function, FunctionLanguage, FunctionRegistry, FunctionVolatility, Parameter};
pub use schema::TableSchema;
pub use tuple::Tuple;
pub use unique::UniqueValidator;
pub use value::Value;
