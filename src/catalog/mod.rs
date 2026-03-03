#![allow(clippy::module_inception)]

mod aggregation;
mod catalog;
mod check;
mod datetime_functions;
mod function;
mod persistence;
mod predicate;
mod schema;
mod string_functions;
mod tuple;
mod unique;
mod value;

#[cfg(test)]
mod batch_insert_tests;
#[cfg(test)]
mod datatype_tests;

// Re-export public types
pub use catalog::Catalog;
pub use check::CheckValidator;
pub use datetime_functions::DateTimeFunctions;
pub use function::{Function, FunctionLanguage, FunctionRegistry, FunctionVolatility, Parameter};
pub use schema::TableSchema;
pub use string_functions::StringFunctions;
pub use tuple::Tuple;
pub use unique::UniqueValidator;
pub use value::Value;
