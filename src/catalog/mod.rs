#![allow(clippy::module_inception)]

mod aggregation;
mod catalog;
mod check;
mod crud_helper;
mod datetime_functions;
mod function;
mod insert_validator;
mod persistence;
pub(crate) mod predicate;
mod schema;
mod select_executor;
pub mod string_functions;
mod tuple;
mod unique;
mod update_delete_executor;
mod value;

#[cfg(test)]
mod batch_insert_tests;
#[cfg(test)]
mod catalog_tests;
#[cfg(test)]
mod composite_tests;
#[cfg(test)]
mod datatype_tests;
#[cfg(test)]
mod enum_tests;
#[cfg(test)]
mod json_tests;

// Re-export public types
pub use crate::parser::ast::{ColumnDef as Column, CompositeTypeDef, DataType, EnumTypeDef};
pub use catalog::Catalog;
pub use check::CheckValidator;
pub use datetime_functions::DateTimeFunctions;
pub use function::{Function, FunctionLanguage, FunctionRegistry, FunctionVolatility, Parameter};
pub use schema::TableSchema;
pub use string_functions::StringFunctions;
pub use tuple::Tuple;
pub use unique::UniqueValidator;
pub use value::{CompositeValue, EnumValue, Range, RangeBound, Value};
