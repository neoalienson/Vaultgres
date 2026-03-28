#![allow(clippy::module_inception)]

mod aggregate_manager;
mod aggregation;
pub mod catalog;
mod check;
mod crud_helper;
mod data_manager;
mod datetime_functions;
mod function;
mod function_manager;
mod index_manager;
mod insert_validator;
mod partition_pruning;
mod persistence;
pub(crate) mod predicate;
mod schema;
mod select_executor;
pub mod string_functions;
mod table_manager;
mod transaction_manager;
mod trigger_manager;
mod tuple;
mod type_manager;
mod unique;
mod update_delete_executor;
mod value;
mod view_manager;

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
#[cfg(test)]
mod materialized_view_tests;
#[cfg(test)]
mod type_tests;

// Re-export public types
pub use crate::parser::ast::{ColumnDef as Column, CompositeTypeDef, DataType, EnumTypeDef};
pub use catalog::Catalog;
pub use check::CheckValidator;
pub use datetime_functions::DateTimeFunctions;
pub use function::{
    Aggregate, Function, FunctionLanguage, FunctionRegistry, FunctionVolatility, Parameter,
};
pub use schema::TableSchema;
pub use string_functions::StringFunctions;
pub use tuple::Tuple;
pub use unique::UniqueValidator;
pub use value::{CompositeValue, EnumValue, Range, RangeBound, Value};
