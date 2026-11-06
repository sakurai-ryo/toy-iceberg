//! A small implementation of the Apache Iceberg table format.

pub mod arrow_schema;
pub mod catalog;
mod error;
pub mod expr;
pub mod manifest;
pub mod metadata;
pub mod partition;
pub mod scan;
pub mod table;
pub mod transform;
pub mod types;
pub mod values;
mod writer;

pub use error::{Error, Result};
