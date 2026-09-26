//! Read fixed-width files into Arrow record batches.

mod error;
mod schema;

pub use error::{Error, Result};
pub use schema::{DataType, Field, Schema};
