//! Read fixed-width files into Arrow record batches.

mod builder;
mod encoding;
mod error;
mod field;
mod framing;
mod options;
mod read_impl;
mod reader;
mod schema;
mod tables;

pub use encoding::Encoding;
pub use error::{Error, Position, Result};
pub use options::ReadOptions;
pub use reader::read;
pub use schema::{DataType, Field, Schema};
