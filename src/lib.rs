//! Read cp1252 and cp850 fixed-width files into Arrow record batches, and
//! write them as CSV or Parquet.
//!
//! For UTF-8 files, use polars directly.

mod builder;
mod encoding;
mod error;
mod field;
mod framing;
mod input;
mod options;
mod read_impl;
mod reader;
mod schema;
mod tables;
mod writer;

pub use encoding::Encoding;
pub use error::{Error, Position, Result};
pub use input::{Container, Location};
pub use options::ReadOptions;
pub use reader::{read, scan};
pub use schema::{DataType, Field, Schema};
pub use writer::{Destination, Format, write};
