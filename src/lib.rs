//! Read fixed-width files into Arrow record batches.

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "used by the reader from step 3")
)]
mod encoding;
mod error;
mod schema;
mod tables;

pub use encoding::Encoding;
pub use error::{Error, Result};
pub use schema::{DataType, Field, Schema};
