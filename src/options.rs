use crate::{Encoding, Schema};

/// How to read a fixed-width file.
#[derive(Debug, Clone)]
pub struct ReadOptions {
    pub(crate) schema: Schema,
    pub(crate) encoding: Encoding,
}

impl ReadOptions {
    /// Reads records laid out by `schema`. There is no default encoding: cp1252
    /// and cp850 both decode almost any byte, so a wrong guess would be silent.
    pub fn new(schema: Schema, encoding: Encoding) -> ReadOptions {
        ReadOptions { schema, encoding }
    }
}
