use crate::{Encoding, Schema};

/// How to read a fixed-width file.
#[derive(Debug, Clone)]
pub struct ReadOptions {
    pub(crate) schema: Schema,
    pub(crate) encoding: Encoding,
}

impl ReadOptions {
    /// Reads records laid out by `schema`, as UTF-8 unless told otherwise.
    pub fn new(schema: Schema) -> ReadOptions {
        ReadOptions {
            schema,
            encoding: Encoding::Utf8,
        }
    }

    pub fn with_encoding(mut self, encoding: Encoding) -> ReadOptions {
        self.encoding = encoding;
        self
    }
}
