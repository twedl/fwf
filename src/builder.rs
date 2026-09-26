use arrow_array::ArrayRef;
use arrow_array::builder::{ArrayBuilder, Float64Builder, StringBuilder};

use crate::{DataType, Encoding, Error, Position, Result};

/// Collects one column's values into an Arrow array.
pub(crate) enum Builder {
    String(StringBuilder),
    Float64(Float64Builder),
}

impl Builder {
    pub(crate) fn new(dtype: DataType) -> Builder {
        match dtype {
            DataType::String => Builder::String(StringBuilder::new()),
            DataType::Float64 => Builder::Float64(Float64Builder::new()),
        }
    }

    pub(crate) fn append_null(&mut self) {
        match self {
            Builder::String(b) => b.append_null(),
            Builder::Float64(b) => b.append_null(),
        }
    }

    /// Appends a trimmed, non-empty field value. `position` places a byte
    /// offset within the value, for the error if there is one.
    pub(crate) fn append(
        &mut self,
        value: &[u8],
        encoding: Encoding,
        scratch: &mut String,
        position: impl Fn(usize) -> Position,
    ) -> Result<()> {
        match self {
            Builder::String(b) => {
                let text =
                    encoding
                        .decode(value, scratch)
                        .map_err(|offset| Error::InvalidByte {
                            position: position(offset),
                            byte: value[offset],
                        })?;
                b.append_value(text);
            }
            Builder::Float64(b) => {
                let number = parse_f64(value).ok_or_else(|| Error::InvalidFloat {
                    position: position(0),
                    value: (encoding.decode(value, scratch)).map_or_else(
                        |_| String::from_utf8_lossy(value).into_owned(),
                        str::to_owned,
                    ),
                })?;
                b.append_value(number);
            }
        }
        Ok(())
    }

    pub(crate) fn finish(&mut self) -> ArrayRef {
        match self {
            Builder::String(b) => ArrayBuilder::finish(b),
            Builder::Float64(b) => ArrayBuilder::finish(b),
        }
    }
}

fn parse_f64(value: &[u8]) -> Option<f64> {
    std::str::from_utf8(value).ok()?.parse().ok()
}
