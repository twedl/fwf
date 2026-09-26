use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{Float64Builder, StringBuilder};
use arrow_schema::{DataType as ArrowType, Field as ArrowField};

use crate::{DataType, Encoding, Error, Field, Position, Result, field};

/// The Arrow field a column is built into. Every column is nullable.
pub(crate) fn arrow_field(field: &Field) -> ArrowField {
    let dtype = match field.dtype {
        DataType::String => ArrowType::Utf8,
        DataType::Float64 => ArrowType::Float64,
    };
    ArrowField::new(&field.name, dtype, true)
}

/// Builds one field's column from a chunk's lines, each with its offset in the
/// chunk. The field's type is matched once, then each type has its own loop.
/// `position(line, byte)` places a bad value by the index of its line and its
/// offset in the chunk. It is `dyn` to keep the loops from being generic: with
/// a generic closure, the compiler once stopped inlining Arrow's `append_value`
/// into them, and parsing was 10% slower.
pub(crate) fn column(
    field: &Field,
    lines: &[(usize, &[u8])],
    encoding: Encoding,
    position: &dyn Fn(usize, usize) -> Position,
) -> Result<ArrayRef> {
    let values = lines.iter().enumerate().map(|(i, &(start, line))| {
        let (at, value) = field::value(line, field.start, field.len);
        (i, start + at, value)
    });
    match field.dtype {
        DataType::String => {
            let mut builder = StringBuilder::with_capacity(lines.len(), lines.len() * field.len);
            let mut scratch = String::new();
            for (i, at, value) in values {
                if value.is_empty() {
                    builder.append_null();
                    continue;
                }
                let text =
                    encoding
                        .decode(value, &mut scratch)
                        .map_err(|offset| Error::InvalidByte {
                            position: position(i, at + offset),
                            byte: value[offset],
                        })?;
                builder.append_value(text);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(lines.len());
            for (i, at, value) in values {
                if value.is_empty() {
                    builder.append_null();
                    continue;
                }
                let number = parse_f64(value).ok_or_else(|| Error::InvalidFloat {
                    position: position(i, at),
                    value: (encoding.decode(value, &mut String::new())).map_or_else(
                        |_| String::from_utf8_lossy(value).into_owned(),
                        str::to_owned,
                    ),
                })?;
                builder.append_value(number);
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

fn parse_f64(value: &[u8]) -> Option<f64> {
    std::str::from_utf8(value).ok()?.parse().ok()
}
