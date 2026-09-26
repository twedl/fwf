use arrow_array::RecordBatch;
use arrow_select::concat::concat_batches;

use crate::input::{self, Input, Location};
use crate::read_impl::Parser;
use crate::{ReadOptions, Result};

/// Reads every unit at `location` (a file, a gzip file, the chosen zip members,
/// stdin, bytes or a reader) into one record batch, in order.
pub fn read(location: impl Into<Location>, options: &ReadOptions) -> Result<RecordBatch> {
    let units = input::open(location.into(), options.container, options.entries.as_ref())?;
    let parser = Parser::new(options);
    let mut batches = Vec::new();
    for unit in units {
        batches.extend(match unit.input {
            Input::Slice(bytes) => parser.parse_slice(&unit.name, &bytes)?,
            Input::Stream(reader) => parser.parse_stream(&unit.name, reader)?,
        });
    }
    let batch = concat_batches(parser.schema(), &batches);
    // Utf8 columns hold at most 2 GiB of text, as they did from one builder.
    Ok(batch.expect("each column's text fits in 2 GiB"))
}
