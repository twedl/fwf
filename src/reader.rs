use std::io::Read;

use arrow_array::RecordBatch;
use bytes::Bytes;

use crate::input::{self, Input, Location};
use crate::read_impl::Parser;
use crate::{ReadOptions, Result};

/// Reads every unit at `location` (a file, a gzip file, the chosen zip members,
/// stdin, bytes or a reader) into one record batch, in order.
pub fn read(location: impl Into<Location>, options: &ReadOptions) -> Result<RecordBatch> {
    let units = input::open(location.into(), options.container, options.entries.as_ref())?;
    let mut parser = Parser::new(options);
    for unit in units {
        let bytes = match unit.input {
            Input::Slice(bytes) => bytes,
            // Until step 5 parses streams block by block, read them whole.
            Input::Stream(mut reader) => {
                let mut buf = Vec::new();
                reader
                    .read_to_end(&mut buf)
                    .map_err(input::io_error(&unit.name))?;
                Bytes::from(buf)
            }
        };
        parser.parse(&unit.name, &bytes)?;
    }
    Ok(parser.finish())
}
