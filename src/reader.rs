use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use arrow_select::concat::concat_batches;

use crate::input::{self, Location};
use crate::read_impl::Parser;
use crate::{Error, ReadOptions, Result};

/// Reads the input at `location` (a file, a gzip file, a zip member, stdin,
/// bytes or a reader) into one record batch.
pub fn read(location: impl Into<Location>, options: &ReadOptions) -> Result<RecordBatch> {
    let (schema, batches) = open(location.into(), options)?;
    let batches = batches.collect::<Result<Vec<_>>>()?;
    // Joining fails only when a Utf8 column outgrows its 32-bit offsets.
    concat_batches(&schema, &batches).map_err(|_| Error::TooLarge)
}

/// Reads the input at `location` as record batches, one per chunk, each parsed
/// when it's asked for. Errors opening the input are returned here. Errors
/// parsing it come from the reader as `ArrowError::ExternalError` holding an
/// [`Error`], and end it.
pub fn scan(
    location: impl Into<Location>,
    options: &ReadOptions,
) -> Result<Box<dyn RecordBatchReader + Send>> {
    let (schema, batches) = open(location.into(), options)?;
    let batches = batches.map(|batch| batch.map_err(|e| ArrowError::ExternalError(Box::new(e))));
    Ok(Box::new(RecordBatchIterator::new(batches, schema)))
}

fn open(
    location: Location,
    options: &ReadOptions,
) -> Result<(
    SchemaRef,
    impl Iterator<Item = Result<RecordBatch>> + Send + use<>,
)> {
    let unit = input::open(location, options.container, options.entry.as_deref())?;
    let parser = Parser::new(options);
    Ok((parser.schema().clone(), parser.batches(unit)))
}
