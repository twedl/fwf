use std::path::Path;

use arrow_array::RecordBatch;

use crate::{Error, ReadOptions, Result, read_impl};

/// Reads a plain fixed-width file into one record batch.
pub fn read(path: impl AsRef<Path>, options: &ReadOptions) -> Result<RecordBatch> {
    let unit = path.as_ref().display().to_string();
    let bytes = std::fs::read(&path).map_err(|source| Error::Io {
        unit: unit.clone(),
        source,
    })?;
    read_impl::parse(&unit, &bytes, options)
}
