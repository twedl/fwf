//! Column builders and batched Parquet output.

use std::ffi::OsStr;
use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use polars::prelude::*;
use tempfile::NamedTempFile;

use crate::record::Record;
use crate::schema::{Field, Kind};

/// Rows per batch, and so rows per Parquet row group.
///
/// `512 * 512` is the default `row_group_size` in polars-io's
/// `ParquetWriter::finish`. `ParquetWriter::batched` does not inherit it — it
/// drops the setting and emits one row group per DataFrame chunk — so on this
/// path the batch size *is* the row group size. Matching the default is what
/// makes the streamed file land in the same shape a plain in-memory write would
/// have produced. The constant is an inline literal in polars rather than an
/// exported item, so it cannot be imported; re-check it on a polars upgrade.
pub const BATCH_ROWS: usize = 512 * 512;

enum ColumnBuilder {
    Char(StringChunkedBuilder),
    Num(PrimitiveChunkedBuilder<Float64Type>),
}

impl ColumnBuilder {
    fn new(field: &Field, capacity: usize) -> Self {
        let name: PlSmallStr = field.name.as_str().into();
        match field.kind {
            Kind::Char => ColumnBuilder::Char(StringChunkedBuilder::new(name, capacity)),
            Kind::Num => ColumnBuilder::Num(PrimitiveChunkedBuilder::new(name, capacity)),
        }
    }

    /// Append one field's text, applying the polars-equivalent semantics:
    /// `str.strip_chars()` for `Char`, and `.cast(Float64, strict=False)` after
    /// the same trim for `Num`.
    fn push(&mut self, raw: &str) {
        let value = raw.trim();
        match self {
            // A blank field is "", never null — matching strip_chars().
            ColumnBuilder::Char(builder) => builder.append_value(value),
            // Empty or unparseable is null — matching strict=False.
            ColumnBuilder::Num(builder) => match value.parse::<f64>() {
                Ok(number) => builder.append_value(number),
                Err(_) => builder.append_null(),
            },
        }
    }

    fn finish(self) -> Column {
        match self {
            ColumnBuilder::Char(builder) => builder.finish().into_series().into_column(),
            ColumnBuilder::Num(builder) => builder.finish().into_series().into_column(),
        }
    }
}

pub struct Writer {
    inner: BatchedWriter<File>,
    /// Where the finished file belongs.
    destination: PathBuf,
    /// Where it is written until then. Held for its `Drop`, which removes the
    /// file unless `finish` persisted it, and written through the cloned handle
    /// in `inner` rather than directly.
    partial: NamedTempFile,
    fields: Vec<Field>,
    builders: Vec<ColumnBuilder>,
    rows_in_batch: usize,
    total_rows: usize,
}

impl Writer {
    pub fn create(path: &Path, fields: Vec<Field>) -> Result<Self> {
        let schema = Schema::from_iter(fields.iter().map(|field| {
            polars::prelude::Field::new(
                field.name.as_str().into(),
                match field.kind {
                    Kind::Char => DataType::String,
                    Kind::Num => DataType::Float64,
                },
            )
        }));

        // Output goes to a sibling file and is renamed into place on success.
        // Writing straight to the destination would truncate an existing good
        // extract the moment this runs, and a failure partway through would
        // leave either an empty file or — past the first row group — one with
        // data and no footer, which no reader can open.
        let destination = path.to_path_buf();
        // Created in the destination's own directory so the final rename stays
        // within one filesystem. A unique name also means concurrent runs against
        // the same --out never share an in-flight file.
        let directory = match path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent,
            _ => Path::new("."),
        };
        // Named after the destination so stray files are recognisable, with the
        // random middle keeping concurrent runs against the same --out apart.
        // Errors name the path the caller passed, not the sibling it never asked
        // about, since this is where an unwritable --out surfaces.
        let stem = path.file_name().unwrap_or(OsStr::new("out"));
        let partial = tempfile::Builder::new()
            .prefix(stem)
            .suffix(".partial")
            .tempfile_in(directory)
            .with_context(|| format!("creating {}", destination.display()))?;

        // Written through a second handle so `partial` stays owned here, where
        // its Drop is what removes the file if this run does not reach `finish`.
        let file = partial
            .as_file()
            .try_clone()
            .with_context(|| format!("opening {} for writing", destination.display()))?;
        // Zstd is already polars' default compression; named here so it is not
        // silently dependent on that staying true.
        let inner = ParquetWriter::new(file)
            .with_compression(ParquetCompression::Zstd(None))
            .batched(&schema)
            .with_context(|| format!("opening {} for writing", destination.display()))?;

        let builders = fields.iter().map(|f| ColumnBuilder::new(f, BATCH_ROWS)).collect();
        Ok(Writer {
            inner,
            destination,
            partial,
            fields,
            builders,
            rows_in_batch: 0,
            total_rows: 0,
        })
    }

    pub fn push(&mut self, record: &Record) -> Result<()> {
        for (field, builder) in self.fields.iter().zip(self.builders.iter_mut()) {
            builder.push(record.field(field));
        }
        self.rows_in_batch += 1;
        self.total_rows += 1;
        if self.rows_in_batch == BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.rows_in_batch == 0 {
            return Ok(());
        }
        let fresh: Vec<ColumnBuilder> =
            self.fields.iter().map(|f| ColumnBuilder::new(f, BATCH_ROWS)).collect();
        let columns: Vec<Column> = std::mem::replace(&mut self.builders, fresh)
            .into_iter()
            .map(ColumnBuilder::finish)
            .collect();
        let frame = DataFrame::new(self.rows_in_batch, columns)?;
        self.inner.write_batch(&frame)?;
        self.rows_in_batch = 0;
        Ok(())
    }

    /// Flush the final partial batch, close the file, and move it into place.
    /// Returns the row count.
    pub fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.inner.finish()?;
        // Consumes the temp file, so persisting and deleting cannot both happen:
        // there is no Drop left that could remove the file after the rename. A
        // run that never reaches here drops it instead, which removes the
        // partial Parquet — empty, or past the first row group, holding data and
        // no footer.
        self.partial.persist(&self.destination).with_context(|| {
            format!("moving the finished file into place at {}", self.destination.display())
        })?;
        Ok(self.total_rows)
    }
}
