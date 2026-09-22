//! Column builders and batched Parquet output.

use std::fs::File;
use std::path::Path;

use anyhow::{Context, Result};
use polars::prelude::*;

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

        let file =
            File::create(path).with_context(|| format!("creating {}", path.display()))?;
        // Zstd is already polars' default compression; named here so it is not
        // silently dependent on that staying true.
        let inner = ParquetWriter::new(file)
            .with_compression(ParquetCompression::Zstd(None))
            .batched(&schema)
            .with_context(|| format!("opening {} for writing", path.display()))?;

        let builders = fields.iter().map(|f| ColumnBuilder::new(f, BATCH_ROWS)).collect();
        Ok(Writer { inner, fields, builders, rows_in_batch: 0, total_rows: 0 })
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

    /// Flush the final partial batch and close the file. Returns the row count.
    pub fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.inner.finish()?;
        Ok(self.total_rows)
    }
}
