//! Column builders and batched Parquet output.

use std::ffi::OsStr;
use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::format::{Item, Parsed, StrftimeItems};
use chrono::NaiveDate;
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
    String(StringChunkedBuilder),
    Float64(PrimitiveChunkedBuilder<Float64Type>),
    Int64(PrimitiveChunkedBuilder<Int64Type>),
    /// Holds the format already broken into items. `NaiveDate::parse_from_str`
    /// would re-lex the format string on every row — it builds a fresh
    /// `StrftimeItems` per call — and the format is identical for every row of
    /// a column, so it is lexed once per batch here instead.
    Date(PrimitiveChunkedBuilder<Int32Type>, Vec<Item<'static>>),
}

impl ColumnBuilder {
    fn new(field: &Field, capacity: usize) -> Self {
        let name: PlSmallStr = field.name.as_str().into();
        match &field.kind {
            Kind::String => ColumnBuilder::String(StringChunkedBuilder::new(name, capacity)),
            Kind::Float64 => ColumnBuilder::Float64(PrimitiveChunkedBuilder::new(name, capacity)),
            Kind::Int64 => ColumnBuilder::Int64(PrimitiveChunkedBuilder::new(name, capacity)),
            // A format that does not lex leaves no items, which parses nothing
            // and so nulls every row — the same outcome a per-row parse against
            // an unusable format already produced.
            Kind::Date(format) => ColumnBuilder::Date(
                PrimitiveChunkedBuilder::new(name, capacity),
                StrftimeItems::new(format).parse_to_owned().unwrap_or_default(),
            ),
        }
    }

    /// Append one field's text, applying the polars-equivalent semantics: every
    /// type is trimmed with `str.strip_chars()`, and then `String` keeps the
    /// text, `Float64` and `Int64` apply `.cast(…, strict=False)`, and `Date`
    /// applies `.str.to_date(format, strict=False)`.
    fn push(&mut self, raw: &str) {
        let value = raw.trim();
        match self {
            // A blank field is "", never null — matching strip_chars().
            ColumnBuilder::String(builder) => builder.append_value(value),
            // Empty or unparseable is null — matching strict=False. The same
            // rule covers the three parsed types; only the parser differs.
            ColumnBuilder::Float64(builder) => match value.parse::<f64>() {
                Ok(number) => builder.append_value(number),
                Err(_) => builder.append_null(),
            },
            // Rust's i64 parser rejects what a strict Int64 cast rejects: a
            // decimal point, an exponent, or anything past i64's range.
            ColumnBuilder::Int64(builder) => match value.parse::<i64>() {
                Ok(number) => builder.append_value(number),
                Err(_) => builder.append_null(),
            },
            ColumnBuilder::Date(builder, items) => {
                // What `NaiveDate::parse_from_str` does, minus re-lexing the
                // format: drive the pre-lexed items over this row's text.
                let mut parsed = Parsed::new();
                let date = chrono::format::parse(&mut parsed, value, items.iter())
                    .and_then(|()| parsed.to_naive_date());
                match date {
                    Ok(date) => builder.append_value(days_since_epoch(date)),
                    Err(_) => builder.append_null(),
                }
            }
        }
    }

    /// Append the null that stands in for a column the record does not carry.
    fn push_null(&mut self) {
        match self {
            ColumnBuilder::String(builder) => builder.append_null(),
            ColumnBuilder::Float64(builder) => builder.append_null(),
            ColumnBuilder::Int64(builder) => builder.append_null(),
            ColumnBuilder::Date(builder, _) => builder.append_null(),
        }
    }

    fn finish(self) -> Column {
        match self {
            ColumnBuilder::String(builder) => builder.finish().into_series().into_column(),
            ColumnBuilder::Float64(builder) => builder.finish().into_series().into_column(),
            ColumnBuilder::Int64(builder) => builder.finish().into_series().into_column(),
            // The Int32 days are reinterpreted as a Date rather than cast: the
            // numbers are already what a Date32 holds.
            ColumnBuilder::Date(builder, _) => {
                builder.finish().into_date().into_series().into_column()
            }
        }
    }
}

/// chrono's date range tops out near ±262,000 years — under 100 million days —
/// so every date it can parse fits in the i32 a Date32 stores.
fn days_since_epoch(date: NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("1970-01-01 is a valid date");
    (date - epoch).num_days() as i32
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
                // An identity mapping, and meant to stay one: the schema's type
                // names are polars' own, so this is a change of representation
                // rather than a translation between two vocabularies.
                match field.kind {
                    Kind::String => DataType::String,
                    Kind::Float64 => DataType::Float64,
                    Kind::Int64 => DataType::Int64,
                    Kind::Date(_) => DataType::Date,
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
            match &field.at {
                Some(at) => builder.push(record.field(at)),
                None => builder.push_null(),
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the sign and the origin of the stored value: a Date32 counts days
    /// from 1970-01-01 and goes negative before it, which is easy to get subtly
    /// wrong and impossible to see in a Parquet file afterwards.
    #[test]
    fn dates_convert_to_days_since_the_epoch() {
        let day = |text| NaiveDate::parse_from_str(text, "%Y%m%d").unwrap();
        assert_eq!(days_since_epoch(day("19700101")), 0);
        assert_eq!(days_since_epoch(day("19700102")), 1);
        assert_eq!(days_since_epoch(day("19691231")), -1);
        // A leap day, past which a naive 365-day arithmetic would drift.
        assert_eq!(days_since_epoch(day("20000301")), 11017);
    }
}
