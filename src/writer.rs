use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_csv::WriterBuilder;
use arrow_schema::{ArrowError, Schema};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::{ArrowColumnWriter, ArrowRowGroupWriterFactory, compute_leaves};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use rayon::prelude::*;

use crate::{Error, Result};

/// Output is written in blocks this large: CSV reached a file about 3% faster
/// than through the format writers' own 8 KiB buffers.
const BUFFER: usize = 1 << 20;

/// How many batches each thread writes at a time, as the parser parses a few
/// chunks per thread at a time.
const BATCHES_PER_THREAD: usize = 4;

/// The format to write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "cli", derive(clap::ValueEnum))]
pub enum Format {
    /// Comma-separated, with a header row. A null is an empty field.
    Csv,
    /// Parquet compressed with zstd.
    Parquet,
}

/// Where to write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Destination {
    Path(PathBuf),
    Stdout,
}

impl From<&Path> for Destination {
    fn from(path: &Path) -> Destination {
        Destination::Path(path.to_owned())
    }
}

impl From<PathBuf> for Destination {
    fn from(path: PathBuf) -> Destination {
        Destination::Path(path)
    }
}

impl From<&str> for Destination {
    fn from(path: &str) -> Destination {
        Destination::Path(path.into())
    }
}

/// Writes every batch to `destination` as `format`.
///
/// A file is written to a temp file beside it, which replaces it only once
/// everything is written: after an error, the file is as it was. On stdout, a
/// reader that stops reading (as `head` does) ends the write with an
/// [`Error::Write`] whose source is of kind `BrokenPipe`. A parsing error from
/// [`scan`](crate::scan) is returned as the [`Error`] it holds.
///
/// The batches are written a few per thread at a time on rayon's current
/// thread pool (choose one with `ThreadPool::install`), while the next few are
/// read. The output is the same as writing them one after another.
pub fn write(
    batches: impl RecordBatchReader + Send,
    format: Format,
    destination: impl Into<Destination>,
) -> Result<()> {
    match destination.into() {
        Destination::Path(path) => to_file(batches, format, &path),
        Destination::Stdout => write_to(batches, format, io::stdout(), "-"),
    }
}

fn to_file(batches: impl RecordBatchReader + Send, format: Format, path: &Path) -> Result<()> {
    let name = path.display().to_string();
    let mut temp = tempfile::Builder::new();
    // The permissions of any new file, rather than a temp file's owner-only.
    #[cfg(unix)]
    temp.permissions(std::os::unix::fs::PermissionsExt::from_mode(0o666));
    let dir = path.parent().unwrap_or(Path::new("."));
    let mut file = temp.tempfile_in(dir).map_err(write_error(&name))?;
    write_to(batches, format, &mut file, &name)?;
    file.persist(path)
        .map_err(|e| write_error(&name)(e.error))?;
    Ok(())
}

/// Writes the batches to `out` as `format`, then flushes it. `name` is the
/// destination's name in errors.
fn write_to(
    batches: impl RecordBatchReader + Send,
    format: Format,
    out: impl Write + Send,
    name: &str,
) -> Result<()> {
    let mut out = Tracked {
        inner: BufWriter::with_capacity(BUFFER, out),
        error: None,
    };
    let written = match format {
        Format::Csv => write_csv(batches, &mut out),
        Format::Parquet => {
            // zstd's own default level.
            let level = ZstdLevel::try_new(3).expect("3 is a zstd level");
            let properties = WriterProperties::builder()
                .set_compression(Compression::ZSTD(level))
                .build();
            write_parquet(batches, &mut out, properties)
        }
    };
    match written.and_then(|()| Ok(out.flush()?)) {
        Ok(()) => Ok(()),
        // A parsing error, which scan() wrapped for Arrow.
        Err(ArrowError::ExternalError(e)) if e.is::<Error>() => {
            Err(*e.downcast().expect("the error is an fwf::Error"))
        }
        Err(e) => Err(write_error(name)(
            out.error.unwrap_or_else(|| io::Error::other(e)),
        )),
    }
}

/// Writes a header row, then the batches, formatting each on its own thread.
fn write_csv(
    batches: impl RecordBatchReader + Send,
    out: &mut (impl Write + Send),
) -> std::result::Result<(), ArrowError> {
    // The header is written with the first batch; an empty one makes sure
    // there is one when there are no records.
    let mut header = arrow_csv::Writer::new(Vec::new());
    header.write(&RecordBatch::new_empty(batches.schema()))?;
    out.write_all(&header.into_inner())?;
    in_waves(batches, |wave| {
        let texts = wave.par_iter().map(|batch| {
            let mut writer = WriterBuilder::new().with_header(false).build(Vec::new());
            writer.write(batch)?;
            Ok(writer.into_inner())
        });
        for text in texts.collect::<std::result::Result<Vec<_>, ArrowError>>()? {
            out.write_all(&text)?;
        }
        Ok(())
    })
}

/// Writes the same file as `ArrowWriter`, encoding each field's columns on
/// their own thread.
fn write_parquet(
    batches: impl RecordBatchReader + Send,
    out: impl Write + Send,
    properties: WriterProperties,
) -> std::result::Result<(), ArrowError> {
    let schema = batches.schema();
    let limit = properties.max_row_group_row_count().unwrap_or(usize::MAX);
    let writer = ArrowWriter::try_new(out, schema.clone(), Some(properties))?;
    let (mut file, factory) = writer.into_serialized_writer()?;
    let mut fields = column_writers(&factory, &file)?;
    let mut rows = 0;
    in_waves(batches, |wave| {
        // The wave's rows for the current row group. A batch that fills it is
        // split, and the rest goes to the next.
        let mut pieces = Vec::new();
        for batch in wave {
            let mut batch = batch.clone();
            while batch.num_rows() > 0 {
                let take = batch.num_rows().min(limit - rows);
                pieces.push(batch.slice(0, take));
                batch = batch.slice(take, batch.num_rows() - take);
                rows += take;
                if rows == limit {
                    encode(&mut fields, &schema, &pieces)?;
                    pieces.clear();
                    close_row_group(&mut file, std::mem::take(&mut fields))?;
                    fields = column_writers(&factory, &file)?;
                    rows = 0;
                }
            }
        }
        Ok(encode(&mut fields, &schema, &pieces)?)
    })?;
    if rows > 0 {
        close_row_group(&mut file, fields)?;
    }
    file.close()?;
    Ok(())
}

/// The next row group's column writers, one per leaf column, grouped by the
/// field they belong to.
fn column_writers<W: Write + Send>(
    factory: &ArrowRowGroupWriterFactory,
    file: &SerializedFileWriter<W>,
) -> parquet::errors::Result<Vec<Vec<ArrowColumnWriter>>> {
    let leaves = file.schema_descr();
    let mut fields: Vec<Vec<_>> = leaves
        .root_schema()
        .get_fields()
        .iter()
        .map(|_| Vec::new())
        .collect();
    let writers = factory.create_column_writers(file.flushed_row_groups().len())?;
    for (leaf, writer) in writers.into_iter().enumerate() {
        fields[leaves.get_column_root_idx(leaf)].push(writer);
    }
    Ok(fields)
}

/// Encodes batches into a row group's writers, each field on its own thread.
fn encode(
    fields: &mut [Vec<ArrowColumnWriter>],
    schema: &Schema,
    batches: &[RecordBatch],
) -> parquet::errors::Result<()> {
    fields
        .par_iter_mut()
        .enumerate()
        .try_for_each(|(i, writers)| {
            for batch in batches {
                let leaves = compute_leaves(schema.field(i), batch.column(i))?;
                for (writer, leaf) in writers.iter_mut().zip(&leaves) {
                    writer.write(leaf)?;
                }
            }
            Ok(())
        })
}

/// Finishes a row group's columns in parallel, then appends them to the file.
fn close_row_group<W: Write + Send>(
    file: &mut SerializedFileWriter<W>,
    fields: Vec<Vec<ArrowColumnWriter>>,
) -> parquet::errors::Result<()> {
    let chunks = fields
        .into_par_iter()
        .flatten()
        .map(ArrowColumnWriter::close);
    let chunks = chunks.collect::<parquet::errors::Result<Vec<_>>>()?;
    let mut row_group = file.next_row_group()?;
    for chunk in chunks {
        chunk.append_to_row_group(&mut row_group)?;
    }
    row_group.close()?;
    Ok(())
}

/// Hands `write` the batches a few per thread at a time, to write in parallel,
/// and reads the next few while it writes. A parsing error ends the write once
/// the batches before it are written, but the ones read with it are dropped.
fn in_waves(
    mut batches: impl RecordBatchReader + Send,
    mut write: impl FnMut(&[RecordBatch]) -> std::result::Result<(), ArrowError> + Send,
) -> std::result::Result<(), ArrowError> {
    let size = rayon::current_num_threads() * BATCHES_PER_THREAD;
    let mut read = || {
        let wave = batches.by_ref().take(size);
        wave.collect::<std::result::Result<Vec<_>, _>>()
    };
    let mut wave = read()?;
    while !wave.is_empty() {
        let (written, next) = rayon::join(|| write(&wave), &mut read);
        written?;
        wave = next?;
    }
    Ok(())
}

fn write_error(destination: &str) -> impl FnOnce(io::Error) -> Error + '_ {
    move |source| Error::Write {
        destination: destination.to_owned(),
        source,
    }
}

/// Passes writes through, keeping the error of the last one if it failed: the
/// format writers can turn an I/O error into text.
struct Tracked<W> {
    inner: W,
    error: Option<io::Error>,
}

impl<W> Tracked<W> {
    fn track<T>(&mut self, result: io::Result<T>) -> io::Result<T> {
        self.error = None;
        result.map_err(|e| {
            let kind = e.kind();
            self.error = Some(e);
            kind.into()
        })
    }
}

impl<W: Write> Write for Tracked<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let result = self.inner.write(buf);
        self.track(result)
    }

    fn flush(&mut self) -> io::Result<()> {
        let result = self.inner.flush();
        self.track(result)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::{Encoding, Location, ReadOptions, Schema};

    const PEOPLE: &[u8] = include_bytes!("../tests/fixtures/people.cp1252.txt");
    const SCHEMA: &[u8] = include_bytes!("../tests/fixtures/people.schema.json");

    fn people() -> Box<dyn RecordBatchReader + Send> {
        let options = ReadOptions::new(Schema::from_json(SCHEMA).unwrap(), Encoding::Cp1252);
        let location = Location::Bytes(Bytes::from_static(PEOPLE));
        crate::scan(location, &options.with_chunk_size(1)).unwrap()
    }

    /// Takes `room` bytes, then fails every write with `kind`.
    struct Full {
        room: usize,
        kind: io::ErrorKind,
    }

    impl Write for Full {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if self.room == 0 {
                return Err(self.kind.into());
            }
            let n = buf.len().min(self.room);
            self.room -= n;
            Ok(n)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_failed_write_keeps_its_io_error() {
        // A closed pipe must keep its kind, so a command-line tool can exit quietly.
        for kind in [io::ErrorKind::BrokenPipe, io::ErrorKind::StorageFull] {
            for format in [Format::Csv, Format::Parquet] {
                for room in [0, 100] {
                    let err = write_to(people(), format, Full { room, kind }, "-").unwrap_err();
                    let Error::Write {
                        destination,
                        source,
                    } = &err
                    else {
                        panic!("{format:?}, room {room}: {err:?}")
                    };
                    assert_eq!(destination, "-");
                    assert_eq!(source.kind(), kind, "{format:?}, room {room}: {err}");
                }
            }
        }
    }

    #[test]
    fn parquet_is_the_file_arrow_writer_writes() {
        // On one thread, 4 batches of about 4 records are written at a time, so
        // 80 records make several waves, and row groups of 7 split batches.
        let bytes = Bytes::from(PEOPLE.repeat(10));
        let options = ReadOptions::new(Schema::from_json(SCHEMA).unwrap(), Encoding::Cp1252);
        let options = options.with_chunk_size(200);
        let scan = || crate::scan(Location::Bytes(bytes.clone()), &options).unwrap();
        let properties = || {
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(7))
                .build()
        };

        let pool = rayon::ThreadPoolBuilder::new().num_threads(1).build();
        let mut ours = Vec::new();
        let written = pool
            .unwrap()
            .install(|| write_parquet(scan(), &mut ours, properties()));
        written.unwrap();

        let mut theirs = Vec::new();
        let writer = ArrowWriter::try_new(&mut theirs, scan().schema(), Some(properties()));
        let mut writer = writer.unwrap();
        for batch in scan() {
            writer.write(&batch.unwrap()).unwrap();
        }
        writer.close().unwrap();
        assert_eq!(ours, theirs);
    }
}
