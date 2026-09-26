use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use arrow_array::{RecordBatch, RecordBatchReader, RecordBatchWriter};
use arrow_schema::ArrowError;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::{Error, Result};

/// Output is written in blocks this large: CSV reached a file about 3% faster
/// than through the format writers' own 8 KiB buffers.
const BUFFER: usize = 1 << 20;

/// The format to write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
/// reader that stops reading (as `head` does) ends the write without an
/// error. A parsing error from [`scan`](crate::scan) is returned as the
/// [`Error`] it holds.
pub fn write(
    batches: impl RecordBatchReader,
    format: Format,
    destination: impl Into<Destination>,
) -> Result<()> {
    match destination.into() {
        Destination::Path(path) => to_file(batches, format, &path),
        Destination::Stdout => to_stdout(batches, format, io::stdout()),
    }
}

fn to_file(batches: impl RecordBatchReader, format: Format, path: &Path) -> Result<()> {
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

fn to_stdout(
    batches: impl RecordBatchReader,
    format: Format,
    stdout: impl Write + Send,
) -> Result<()> {
    match write_to(batches, format, stdout, "-") {
        Err(Error::Write { source, .. }) if source.kind() == io::ErrorKind::BrokenPipe => Ok(()),
        result => result,
    }
}

/// Writes the batches to `out` as `format`, then flushes it. `name` is the
/// destination's name in errors.
fn write_to(
    batches: impl RecordBatchReader,
    format: Format,
    out: impl Write + Send,
    name: &str,
) -> Result<()> {
    let schema = batches.schema();
    let mut out = Tracked {
        inner: BufWriter::with_capacity(BUFFER, out),
        error: None,
    };
    let written = match format {
        Format::Csv => {
            let mut writer = arrow_csv::Writer::new(&mut out);
            // The header is written with the first batch; an empty one makes
            // sure there is one when there are no records.
            let header = writer.write(&RecordBatch::new_empty(schema));
            header.and_then(|()| copy(batches, writer))
        }
        Format::Parquet => {
            // zstd's own default level.
            let level = ZstdLevel::try_new(3).expect("3 is a zstd level");
            let properties = WriterProperties::builder()
                .set_compression(Compression::ZSTD(level))
                .build();
            ArrowWriter::try_new(&mut out, schema, Some(properties))
                .map_err(ArrowError::from)
                .and_then(|writer| copy(batches, writer))
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

fn copy(
    batches: impl RecordBatchReader,
    mut writer: impl RecordBatchWriter,
) -> std::result::Result<(), ArrowError> {
    for batch in batches {
        writer.write(&batch?)?;
    }
    writer.close()
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
    fn stdout_stops_quietly_when_its_reader_does() {
        for format in [Format::Csv, Format::Parquet] {
            for room in [0, 100] {
                let kind = io::ErrorKind::BrokenPipe;
                let result = to_stdout(people(), format, Full { room, kind });
                assert!(result.is_ok(), "{format:?}, room {room}: {result:?}");
            }
        }
    }

    #[test]
    fn a_failed_write_keeps_its_io_error() {
        for format in [Format::Csv, Format::Parquet] {
            let kind = io::ErrorKind::StorageFull;
            let err = to_stdout(people(), format, Full { room: 100, kind }).unwrap_err();
            let Error::Write {
                destination,
                source,
            } = &err
            else {
                panic!("{format:?}: {err:?}")
            };
            assert_eq!(destination, "-");
            assert_eq!(source.kind(), kind, "{format:?}: {err}");
        }
    }
}
