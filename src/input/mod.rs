use std::fs::File;
use std::io::{self, BufWriter, Cursor, Read, Write};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use flate2::bufread::MultiGzDecoder;

use crate::{Error, Result};

mod zip;

/// Where to read from.
pub enum Location {
    Path(PathBuf),
    Stdin,
    Bytes(Bytes),
    Reader(Box<dyn Read + Send>),
}

impl From<&Path> for Location {
    fn from(path: &Path) -> Location {
        Location::Path(path.to_owned())
    }
}

impl From<PathBuf> for Location {
    fn from(path: PathBuf) -> Location {
        Location::Path(path)
    }
}

impl From<&str> for Location {
    fn from(path: &str) -> Location {
        Location::Path(path.into())
    }
}

/// How the input is packaged. `Auto` decides from the first bytes: `PK\x03\x04`
/// is zip, `1F 8B` is gzip, anything else is plain text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "cli", derive(clap::ValueEnum))]
pub enum Container {
    Auto,
    Plain,
    Gzip,
    Zip,
}

/// One file's bytes, ready to parse: a plain file, a decompressed gzip file or
/// one zip member. `name` places it in error messages.
pub(crate) struct Unit {
    pub name: String,
    pub input: Input,
}

pub(crate) enum Input {
    Slice(Bytes),
    Stream(Box<dyn Read + Send>),
}

/// Opens a location as the one unit it holds. Without an `entry`, a zip must
/// hold exactly one file; with one, the file of that name is read.
pub(crate) fn open(location: Location, container: Container, entry: Option<&str>) -> Result<Unit> {
    let (name, input) = match location {
        Location::Path(path) => {
            let name = path.display().to_string();
            let input = File::open(&path).and_then(from_file);
            (name.clone(), input.map_err(io_error(&name))?)
        }
        Location::Stdin => ("-".to_owned(), stdin().map_err(io_error("-"))?),
        Location::Bytes(bytes) => ("<bytes>".to_owned(), Input::Slice(bytes)),
        Location::Reader(reader) => ("<reader>".to_owned(), Input::Stream(reader)),
    };
    let (container, input) = match container {
        Container::Auto => sniff(input).map_err(io_error(&name))?,
        container => (container, input),
    };
    if container == Container::Zip {
        let bytes = match input {
            Input::Slice(bytes) => bytes,
            Input::Stream(reader) => spool(reader).map_err(io_error(&name))?,
        };
        return zip::member(&name, bytes, entry);
    }
    let input = match (container, input) {
        (Container::Gzip, Input::Slice(bytes)) => {
            Input::Stream(Box::new(MultiGzDecoder::new(Cursor::new(bytes))))
        }
        (Container::Gzip, Input::Stream(reader)) => {
            Input::Stream(Box::new(flate2::read::MultiGzDecoder::new(reader)))
        }
        (_, input) => input,
    };
    Ok(Unit { name, input })
}

pub(crate) fn io_error(unit: &str) -> impl FnOnce(io::Error) -> Error + '_ {
    move |source| Error::Io {
        unit: unit.to_owned(),
        source,
    }
}

/// Decides the container from the first bytes, putting them back in front of a
/// stream so nothing is lost.
fn sniff(input: Input) -> io::Result<(Container, Input)> {
    let detect = |head: &[u8]| match head {
        [b'P', b'K', 3, 4, ..] => Container::Zip,
        [0x1F, 0x8B, ..] => Container::Gzip,
        _ => Container::Plain,
    };
    Ok(match input {
        Input::Slice(bytes) => (detect(&bytes), Input::Slice(bytes)),
        Input::Stream(mut reader) => {
            let mut head = Vec::with_capacity(4);
            (&mut reader).take(4).read_to_end(&mut head)?;
            let container = detect(&head);
            (
                container,
                Input::Stream(Box::new(Cursor::new(head).chain(reader))),
            )
        }
    })
}

/// A regular file is memory-mapped; anything else, such as a pipe, is streamed.
fn from_file(file: File) -> io::Result<Input> {
    if file.metadata()?.is_file() {
        map(&file).map(Input::Slice)
    } else {
        Ok(Input::Stream(Box::new(file)))
    }
}

fn map(file: &File) -> io::Result<Bytes> {
    // SAFETY: the map is read-only. If another process changes or truncates the
    // file while it is mapped, reads can see the change or crash (SIGBUS), the
    // same trade-off ripgrep and polars make; a `--no-mmap` option is the remedy.
    let map = unsafe { memmap2::Mmap::map(file)? };
    Ok(Bytes::from_owner(map))
}

/// Stdin redirected from a file (`< file`) is mapped like the file; a pipe is
/// streamed.
fn stdin() -> io::Result<Input> {
    #[cfg(unix)]
    let handle = std::os::fd::AsFd::as_fd(&io::stdin()).try_clone_to_owned()?;
    #[cfg(windows)]
    let handle = std::os::windows::io::AsHandle::as_handle(&io::stdin()).try_clone_to_owned()?;
    from_file(File::from(handle))
}

/// Copies a stream to an anonymous temp file and maps it: a zip's index sits
/// at its end, so the whole archive has to be at hand.
fn spool(mut reader: impl Read) -> io::Result<Bytes> {
    let file = tempfile::tempfile()?;
    // A large buffer: io::copy's own is 8 KiB, about 2× slower per GB.
    let mut out = BufWriter::with_capacity(1 << 20, &file);
    io::copy(&mut reader, &mut out)?;
    out.flush()?;
    drop(out);
    map(&file)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PEOPLE: &[u8] = include_bytes!("../../tests/fixtures/people.cp1252.txt");

    fn contents(input: Input) -> Vec<u8> {
        match input {
            Input::Slice(bytes) => bytes.to_vec(),
            Input::Stream(mut reader) => {
                let mut out = Vec::new();
                reader.read_to_end(&mut out).unwrap();
                out
            }
        }
    }

    #[test]
    fn a_regular_file_is_mapped() {
        let mut file = tempfile::tempfile().unwrap();
        io::Write::write_all(&mut file, PEOPLE).unwrap();
        let input = from_file(file).unwrap();
        assert!(matches!(input, Input::Slice(_)));
        assert_eq!(contents(input), PEOPLE);
    }

    #[cfg(unix)]
    #[test]
    fn a_pipe_is_streamed() {
        let (reader, mut writer) = io::pipe().unwrap();
        let writing = std::thread::spawn(move || io::Write::write_all(&mut writer, PEOPLE));
        let file = File::from(std::os::fd::OwnedFd::from(reader));
        let input = from_file(file).unwrap();
        assert!(matches!(input, Input::Stream(_)));
        assert_eq!(contents(input), PEOPLE);
        writing.join().unwrap().unwrap();
    }

    #[test]
    fn sniffing_a_stream_keeps_its_first_bytes() {
        let stream = Input::Stream(Box::new(Cursor::new(PEOPLE)));
        let (container, input) = sniff(stream).unwrap();
        assert_eq!(container, Container::Plain);
        assert_eq!(contents(input), PEOPLE);
    }

    #[test]
    fn an_empty_file_is_an_empty_slice() {
        let input = from_file(tempfile::tempfile().unwrap()).unwrap();
        assert!(matches!(&input, Input::Slice(b) if b.is_empty()));
    }
}
