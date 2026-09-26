use std::io::{self, Cursor, Read};

use bytes::Bytes;
use deflate64::Deflate64Decoder;
use flate2::bufread::DeflateDecoder;
use globset::GlobSet;
use zip::ZipArchive;

use super::{Input, Unit, io_error};
use crate::{Error, Result};

/// The archive's chosen members as units. The `zip` crate only reads the
/// index: each member's bytes are sliced straight from the archive, so a stored
/// member stays in memory as is and a compressed one is decoded as it's read.
pub(super) fn members(archive: &str, bytes: Bytes, entries: Option<&GlobSet>) -> Result<Vec<Unit>> {
    let error = |unit: &str, message: String| Error::Archive {
        unit: unit.to_owned(),
        message,
    };
    let mut index = ZipArchive::new(Cursor::new(bytes.clone()))
        .map_err(|e| error(archive, format!("not a readable zip archive ({e})")))?;
    let mut files = Vec::new();
    let mut units = Vec::new();
    for i in 0..index.len() {
        let member = index
            .by_index_raw(i)
            .map_err(|e| error(archive, e.to_string()))?;
        if member.is_dir() {
            continue;
        }
        files.push(member.name().to_owned());
        if entries.is_some_and(|entries| !entries.is_match(member.name())) {
            continue;
        }
        let name = format!("{archive}!{}", member.name());
        if member.encrypted() {
            return Err(error(&name, "is encrypted".to_owned()));
        }
        let start = member
            .data_start()
            .expect("by_index_raw reads the local header") as usize;
        let data = (bytes.len().checked_sub(start))
            .filter(|&left| left >= member.compressed_size() as usize)
            .map(|_| bytes.slice(start..start + member.compressed_size() as usize))
            .ok_or_else(|| error(&name, "runs past the end of the archive".to_owned()))?;
        let (crc, size) = (member.crc32(), member.size());
        // Without its codec features the crate has no names for methods 8 and 9.
        #[allow(deprecated)]
        let method = member.compression().to_u16();
        let input = match method {
            0 if crc32fast::hash(&data) != crc => return Err(io_error(&name)(crc_mismatch())),
            0 => Input::Slice(data),
            8 => Input::Stream(Box::new(Checked::new(
                DeflateDecoder::new(Cursor::new(data)),
                crc,
                size,
            ))),
            9 => {
                let decoder = Deflate64Decoder::with_buffer(Cursor::new(data));
                Input::Stream(Box::new(Checked::new(decoder, crc, size)))
            }
            n => {
                let message = format!(
                    "uses compression method {n}; only stored (0), deflate (8) and deflate64 (9) are supported"
                );
                return Err(error(&name, message));
            }
        };
        units.push(Unit { name, input });
    }
    match (entries, units.len()) {
        (None, 1) | (Some(_), 1..) => Ok(units),
        (None, n) => Err(error(
            archive,
            format!(
                "holds {n} files ({}); choose which to read with entries",
                files.join(", ")
            ),
        )),
        (Some(_), 0) => Err(error(
            archive,
            format!("no file matches the entries ({})", files.join(", ")),
        )),
    }
}

/// A member whose data doesn't match the size and CRC-32 its header records.
fn crc_mismatch() -> io::Error {
    let message = "data doesn't match the archive's size and CRC-32";
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// Checks a member's CRC-32 and size when its decoded stream ends.
struct Checked<R> {
    inner: R,
    hasher: crc32fast::Hasher,
    read: u64,
    crc: u32,
    size: u64,
}

impl<R> Checked<R> {
    fn new(inner: R, crc: u32, size: u64) -> Checked<R> {
        let hasher = crc32fast::Hasher::new();
        Checked {
            inner,
            hasher,
            read: 0,
            crc,
            size,
        }
    }
}

impl<R: Read> Read for Checked<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hasher.update(&buf[..n]);
        self.read += n as u64;
        if n == 0
            && !buf.is_empty()
            && (self.read != self.size || self.hasher.clone().finalize() != self.crc)
        {
            return Err(crc_mismatch());
        }
        Ok(n)
    }
}
