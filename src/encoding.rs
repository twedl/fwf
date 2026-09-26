use std::fmt;

use crate::tables::{CP850, CP1252};

/// The character encoding of a fixed-width file.
///
/// Both are single-byte code pages, so a character is one byte and field
/// positions are byte positions. For UTF-8 files, use polars directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    Cp1252,
    Cp850,
}

impl fmt::Display for Encoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Encoding::Cp1252 => "cp1252",
            Encoding::Cp850 => "cp850",
        })
    }
}

impl Encoding {
    /// Decodes a field to UTF-8. ASCII fields are borrowed as they are; bytes
    /// above 0x7F are transcoded into `scratch`. An undefined byte's offset is
    /// the error.
    pub(crate) fn decode<'a>(
        self,
        bytes: &'a [u8],
        scratch: &'a mut String,
    ) -> Result<&'a str, usize> {
        if bytes.is_ascii() {
            // SAFETY: every byte is below 0x80, and ASCII is valid UTF-8.
            return Ok(unsafe { std::str::from_utf8_unchecked(bytes) });
        }
        let table = match self {
            Encoding::Cp1252 => &CP1252,
            Encoding::Cp850 => &CP850,
        };
        scratch.clear();
        for (offset, &byte) in bytes.iter().enumerate() {
            scratch.push(match byte {
                0..0x80 => char::from(byte),
                _ => table[usize::from(byte - 0x80)].ok_or(offset)?,
            });
        }
        Ok(scratch.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(encoding: Encoding, bytes: &[u8]) -> Result<String, usize> {
        encoding
            .decode(bytes, &mut String::new())
            .map(str::to_owned)
    }

    #[test]
    fn ascii_is_borrowed() {
        let mut scratch = String::new();
        assert_eq!(Encoding::Cp1252.decode(b"Leeds", &mut scratch), Ok("Leeds"));
        assert!(scratch.is_empty());
    }

    #[test]
    fn code_pages_map_high_bytes() {
        // C3 A9 would be "é" in UTF-8; in cp1252 it is two characters.
        assert_eq!(
            decode(Encoding::Cp1252, b"\x80\x9F\xE9\xC3\xA9").unwrap(),
            "€ŸéÃ©"
        );
        assert_eq!(
            decode(Encoding::Cp850, b"\x82\xE9\xD5\xFF").unwrap(),
            "éÚı\u{A0}"
        );
    }

    #[test]
    fn cp1252_rejects_its_five_undefined_bytes() {
        for byte in [0x81, 0x8D, 0x8F, 0x90, 0x9D] {
            assert_eq!(decode(Encoding::Cp1252, &[b'a', byte]), Err(1));
        }
        assert_eq!(CP1252.iter().filter(|c| c.is_none()).count(), 5);
        assert!(CP850.iter().all(Option::is_some));
    }
}
