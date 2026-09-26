use crate::tables::{CP850, CP1252};

/// The character encoding of a fixed-width file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    Utf8,
    Cp1252,
    Cp850,
}

/// A byte that isn't valid in the field's encoding.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DecodeError {
    /// Offset of the byte within the field.
    pub offset: usize,
    pub byte: u8,
}

impl Encoding {
    /// Appends to `out` the byte offset in `line` of each character position in
    /// `chars`, which must be sorted. Positions past the end of the line map to
    /// `line.len()`.
    ///
    /// Only UTF-8 lines with non-ASCII characters need a scan; everywhere else a
    /// character is one byte.
    pub(crate) fn byte_offsets(self, line: &[u8], chars: &[usize], out: &mut Vec<usize>) {
        debug_assert!(chars.is_sorted());
        if self != Encoding::Utf8 || line.is_ascii() {
            out.extend(chars.iter().map(|&c| c.min(line.len())));
            return;
        }
        let mut wanted = chars.iter().copied().peekable();
        let mut seen = 0; // characters before byte `i`
        for (i, &b) in line.iter().enumerate() {
            if b & 0xC0 == 0x80 {
                continue; // continuation byte, inside a character
            }
            while wanted.next_if_eq(&seen).is_some() {
                out.push(i);
            }
            if wanted.peek().is_none() {
                return;
            }
            seen += 1;
        }
        out.extend(wanted.map(|_| line.len()));
    }

    /// Decodes a field to UTF-8. Bytes that are already UTF-8 are borrowed;
    /// code-page bytes above 0x7F are transcoded into `scratch`.
    pub(crate) fn decode<'a>(
        self,
        bytes: &'a [u8],
        scratch: &'a mut String,
    ) -> Result<&'a str, DecodeError> {
        let table = match self {
            Encoding::Utf8 => {
                return std::str::from_utf8(bytes).map_err(|e| DecodeError {
                    offset: e.valid_up_to(),
                    byte: bytes[e.valid_up_to()],
                });
            }
            Encoding::Cp1252 => &CP1252,
            Encoding::Cp850 => &CP850,
        };
        if bytes.is_ascii() {
            // SAFETY: every byte is below 0x80, and ASCII is valid UTF-8.
            return Ok(unsafe { std::str::from_utf8_unchecked(bytes) });
        }
        scratch.clear();
        for (offset, &byte) in bytes.iter().enumerate() {
            scratch.push(match byte {
                0..0x80 => char::from(byte),
                _ => table[usize::from(byte - 0x80)].ok_or(DecodeError { offset, byte })?,
            });
        }
        Ok(scratch.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Schema;

    fn offsets(encoding: Encoding, line: &[u8], chars: &[usize]) -> Vec<usize> {
        let mut out = Vec::new();
        encoding.byte_offsets(line, chars, &mut out);
        out
    }

    fn decode(encoding: Encoding, bytes: &[u8]) -> Result<String, DecodeError> {
        encoding
            .decode(bytes, &mut String::new())
            .map(str::to_owned)
    }

    #[test]
    fn code_page_offsets_are_character_positions() {
        let line = b"Jos\xE9 G"; // é is one byte
        assert_eq!(
            offsets(Encoding::Cp850, line, &[0, 3, 4, 6, 9]),
            [0, 3, 4, 6, 6]
        );
    }

    #[test]
    fn utf8_offsets_count_characters_not_bytes() {
        // é and Ω take 2 bytes, 😀 takes 4.
        let line = "José Ω😀x".as_bytes();
        assert_eq!(
            offsets(Encoding::Utf8, line, &[0, 3, 4, 5, 6, 7, 8, 20]),
            [0, 3, 5, 6, 8, 12, 13, 13]
        );
    }

    #[test]
    fn utf8_fixture_fields_match_cp1252_fields() {
        // Widths count characters, so slicing either file at the schema's
        // boundaries must give the same field values.
        let schema = Schema::from_json(include_bytes!("../tests/fixtures/people.schema.json"));
        let mut boundaries: Vec<usize> = (schema.unwrap().fields().iter())
            .flat_map(|f| [f.start, f.start + f.len])
            .collect();
        boundaries.sort_unstable();
        boundaries.dedup();
        let utf8 = include_str!("../tests/fixtures/people.utf-8.txt").lines();
        let cp1252 = include_bytes!("../tests/fixtures/people.cp1252.txt").split(|&b| b == b'\n');
        for (u, c) in utf8.zip(cp1252) {
            let uo = offsets(Encoding::Utf8, u.as_bytes(), &boundaries);
            let co = offsets(Encoding::Cp1252, c, &boundaries);
            for (uw, cw) in uo.windows(2).zip(co.windows(2)) {
                let expected = decode(Encoding::Cp1252, &c[cw[0]..cw[1]]).unwrap();
                assert_eq!(&u[uw[0]..uw[1]], expected);
            }
        }
    }

    #[test]
    fn utf8_rejects_invalid_bytes() {
        let err = decode(Encoding::Utf8, b"Jos\xE9").unwrap_err();
        assert_eq!(
            err,
            DecodeError {
                offset: 3,
                byte: 0xE9
            }
        );
    }

    #[test]
    fn code_page_ascii_is_borrowed() {
        let mut scratch = String::new();
        assert_eq!(Encoding::Cp1252.decode(b"Leeds", &mut scratch), Ok("Leeds"));
        assert!(scratch.is_empty());
    }

    #[test]
    fn cp1252_does_not_treat_utf8_lookalikes_as_utf8() {
        // C3 A9 is "é" in UTF-8 but two characters in cp1252.
        assert_eq!(decode(Encoding::Cp1252, b"\xC3\xA9").unwrap(), "Ã©");
    }

    #[test]
    fn code_pages_map_high_bytes() {
        assert_eq!(decode(Encoding::Cp1252, b"\x80\x9F\xE9").unwrap(), "€Ÿé");
        assert_eq!(
            decode(Encoding::Cp850, b"\x82\xE9\xD5\xFF").unwrap(),
            "éÚı\u{A0}"
        );
    }

    #[test]
    fn cp1252_rejects_its_five_undefined_bytes() {
        for byte in [0x81, 0x8D, 0x8F, 0x90, 0x9D] {
            let err = decode(Encoding::Cp1252, &[b'a', byte]).unwrap_err();
            assert_eq!(err, DecodeError { offset: 1, byte });
        }
        assert_eq!(CP1252.iter().filter(|c| c.is_none()).count(), 5);
        assert!(CP850.iter().all(Option::is_some));
    }

    #[test]
    fn code_page_fixtures_decode_to_the_utf8_fixture() {
        let utf8 = include_str!("../tests/fixtures/people.utf-8.txt");
        let cp1252 = include_bytes!("../tests/fixtures/people.cp1252.txt");
        let cp850 = include_bytes!("../tests/fixtures/people.cp850.txt");
        assert_eq!(decode(Encoding::Cp1252, cp1252).unwrap(), utf8);
        assert_eq!(decode(Encoding::Cp850, cp850).unwrap(), utf8);
    }
}
