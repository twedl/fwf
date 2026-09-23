//! One decoded record, held in a buffer that is reused across rows.
//!
//! Fields are positioned by character, but a `&str` is indexed by byte, so the
//! buffer carries a character-index-to-byte-offset table alongside the text.
//! Building it once per record makes each field extraction an O(1) slice that
//! borrows from the buffer, instead of an allocation per field per row.
//!
//! A record whose bytes are all ASCII skips the table entirely: there, character
//! index and byte offset are the same number.

use anyhow::{Context, Result};

use crate::encoding::Encoding;
use crate::schema::Span;

pub struct Record {
    text: String,
    /// Byte offset in `text` of each character, plus a trailing `text.len()`.
    /// Left empty when `ascii` is true.
    offsets: Vec<u32>,
    ascii: bool,
}

impl Record {
    // There is one construction site. A `Default` impl to satisfy the lint would
    // add a second way to build this that nothing calls.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Record { text: String::new(), offsets: Vec::new(), ascii: true }
    }

    /// Decode one record's bytes into the buffer, replacing what was there.
    pub fn fill(&mut self, raw: &[u8], encoding: Encoding) -> Result<()> {
        self.text.clear();
        self.offsets.clear();

        // ASCII is a subset of all four encodings, decodes to itself, and needs
        // no offset table. In a mostly-ASCII file this is nearly every record.
        if raw.is_ascii() {
            self.ascii = true;
            self.text
                .push_str(std::str::from_utf8(raw).expect("ascii is always valid utf-8"));
            return Ok(());
        }
        self.ascii = false;

        let (text, offsets) = (&mut self.text, &mut self.offsets);
        match encoding.table() {
            Some(table) => {
                text.reserve(raw.len());
                offsets.reserve(raw.len() + 1);
                for &byte in raw {
                    offsets.push(text.len() as u32);
                    text.push(table[byte as usize]);
                }
            }
            None => {
                let decoded = std::str::from_utf8(raw)
                    .context("record is not valid UTF-8; is --encoding correct?")?;
                offsets.reserve(decoded.len() + 1);
                offsets.extend(decoded.char_indices().map(|(at, _)| at as u32));
                text.push_str(decoded);
            }
        }
        offsets.push(text.len() as u32);
        Ok(())
    }

    /// The text at one span, trimmed by the caller.
    pub fn field(&self, at: &Span) -> &str {
        &self.text[self.byte_of(at.start())..self.byte_of(at.end())]
    }

    /// Byte offset of a character index, clamped to the end of the record.
    ///
    /// Clamping is what makes a short record yield `""` instead of an error, and
    /// it is what polars does: `substring_ternary_offsets_value` returns an empty
    /// range once the offset runs past the end of the string.
    fn byte_of(&self, char_index: usize) -> usize {
        if self.ascii {
            char_index.min(self.text.len())
        } else {
            match self.offsets.get(char_index) {
                Some(&offset) => offset as usize,
                None => self.text.len(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn span(position: usize, length: usize) -> Span {
        Span { position, length }
    }

    fn slice(raw: &[u8], encoding: Encoding, position: usize, length: usize) -> String {
        let mut record = Record::new();
        record.fill(raw, encoding).unwrap();
        record.field(&span(position, length)).to_string()
    }

    #[test]
    fn slices_ascii_by_position() {
        let raw = b"ON 0012345 ACME";
        assert_eq!(slice(raw, Encoding::Cp1252, 1, 2), "ON");
        assert_eq!(slice(raw, Encoding::Cp1252, 4, 7), "0012345");
        assert_eq!(slice(raw, Encoding::Cp1252, 12, 4), "ACME");
    }

    #[test]
    fn a_field_past_the_end_is_empty_not_an_error() {
        assert_eq!(slice(b"short", Encoding::Cp1252, 40, 10), "");
    }

    #[test]
    fn a_partly_present_field_yields_what_is_there() {
        assert_eq!(slice(b"abcde", Encoding::Cp1252, 4, 10), "de");
    }

    #[test]
    fn an_empty_record_yields_empty_fields() {
        assert_eq!(slice(b"", Encoding::Utf8, 1, 5), "");
    }

    /// The point of positioning by character: a record means the same thing in
    /// every encoding, so one schema reads all of them.
    #[test]
    fn positions_survive_transcoding() {
        // "MONTRÉAL  QC" — the accent sits inside the first field.
        let as_cp1252 = b"MONTR\xc9AL  QC";
        let as_utf8 = "MONTRÉAL  QC".as_bytes();
        assert_ne!(as_cp1252.len(), as_utf8.len(), "byte layouts must actually differ");

        for (position, length) in [(1usize, 8usize), (11, 2)] {
            assert_eq!(
                slice(as_cp1252, Encoding::Cp1252, position, length),
                slice(as_utf8, Encoding::Utf8, position, length),
                "field at {position}..{}", position + length
            );
        }
    }

    #[test]
    fn decodes_each_single_byte_encoding_with_its_own_table() {
        // 0x82 is a low quote in cp1252, e-acute in cp850, and a C1 control in latin-1.
        assert_eq!(slice(b"\x82", Encoding::Cp1252, 1, 1), "\u{201a}");
        assert_eq!(slice(b"\x82", Encoding::Cp850, 1, 1), "é");
        assert_eq!(slice(b"\x82", Encoding::Latin1, 1, 1), "\u{0082}");
    }

    #[test]
    fn rejects_invalid_utf8_rather_than_producing_nonsense() {
        let mut record = Record::new();
        let err = record.fill(b"caf\xe9", Encoding::Utf8).unwrap_err();
        assert!(format!("{err:#}").contains("not valid UTF-8"));
    }

    #[test]
    fn the_buffer_is_reusable_across_records() {
        let mut record = Record::new();
        record.fill("PRÉCÉDENT".as_bytes(), Encoding::Utf8).unwrap();
        assert_eq!(record.field(&span(1, 3)), "PRÉ");

        record.fill(b"PLAIN", Encoding::Utf8).unwrap();
        assert_eq!(record.field(&span(1, 3)), "PLA");
        assert_eq!(record.field(&span(4, 99)), "IN");
    }
}
