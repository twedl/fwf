//! The four supported encodings.
//!
//! Three of them are single-byte: one byte decodes to exactly one character, for
//! all 256 values, so a lookup table *is* the decoder. UTF-8 is the only one that
//! needs real decoding, and the only one where a character position is not also a
//! byte position.
//!
//! No encoding crate covers this set. `encoding_rs` implements the WHATWG standard,
//! which has no cp850 at all and maps the label `latin1` onto windows-1252 — those
//! two differ across 0x80..=0x9F, so asking for latin-1 would quietly return cp1252.
//! The tables in [`crate::tables`] avoid both problems and are generated, not typed.

use crate::tables;

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Encoding {
    #[value(name = "utf8")]
    Utf8,
    #[value(name = "cp1252")]
    Cp1252,
    #[value(name = "cp850")]
    Cp850,
    #[value(name = "latin1")]
    Latin1,
}

impl Encoding {
    /// The most bytes a single character can occupy in this encoding. Used only
    /// to bound how long one record could legitimately be.
    pub fn max_bytes_per_char(self) -> usize {
        match self {
            Encoding::Utf8 => 4,
            Encoding::Cp1252 | Encoding::Cp850 | Encoding::Latin1 => 1,
        }
    }

    /// The byte-to-character table, or `None` for UTF-8, which has no such mapping.
    pub fn table(self) -> Option<&'static [char; 256]> {
        match self {
            Encoding::Utf8 => None,
            Encoding::Cp1252 => Some(&tables::CP1252),
            Encoding::Cp850 => Some(&tables::CP850),
            Encoding::Latin1 => Some(&tables::LATIN1),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FNV-1a over every code point in a table.
    ///
    /// The targeted assertions below pin the entries that carry meaning, but they
    /// leave most of each table unchecked — and `tables.rs` is generated, so a
    /// regeneration that produced different output would otherwise pass in
    /// silence. These digests are recorded by hand from tables verified against
    /// Python's codecs, so any change to a single entry, from any cause, fails
    /// here and has to be acknowledged deliberately.
    fn digest(table: &[char; 256]) -> u64 {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for character in table {
            for byte in (*character as u32).to_le_bytes() {
                hash ^= byte as u64;
                hash = hash.wrapping_mul(0x100_0000_01b3);
            }
        }
        hash
    }

    #[test]
    fn every_table_entry_is_pinned() {
        assert_eq!(digest(&tables::CP1252), 0xbf23_881d_0be0_cf78, "cp1252 table changed");
        assert_eq!(digest(&tables::CP850), 0x7161_c70c_005d_57a5, "cp850 table changed");
        assert_eq!(digest(&tables::LATIN1), 0x8084_b7f6_c938_af25, "latin1 table changed");
    }

    /// cp850 is the one table with no arithmetic rule behind it, so a spread of
    /// its upper half is checked explicitly — box drawing, shading, and letters.
    #[test]
    fn cp850_upper_half_decodes_correctly() {
        for (byte, expected) in [
            (0x80u8, '\u{00c7}'),
            (0x82, '\u{00e9}'),
            (0x9b, '\u{00f8}'),
            (0xb0, '\u{2591}'),
            (0xc5, '\u{253c}'),
            (0xdb, '\u{2588}'),
            (0xe1, '\u{00df}'),
            (0xf1, '\u{00b1}'),
            (0xff, '\u{00a0}'),
        ] {
            assert_eq!(tables::CP850[byte as usize], expected, "cp850 byte {byte:#04x}");
        }
    }

    /// cp1252's C1 range is the part that distinguishes it from latin-1, and the
    /// part a WHATWG-based decoder would get subtly different.
    #[test]
    fn cp1252_c1_range_decodes_correctly() {
        for (byte, expected) in [
            (0x80u8, '\u{20ac}'),
            (0x82, '\u{201a}'),
            (0x91, '\u{2018}'),
            (0x92, '\u{2019}'),
            (0x93, '\u{201c}'),
            (0x94, '\u{201d}'),
            (0x96, '\u{2013}'),
            (0x97, '\u{2014}'),
            (0x99, '\u{2122}'),
            (0x9f, '\u{0178}'),
        ] {
            assert_eq!(tables::CP1252[byte as usize], expected, "cp1252 byte {byte:#04x}");
        }
    }

    #[test]
    fn utf8_has_no_table() {
        assert!(Encoding::Utf8.table().is_none());
        for e in [Encoding::Cp1252, Encoding::Cp850, Encoding::Latin1] {
            assert!(e.table().is_some(), "{e:?}");
        }
    }

    /// The three encodings genuinely differ where it matters. If a future change
    /// swapped in a WHATWG-based decoder, latin1 would start returning cp1252's
    /// values and this would catch it.
    #[test]
    fn the_high_range_distinguishes_the_encodings() {
        assert_eq!(tables::CP1252[0x80], '\u{20ac}', "cp1252 0x80 is a euro sign");
        assert_eq!(tables::CP850[0x80], '\u{00c7}', "cp850 0x80 is C-cedilla");
        assert_eq!(tables::LATIN1[0x80], '\u{0080}', "latin1 0x80 is a C1 control");

        assert_eq!(tables::CP1252[0xe9], 'é');
        assert_eq!(tables::CP850[0x82], 'é');
        assert_eq!(tables::LATIN1[0xe9], 'é');
    }

    /// latin-1 is the identity mapping onto the first 256 code points.
    #[test]
    fn latin1_is_the_identity_mapping() {
        for b in 0..=255u8 {
            assert_eq!(tables::LATIN1[b as usize], b as char, "byte {b:#04x}");
        }
    }

    /// cp1252 agrees with latin-1 everywhere except 0x80..=0x9F.
    #[test]
    fn cp1252_differs_from_latin1_only_in_the_c1_range() {
        for b in 0..=255u8 {
            let same = tables::CP1252[b as usize] == tables::LATIN1[b as usize];
            let in_c1 = (0x80..=0x9f).contains(&b);
            assert!(same || in_c1, "byte {b:#04x} differs outside the C1 range");
        }
    }

    /// The five bytes cp1252 leaves undefined fall back to their C1 control.
    #[test]
    fn undefined_cp1252_bytes_map_to_their_own_code_point() {
        for b in [0x81u8, 0x8d, 0x8f, 0x90, 0x9d] {
            assert_eq!(tables::CP1252[b as usize], b as char, "byte {b:#04x}");
        }
    }

    /// Every table is a total function: no byte decodes to the replacement character
    /// unless the encoding genuinely maps it there.
    #[test]
    fn no_table_has_holes() {
        for (name, t) in [
            ("cp1252", &tables::CP1252),
            ("cp850", &tables::CP850),
            ("latin1", &tables::LATIN1),
        ] {
            for b in 0..=255u8 {
                assert_ne!(t[b as usize], '\u{fffd}', "{name} byte {b:#04x} is a hole");
            }
        }
    }
}
