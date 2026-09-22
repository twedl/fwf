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
