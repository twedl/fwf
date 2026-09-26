use memchr::memchr;

use crate::Encoding;

const BOM: &[u8] = b"\xEF\xBB\xBF";

/// A unit without what sits at its edges rather than in its records: a UTF-8
/// BOM at the start of a UTF-8 unit, and a DOS end-of-file marker (0x1A) right
/// after the last line ending. Returns where the records start, and their bytes.
pub(crate) fn unit_body(bytes: &[u8], encoding: Encoding) -> (usize, &[u8]) {
    let start = match encoding {
        Encoding::Utf8 if bytes.starts_with(BOM) => BOM.len(),
        _ => 0,
    };
    let end = match bytes {
        [.., b'\n', 0x1A] => bytes.len() - 1,
        _ => bytes.len(),
    };
    (start, &bytes[start..end])
}

/// Splits bytes into records, yielding each line's byte offset and its bytes
/// without the `\n` or `\r\n` that ends it.
pub(crate) fn lines(bytes: &[u8]) -> impl Iterator<Item = (usize, &[u8])> {
    let mut pos = 0;
    std::iter::from_fn(move || {
        if pos >= bytes.len() {
            return None;
        }
        let start = pos;
        let end = memchr(b'\n', &bytes[start..]).map_or(bytes.len(), |i| start + i);
        pos = end + 1;
        let line = &bytes[start..end];
        Some((start, line.strip_suffix(b"\r").unwrap_or(line)))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split(bytes: &[u8]) -> Vec<(usize, &[u8])> {
        lines(bytes).collect()
    }

    #[test]
    fn strips_lf_and_crlf() {
        let got = split(b"ab\r\ncd\nef");
        assert_eq!(got, [(0, &b"ab"[..]), (4, b"cd"), (7, b"ef")]);
    }

    #[test]
    fn a_final_line_ending_adds_no_record() {
        assert_eq!(split(b"ab\n"), [(0, &b"ab"[..])]);
        assert!(split(b"").is_empty());
    }

    #[test]
    fn blank_lines_are_records() {
        assert_eq!(split(b"a\n\nb\n"), [(0, &b"a"[..]), (2, b""), (3, b"b")]);
    }

    #[test]
    fn strips_a_bom_only_from_utf8() {
        let bytes = b"\xEF\xBB\xBFab\n";
        assert_eq!(unit_body(bytes, Encoding::Utf8), (3, &b"ab\n"[..]));
        assert_eq!(unit_body(bytes, Encoding::Cp1252), (0, &bytes[..]));
    }

    #[test]
    fn drops_0x1a_only_after_the_last_line_ending() {
        assert_eq!(
            unit_body(b"ab\r\n\x1A", Encoding::Cp850),
            (0, &b"ab\r\n"[..])
        );
        assert_eq!(unit_body(b"ab\x1A", Encoding::Cp850), (0, &b"ab\x1A"[..]));
    }
}
