use std::ops::Range;

use memchr::{memchr, memrchr};

/// The end of a unit without a DOS end-of-file marker (0x1A) right after its
/// last line ending: the marker sits at the unit's edge, not in a record.
/// `after_line_end` says the bytes follow a line ending, as a stream's blocks
/// after the first do.
pub(crate) fn strip_eof_marker(bytes: &[u8], after_line_end: bool) -> &[u8] {
    match bytes {
        [body @ .., 0x1A] if body.ends_with(b"\n") || (body.is_empty() && after_line_end) => body,
        _ => bytes,
    }
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

/// Where the last line ending in `bytes` ends: the length of its whole lines.
pub(crate) fn last_line_end(bytes: &[u8]) -> Option<usize> {
    memrchr(b'\n', bytes).map(|i| i + 1)
}

/// Splits bytes into chunks of whole lines: each chunk ends at the first line
/// ending at or past `size` bytes, or at the end of `bytes`.
pub(crate) fn chunks(bytes: &[u8], size: usize) -> impl Iterator<Item = Range<usize>> {
    let mut start = 0;
    std::iter::from_fn(move || {
        if start >= bytes.len() {
            return None;
        }
        let from = (start + size - 1).min(bytes.len());
        let end = memchr(b'\n', &bytes[from..]).map_or(bytes.len(), |i| from + i + 1);
        let chunk = start..end;
        start = end;
        Some(chunk)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split(bytes: &[u8]) -> Vec<(usize, &[u8])> {
        lines(bytes).collect()
    }

    fn cut(bytes: &[u8], size: usize) -> Vec<Range<usize>> {
        chunks(bytes, size).collect()
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
    fn drops_0x1a_only_after_the_last_line_ending() {
        assert_eq!(strip_eof_marker(b"ab\r\n\x1A", false), b"ab\r\n");
        assert_eq!(strip_eof_marker(b"ab\x1A", false), b"ab\x1A");
        assert_eq!(strip_eof_marker(b"\x1A", false), b"\x1A");
        assert_eq!(strip_eof_marker(b"\x1A", true), b"");
    }

    #[test]
    fn chunks_end_at_the_first_line_ending_past_their_size() {
        assert_eq!(cut(b"ab\ncd\nef\n", 3), [0..3, 3..6, 6..9]);
        assert_eq!(cut(b"ab\ncd\nef", 4), [0..6, 6..8]);
        // A line longer than the size makes a longer chunk.
        assert_eq!(cut(b"abcdef\ng\n", 2), [0..7, 7..9]);
        assert_eq!(cut(b"\n\n", 1), [0..1, 1..2]);
        assert!(cut(b"", 1).is_empty());
    }
}
