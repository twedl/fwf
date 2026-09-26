use memchr::memchr;

/// A unit without a DOS end-of-file marker (0x1A) right after its last line
/// ending: the marker sits at the unit's edge, not in a record.
pub(crate) fn strip_eof_marker(bytes: &[u8]) -> &[u8] {
    match bytes {
        [body @ .., 0x1A] if body.ends_with(b"\n") => body,
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
    fn drops_0x1a_only_after_the_last_line_ending() {
        assert_eq!(strip_eof_marker(b"ab\r\n\x1A"), b"ab\r\n");
        assert_eq!(strip_eof_marker(b"ab\x1A"), b"ab\x1A");
    }
}
