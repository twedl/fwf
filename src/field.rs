/// The value of the field at bytes `start..start + len` of a line: trimmed of
/// surrounding whitespace, with where it starts in the line. A blank field, or
/// one past the end of a short line, is empty.
pub(crate) fn value(line: &[u8], start: usize, len: usize) -> (usize, &[u8]) {
    let raw = &line[start.min(line.len())..(start + len).min(line.len())];
    let value = raw.trim_ascii_start();
    (start + raw.len() - value.len(), value.trim_ascii_end())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trims_and_reports_where_the_value_starts() {
        assert_eq!(value(b"  -42.1 ", 0, 8), (2, &b"-42.1"[..]));
    }

    #[test]
    fn fields_past_a_short_line_are_empty() {
        let line = b"abcd";
        assert_eq!(value(line, 0, 3), (0, &b"abc"[..]));
        assert_eq!(value(line, 3, 3), (3, &b"d"[..]));
        assert_eq!(value(line, 6, 3).1, b"");
    }
}
