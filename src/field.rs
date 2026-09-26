use crate::Field;

/// The character positions where fields start and end, sorted and without
/// duplicates so `Encoding::byte_offsets` can convert a whole line's in one
/// pass, and each field's start and end as indexes into them.
pub(crate) struct Spans {
    boundaries: Vec<usize>,
    fields: Vec<(usize, usize)>,
}

impl Spans {
    pub(crate) fn new(fields: &[Field]) -> Spans {
        let mut boundaries: Vec<usize> = fields
            .iter()
            .flat_map(|f| [f.start, f.start + f.len])
            .collect();
        boundaries.sort_unstable();
        boundaries.dedup();
        let index = |c| {
            boundaries
                .binary_search(&c)
                .expect("every start and end is a boundary")
        };
        let fields = fields
            .iter()
            .map(|f| (index(f.start), index(f.start + f.len)))
            .collect();
        Spans { boundaries, fields }
    }

    pub(crate) fn boundaries(&self) -> &[usize] {
        &self.boundaries
    }

    /// Field `i` of a line whose boundaries sit at byte `offsets`: its value with
    /// surrounding whitespace trimmed, and that value's byte offset in the line.
    /// A blank field, or one past the end of a short line, is empty.
    pub(crate) fn value<'a>(
        &self,
        i: usize,
        line: &'a [u8],
        offsets: &[usize],
    ) -> (usize, &'a [u8]) {
        let (start, end) = self.fields[i];
        let raw = &line[offsets[start]..offsets[end]];
        let value = raw.trim_ascii_start();
        (
            offsets[start] + raw.len() - value.len(),
            value.trim_ascii_end(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DataType, Encoding};

    fn field(name: &str, start: usize, len: usize) -> Field {
        let dtype = DataType::String;
        Field {
            name: name.into(),
            start,
            len,
            dtype,
        }
    }

    fn values<'a>(spans: &Spans, line: &'a [u8]) -> Vec<(usize, &'a [u8])> {
        let mut offsets = Vec::new();
        Encoding::Utf8.byte_offsets(line, spans.boundaries(), &mut offsets);
        (0..spans.fields.len())
            .map(|i| spans.value(i, line, &offsets))
            .collect()
    }

    #[test]
    fn overlapping_fields_and_gaps() {
        let spans = Spans::new(&[
            field("date", 0, 8),
            field("year", 0, 4),
            field("code", 12, 2),
        ]);
        assert_eq!(spans.boundaries(), [0, 4, 8, 12, 14]);
        assert_eq!(
            values(&spans, b"20240115----XY"),
            [(0, &b"20240115"[..]), (0, b"2024"), (12, b"XY")]
        );
    }

    #[test]
    fn trims_and_reports_where_the_value_starts() {
        let spans = Spans::new(&[field("amount", 0, 8)]);
        assert_eq!(values(&spans, b"  -42.1 "), [(2, &b"-42.1"[..])]);
    }

    #[test]
    fn fields_past_a_short_line_are_empty() {
        let spans = Spans::new(&[field("a", 0, 3), field("b", 3, 3), field("c", 6, 3)]);
        assert_eq!(
            values(&spans, b"abcd"),
            [(0, &b"abc"[..]), (3, b"d"), (4, b"")]
        );
    }
}
