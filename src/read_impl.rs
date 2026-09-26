use std::io::Read;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{Schema as ArrowSchema, SchemaRef};
use rayon::prelude::*;

use crate::{Position, ReadOptions, Result, builder, framing, input};

/// About 1 MiB, inside the 1–4 MB range where filling a column at a time
/// measured fastest: small enough that a chunk's lines and columns stay in
/// cache.
const CHUNK_SIZE: usize = 1 << 20;

/// Parses units into record batches, one per chunk. Each chunk is filled a
/// column at a time.
pub(crate) struct Parser<'a> {
    options: &'a ReadOptions,
    schema: SchemaRef,
    chunk_size: usize,
}

impl<'a> Parser<'a> {
    pub(crate) fn new(options: &'a ReadOptions) -> Parser<'a> {
        let fields = options.schema.fields().iter().map(builder::arrow_field);
        Parser {
            options,
            schema: Arc::new(ArrowSchema::new(fields.collect::<Vec<_>>())),
            chunk_size: CHUNK_SIZE,
        }
    }

    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Parses an in-memory unit's chunks in parallel, in order. If several
    /// chunks fail, the error is the one that comes first in the unit.
    pub(crate) fn parse_slice(&self, unit: &str, bytes: &[u8]) -> Result<Vec<RecordBatch>> {
        let bytes = framing::strip_eof_marker(bytes, false);
        let chunks: Vec<_> = framing::chunks(bytes, self.chunk_size).collect();
        let batches: Vec<Result<RecordBatch>> = (chunks.into_par_iter())
            // Each worker reuses one line index from chunk to chunk.
            .map_init(Vec::new, |lines, chunk| {
                lines.clear();
                lines.extend(framing::lines(&bytes[chunk.clone()]));
                let records_before = || framing::lines(&bytes[..chunk.start]).count();
                self.parse_chunk(unit, lines, chunk.start, records_before)
            })
            .collect();
        batches.into_iter().collect()
    }

    /// Parses a streamed unit a block at a time: each block is cut after its
    /// last line ending, and the rest is carried into the next.
    pub(crate) fn parse_stream(
        &self,
        unit: &str,
        mut reader: impl Read,
    ) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        let mut buf = Vec::new();
        let mut offset = 0;
        loop {
            let read = (&mut reader)
                .take(self.chunk_size as u64)
                .read_to_end(&mut buf)
                .map_err(input::io_error(unit))?;
            let at_end = read < self.chunk_size;
            let end = match framing::last_line_end(&buf) {
                _ if at_end => buf.len(),
                Some(end) => end,
                // A line longer than a block: read more of it.
                None => continue,
            };
            // Every block after the first follows the line ending it was cut at.
            let block = framing::strip_eof_marker(&buf[..end], offset > 0);
            if !block.is_empty() {
                let lines: Vec<_> = framing::lines(block).collect();
                let records_before = || batches.iter().map(RecordBatch::num_rows).sum();
                let batch = self.parse_chunk(unit, &lines, offset, records_before)?;
                batches.push(batch);
            }
            if at_end {
                return Ok(batches);
            }
            offset += end;
            buf.drain(..end);
        }
    }

    /// Parses a chunk's lines, each with its offset in the chunk, a column at a
    /// time. `offset` is where the chunk starts in its unit; `records_before`
    /// counts the unit's records before it, and is only called for an error.
    fn parse_chunk(
        &self,
        unit: &str,
        lines: &[(usize, &[u8])],
        offset: usize,
        records_before: impl Fn() -> usize,
    ) -> Result<RecordBatch> {
        let fields = self.options.schema.fields();
        let columns = fields.iter().map(|field| {
            builder::column(field, lines, self.options.encoding, |line, byte| Position {
                unit: unit.to_owned(),
                record: records_before() + line + 1,
                field: field.name.clone(),
                byte: offset + byte,
            })
        });
        let columns = columns.collect::<Result<Vec<_>>>()?;
        // The row count keeps a schema with no fields from failing.
        let batch_options = RecordBatchOptions::new().with_row_count(Some(lines.len()));
        let batch = RecordBatch::try_new_with_options(self.schema.clone(), columns, &batch_options);
        Ok(batch.expect("each column is built as its field's type"))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow_select::concat::concat_batches;

    use super::*;
    use crate::{Encoding, Schema};

    const PEOPLE: &[u8] = include_bytes!("../tests/fixtures/people.cp1252.txt");
    const SCHEMA: &[u8] = include_bytes!("../tests/fixtures/people.schema.json");

    const AMOUNTS: &str = r#"{"fields": [
        {"name": "name", "position": 1, "length": 4},
        {"name": "amount", "position": 5, "length": 6, "type": "Float64"}
    ]}"#;

    fn options(json: &[u8]) -> ReadOptions {
        ReadOptions::new(Schema::from_json(json).unwrap(), Encoding::Cp1252)
    }

    /// Reads bytes as one slice or one stream, in chunks of `chunk_size`.
    fn read(
        options: &ReadOptions,
        bytes: &[u8],
        chunk_size: usize,
        stream: bool,
    ) -> Result<RecordBatch> {
        let parser = Parser {
            chunk_size,
            ..Parser::new(options)
        };
        let batches = if stream {
            parser.parse_stream("test.txt", Cursor::new(bytes))?
        } else {
            parser.parse_slice("test.txt", bytes)?
        };
        Ok(concat_batches(parser.schema(), &batches).unwrap())
    }

    #[test]
    fn chunk_and_block_sizes_dont_change_the_records() {
        let options = options(SCHEMA);
        let whole = read(&options, PEOPLE, PEOPLE.len(), false).unwrap();
        assert_eq!(whole.num_rows(), 8);
        for size in [1, 7, 61, 62, 100] {
            for stream in [false, true] {
                let batch = read(&options, PEOPLE, size, stream).unwrap();
                assert_eq!(batch, whole, "chunk size {size}, stream {stream}");
            }
        }
    }

    #[test]
    fn reports_where_a_float_fails() {
        let options = options(AMOUNTS.as_bytes());
        let bytes = b"ab     1.5\ncd    1,50\nef    2,50\n";
        for size in [1, 11, 100] {
            for stream in [false, true] {
                let err = read(&options, bytes, size, stream).unwrap_err();
                assert_eq!(
                    err.to_string(),
                    r#"test.txt: record 2, field "amount" (byte 17): "1,50" is not a Float64"#,
                    "chunk size {size}, stream {stream}"
                );
            }
        }
        // The bad value is shown in the file's encoding: 0xBD is ½ in cp1252.
        let err = read(&options, b"ab    1\xBD\n", 100, false).unwrap_err();
        assert!(
            err.to_string().ends_with(r#""1½" is not a Float64"#),
            "{err}"
        );
    }

    #[test]
    fn the_eof_marker_is_dropped_wherever_blocks_are_cut() {
        let options = options(AMOUNTS.as_bytes());
        for size in [1, 3, 4, 100] {
            for stream in [false, true] {
                let rows = |bytes| read(&options, bytes, size, stream).unwrap().num_rows();
                assert_eq!(rows(b"ab\n\x1A"), 1, "chunk size {size}, stream {stream}");
                assert_eq!(rows(b"ab\ncd\x1A"), 2, "chunk size {size}, stream {stream}");
                assert_eq!(rows(b"\x1A"), 1, "chunk size {size}, stream {stream}");
            }
        }
    }

    #[test]
    fn a_schema_with_no_fields_still_counts_records() {
        let options = options(br#"{"fields": []}"#);
        for stream in [false, true] {
            let batch = read(&options, b"a\nb\n", 1, stream).unwrap();
            assert_eq!((batch.num_columns(), batch.num_rows()), (0, 2));
        }
    }

    #[test]
    fn an_empty_unit_has_no_records() {
        let options = options(SCHEMA);
        for stream in [false, true] {
            let batch = read(&options, b"", 1, stream).unwrap();
            assert_eq!(batch.num_rows(), 0);
        }
    }
}
