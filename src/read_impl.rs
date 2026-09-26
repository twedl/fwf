use std::io::Read;
use std::ops::Range;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{Schema as ArrowSchema, SchemaRef};
use bytes::Bytes;
use rayon::prelude::*;

use crate::input::{Input, Unit};
use crate::{Field, Position, ReadOptions, Result, builder, framing, input};

/// How many chunks of an in-memory unit each thread parses at a time: enough
/// to even out uneven chunks, few enough that the batches waiting to be read
/// stay few.
const CHUNKS_PER_THREAD: usize = 4;

/// A block of a streamed unit's whole lines, read ahead to be parsed in
/// parallel with others.
struct Block {
    bytes: Vec<u8>,
    /// Where the block starts in its unit.
    offset: usize,
    /// The lines to parse: `take` of them, after the first `skip`.
    skip: usize,
    take: usize,
    /// The unit's lines before the first one parsed, skipped ones included.
    records_before: usize,
}

/// Parses a unit into record batches, one per chunk, as they're asked for.
/// Each chunk is filled a column at a time.
pub(crate) struct Parser {
    options: ReadOptions,
    /// The chosen fields, in output order.
    fields: Vec<Field>,
    schema: SchemaRef,
}

impl Parser {
    pub(crate) fn new(options: &ReadOptions) -> Parser {
        let all = options.schema.fields();
        let fields: Vec<Field> = match &options.columns {
            Some(columns) => columns.iter().map(|&i| all[i].clone()).collect(),
            None => all.to_vec(),
        };
        let schema = ArrowSchema::new(fields.iter().map(builder::arrow_field).collect::<Vec<_>>());
        Parser {
            options: options.clone(),
            fields,
            schema: Arc::new(schema),
        }
    }

    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// The unit's batches in order, each parsed when it's asked for. Nothing
    /// more is parsed after an error.
    pub(crate) fn batches(self, unit: Unit) -> impl Iterator<Item = Result<RecordBatch>> + Send {
        let mut batches: Box<dyn Iterator<Item = Result<RecordBatch>> + Send> = match unit.input {
            Input::Slice(bytes) => Box::new(self.parse_slice(unit.name, bytes)),
            Input::Stream(reader) => Box::new(self.parse_stream(unit.name, reader)),
        };
        let mut failed = false;
        std::iter::from_fn(move || {
            if failed {
                return None;
            }
            let batch = batches.next()?;
            failed = batch.is_err();
            Some(batch)
        })
    }

    /// Parses an in-memory unit's chunks in parallel, a few per thread at a
    /// time, in order.
    fn parse_slice(self, unit: String, bytes: Bytes) -> impl Iterator<Item = Result<RecordBatch>> {
        let body = framing::strip_eof_marker(&bytes, false);
        let start = framing::after_lines(body, self.options.skip_rows);
        let end = match self.options.n_rows {
            Some(n) => start + framing::after_lines(&body[start..], n),
            None => body.len(),
        };
        let chunks: Vec<_> = framing::chunks(&body[start..end], self.options.chunk_size)
            .map(|chunk| start + chunk.start..start + chunk.end)
            .collect();
        let threads = self.install(rayon::current_num_threads);
        let waves: Vec<_> = chunks
            .chunks(threads * CHUNKS_PER_THREAD)
            .map(<[_]>::to_vec)
            .collect();
        waves
            .into_iter()
            .flat_map(move |wave| self.parse_wave(&unit, &bytes, wave))
    }

    /// Parses chunks of an in-memory unit in parallel, keeping their order.
    fn parse_wave(
        &self,
        unit: &str,
        bytes: &[u8],
        chunks: Vec<Range<usize>>,
    ) -> Vec<Result<RecordBatch>> {
        self.install(|| {
            (chunks.into_par_iter())
                // Each worker reuses one line index from chunk to chunk.
                .map_init(Vec::new, |lines, chunk| {
                    lines.clear();
                    lines.extend(framing::lines(&bytes[chunk.clone()]));
                    // Skipped lines count too, so records are the unit's lines.
                    let records_before = || framing::lines(&bytes[..chunk.start]).count();
                    self.parse_chunk(unit, lines, chunk.start, records_before)
                })
                .collect()
        })
    }

    /// Parses a streamed unit a few blocks per thread at a time: the blocks are
    /// read one after another, each cut after its last line ending with the
    /// rest carried into the next, then parsed in parallel.
    fn parse_stream(
        self,
        unit: String,
        mut reader: Box<dyn Read + Send>,
    ) -> impl Iterator<Item = Result<RecordBatch>> {
        let size = self.options.chunk_size;
        let skip_rows = self.options.skip_rows;
        let mut buf = Vec::new();
        let mut offset = 0;
        // The unit's lines so far, skipped ones included.
        let mut records = 0;
        let mut rows_left = self.options.n_rows.unwrap_or(usize::MAX);
        let mut at_end = false;
        let name = unit.clone();
        let mut read_block = move || {
            while !at_end && rows_left > 0 {
                let read = (&mut reader).take(size as u64).read_to_end(&mut buf);
                let read = match read {
                    Ok(read) => read,
                    Err(e) => return Some(Err(input::io_error(&name)(e))),
                };
                at_end = read < size;
                let end = match framing::last_line_end(&buf) {
                    _ if at_end => buf.len(),
                    Some(end) => end,
                    // A line longer than a block: read more of it.
                    None => continue,
                };
                let mut rest = Vec::with_capacity(buf.len() - end + size);
                rest.extend_from_slice(&buf[end..]);
                buf.truncate(end);
                let mut bytes = std::mem::replace(&mut buf, rest);
                // Every block after the first follows the line ending it was cut at.
                bytes.truncate(framing::strip_eof_marker(&bytes, offset > 0).len());
                let lines = framing::lines(&bytes).count();
                let skip = skip_rows.saturating_sub(records).min(lines);
                let take = (lines - skip).min(rows_left);
                let block = Block {
                    bytes,
                    offset,
                    skip,
                    take,
                    records_before: records + skip,
                };
                records += skip + take;
                rows_left -= take;
                offset += end;
                if take > 0 {
                    return Some(Ok(block));
                }
            }
            None
        };
        let wave = self.install(rayon::current_num_threads) * CHUNKS_PER_THREAD;
        std::iter::from_fn(move || {
            let mut blocks = Vec::new();
            let mut failed = None;
            while blocks.len() < wave && failed.is_none() {
                match read_block() {
                    Some(Ok(block)) => blocks.push(block),
                    Some(Err(e)) => failed = Some(e),
                    None => break,
                }
            }
            if blocks.is_empty() && failed.is_none() {
                return None;
            }
            // A read error comes after the blocks read before it.
            let mut batches = self.parse_blocks(&unit, &blocks);
            batches.extend(failed.map(Err));
            Some(batches)
        })
        .flatten()
    }

    /// Parses a streamed unit's blocks in parallel, keeping their order.
    fn parse_blocks(&self, unit: &str, blocks: &[Block]) -> Vec<Result<RecordBatch>> {
        self.install(|| {
            (blocks.par_iter())
                // Each worker reuses one line index from block to block.
                .map_init(Vec::new, |lines, block| {
                    lines.clear();
                    let chosen = framing::lines(&block.bytes).skip(block.skip);
                    lines.extend(chosen.take(block.take));
                    self.parse_chunk(unit, lines, block.offset, || block.records_before)
                })
                .collect()
        })
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
        let columns = self.fields.iter().map(|field| {
            builder::column(field, lines, self.options.encoding, &|line, byte| {
                Position {
                    unit: unit.to_owned(),
                    record: records_before() + line + 1,
                    field: field.name.clone(),
                    byte: offset + byte,
                }
            })
        });
        let columns = columns.collect::<Result<Vec<_>>>()?;
        // The row count keeps a schema with no fields from failing.
        let batch_options = RecordBatchOptions::new().with_row_count(Some(lines.len()));
        let batch = RecordBatch::try_new_with_options(self.schema.clone(), columns, &batch_options);
        Ok(batch.expect("each column is built as its field's type"))
    }

    /// Runs `op` on the options' thread pool, or else rayon's global one.
    fn install<R: Send>(&self, op: impl FnOnce() -> R + Send) -> R {
        match &self.options.pool {
            Some(pool) => pool.install(op),
            None => op(),
        }
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

    /// Parses bytes as one slice or one stream, in chunks of `chunk_size`.
    fn batches(
        options: &ReadOptions,
        bytes: &[u8],
        chunk_size: usize,
        stream: bool,
    ) -> impl Iterator<Item = Result<RecordBatch>> {
        let input = if stream {
            Input::Stream(Box::new(Cursor::new(bytes.to_vec())))
        } else {
            Input::Slice(Bytes::copy_from_slice(bytes))
        };
        let unit = Unit {
            name: "test.txt".to_owned(),
            input,
        };
        Parser::new(&options.clone().with_chunk_size(chunk_size)).batches(unit)
    }

    /// Reads bytes as one slice or one stream, in chunks of `chunk_size`.
    fn read(
        options: &ReadOptions,
        bytes: &[u8],
        chunk_size: usize,
        stream: bool,
    ) -> Result<RecordBatch> {
        let schema = Parser::new(options).schema().clone();
        let batches = batches(options, bytes, chunk_size, stream).collect::<Result<Vec<_>>>()?;
        Ok(concat_batches(&schema, &batches).unwrap())
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

    #[test]
    fn skipped_lines_are_not_parsed_but_are_counted() {
        let options = options(AMOUNTS.as_bytes()).with_skip_rows(2);
        let bytes = b"name amount\n---- ------\nab     1.5\ncd    1,50\n";
        for size in [1, 11, 100] {
            for stream in [false, true] {
                let err = read(&options, bytes, size, stream).unwrap_err();
                assert_eq!(
                    err.to_string(),
                    r#"test.txt: record 4, field "amount" (byte 41): "1,50" is not a Float64"#,
                    "chunk size {size}, stream {stream}"
                );
                let rows = read(&options, &bytes[..35], size, stream)
                    .unwrap()
                    .num_rows();
                assert_eq!(rows, 1, "chunk size {size}, stream {stream}");
                let rows = read(&options, b"a\n", size, stream).unwrap().num_rows();
                assert_eq!(rows, 0, "chunk size {size}, stream {stream}");
            }
        }
    }

    #[test]
    fn nothing_past_n_rows_is_parsed() {
        let bytes = b"ab     1.5\ncd     2.5\nef    1,50\n";
        for size in [1, 11, 100] {
            for stream in [false, true] {
                let rows = |options: ReadOptions| {
                    let batch = read(&options, bytes, size, stream);
                    batch.unwrap().num_rows()
                };
                let options = options(AMOUNTS.as_bytes());
                assert_eq!(rows(options.clone().with_n_rows(0)), 0);
                assert_eq!(rows(options.clone().with_n_rows(2)), 2);
                assert_eq!(rows(options.with_skip_rows(1).with_n_rows(1)), 1);
            }
        }
        let options = options(SCHEMA).with_n_rows(100);
        assert_eq!(read(&options, PEOPLE, 7, true).unwrap().num_rows(), 8);
        assert_eq!(read(&options, PEOPLE, 7, false).unwrap().num_rows(), 8);
    }

    #[test]
    fn batches_before_an_error_come_first_and_none_after() {
        // One chunk per line, more lines than one wave holds.
        let mut bytes = b"ab     1.5\n".repeat(1000);
        bytes.extend(b"cd    1,50\nef     2.5\n");
        for stream in [false, true] {
            let results: Vec<_> =
                batches(&options(AMOUNTS.as_bytes()), &bytes, 1, stream).collect();
            assert_eq!(results.len(), 1001, "stream {stream}");
            assert!(results[..1000].iter().all(Result::is_ok), "stream {stream}");
            assert!(results[1000].is_err(), "stream {stream}");
        }
    }

    #[test]
    fn a_read_error_comes_after_the_blocks_before_it() {
        struct Broken;
        impl Read for Broken {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("broken"))
            }
        }
        // One 62-byte line per block, then the error.
        let reader = Cursor::new(PEOPLE).chain(Broken);
        let unit = Unit {
            name: "test.txt".to_owned(),
            input: Input::Stream(Box::new(reader)),
        };
        let options = options(SCHEMA).with_chunk_size(62);
        let results: Vec<_> = Parser::new(&options).batches(unit).collect();
        assert_eq!(results.len(), 9);
        assert!(results[..8].iter().all(Result::is_ok));
        assert!(matches!(results[8], Err(crate::Error::Io { .. })));
    }
}
