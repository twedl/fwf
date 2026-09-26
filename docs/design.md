# fwf rewrite: design decisions

This file is the maintained design. It started on 2026-09-26 as an export of a [Claude Doc](https://claude.ai/code/artifact/87dd86a4-6d4d-4772-ba14-dc201d729b60), which is no longer updated.

## Context and scope

fwf is a Rust reader for cp1252 and cp850 fixed-width files, shaped like polars' CSV reader (columnar, parallel, Arrow output). For UTF-8 files, use polars directly. It is being rewritten from scratch: on 2026-09-26 `main` was reset to a branch cut from the initial commit. The earlier implementation is kept at tag `pre-rewrite` and is not a reference.

The target workflow is {zip, gzip, txt, stdin} → fwf → {csv, parquet, stdout}. Work starts on the input side; outputs come later.

## Decision log

Rows marked Decided or Dropped are the user's calls; rows marked Agreed were proposed and accepted without objection. Anything still unsettled is under Open questions.

| Decision | Status | What we chose | Why |
| --- | --- | --- | --- |
| Column types | Decided | No type guessing. A column with no declared type is a string. | Guessed types are silently wrong on FWF data (e.g. `01234` a ZIP code vs `0000123` a zero-filled amount). Also means input is read strictly forward, with no sample-and-replay. |
| Layout | Decided | Every layout gives each field's position (1-based) and length. Types are optional. Start–end ranges and width lists are dropped for now. | That is what the schemas we receive contain, so no width inference is needed. |
| Schema file | Decided | JSON: `{"fields": [...]}`. Each field has `name`, `position` (1-based), `length` and an optional `type`; any other keys are ignored. | Chosen by the user. |
| Type names | Decided | Polars names. Only `String` and `Float64` for now. No `type` means `String`; an unknown `type` is an error. | Polars names map one-to-one to Arrow types and are what downstream users know. The error catches typos like `Flaot64`. |
| Width unit | Decided | Widths count characters, which in cp1252 and cp850 are bytes. | Chosen by the user. UTF-8, where the two differ, was dropped on 2026-09-26. |
| Containers | Decided | Plain text, gzip, and zip with stored, deflate or deflate64 members. | Requested by the user; deflate64 was added on 2026-09-26. |
| Encodings | Decided | `cp1252` and `cp850`, with no default: the caller must choose. | These cover the files we expect. Both are single-byte and ASCII-compatible, so positions are byte offsets and framing, trimming and number parsing work on raw bytes. |
| UTF-8 | Dropped | Not supported; for UTF-8 files, use polars directly. | Dropped on 2026-09-26. Counting UTF-8 widths in characters was about 15% of the code, made accented UTF-8 read about 2× slower than cp1252, and ruled out filling a column at a time. |
| EBCDIC-037 | Dropped | Not supported. | Not needed. Dropping it removes the only encoding where space, digits and newline differ from ASCII. |
| Latin-1 | Dropped | Not supported; use `cp1252`. | Identical except bytes 0x80–0x9F, where Latin-1 has control codes that almost never occur in real data. |
| Scope order | Decided | Input surface first; outputs later. | Keeps the first slice small. |
| Architecture | Agreed | Shared core + columnar reader, laid out like polars-io's `csv/read`. | See Core architecture and Crate layout. |
| Row reader | Dropped | No csv-crate-style row reader (serde, `ByteRecord`) or writer. | Nothing on the path to Arrow and Parquet needs it; add it only if something does. |
| Input model | Agreed | Every input resolves to units of `Slice` or `Stream`. | See Input surface. |
| Parsing order | Decided | Chunks of about 1 MiB are filled a column at a time, in parallel. | Measured 26% faster on 6 fields × 61 characters and 43–44% on 40 fields × 200 characters; see Parsing order. |
| Benchmark | Dropped | The records-vs-columns benchmark and the record-at-a-time parser are not kept in the repo. | Dropped on 2026-09-26. The measurements under Parsing order stand as the record of the choice. |
| Joining chunks | Agreed | Each chunk becomes a `RecordBatch`; `read()` joins them in order with `arrow-select`'s `concat_batches`. | The same batches are what `scan()` will yield. |
| Parallel errors | Agreed | If several chunks fail, the error reported is the first in the unit. | Errors stay the same from run to run, whichever thread finds one first. |

## Core architecture

In FWF the layout gives field boundaries before any byte is read; only record boundaries are found by scanning. So there is no tokenizer, skipped columns cost nothing, and parallel splits are always correct.

- **Core**, shared by everything:
  - `Layout`: columns with 0-based, half-open byte spans, trim, pad byte and null rule, plus an optional type (default: `String`). Each field is given as a 1-based position and a length. Gaps and overlaps are allowed.
  - Framing: a record ends at `\n`, and a `\r` before it is dropped. Chunks are cut after a `\n`.
  - `field::value(line, start, len)` returns a field's trimmed bytes and where they start; an empty value is null, and a short line leaves later fields empty.
  - Byte-level parsers for declared types, one specialized loop per column. v1 has only `Float64`; integers, dates and decimals can come later.
- **Columnar reader** (polars shape): split each in-memory unit into chunks of about 1 MiB and parse them in parallel (rayon); read streamed units a block of the same size at a time. Each chunk becomes one Arrow `RecordBatch`, and `read()` joins them in order. Within a chunk, one pass indexes the lines, then each column is filled in its own loop: its type is chosen once and its builder stays in cache. Polars goes a record at a time instead.

There is no csv-crate-style row reader. Ship one crate with cargo features; split out a core crate only if something needs it on its own.

### Parsing order

Measured on 2026-09-26 with a scratch benchmark: Apple M1, one thread, cp1252, best of 10 runs with the two parsers alternating.

| Records | Chunking | Record at a time | Column at a time | Columns faster by |
| --- | --- | --- | --- | --- |
| people: 6 fields, 61 chars, 5M records (295 MB) | whole file | 623 ms | 519 ms | 17% |
| people | 1–4 MB chunks | 604–609 ms | 446–448 ms | 26% |
| wide: 40 fields (24 String, 16 Float64), 200 chars, 1.5M records (287 MB) | whole file | 1,220 ms | 1,220 ms | 0% |
| wide | 256 KB chunks | 1,205 ms | 690 ms | 43% |
| wide | 1–4 MB chunks | 1,200–1,214 ms | 674–682 ms | 43–44% |
| wide | 16 MB chunks | 1,198 ms | 833 ms | 30% |

Columns win because each column's type is chosen once and its builder stays in cache, so wider records gain more. Without chunking, every column pass rereads the whole input from memory and the gain disappears; chunks larger than the L2 cache (12 MB on the M1) lose some of it. The column prototype skipped error positions; the real one keeps them by storing each line's start during the indexing pass. The benchmark was a scratch program and is not kept in the repo.

After step 5, on a generated file of 40 fields (24 String, 16 Float64) × 200 characters, 1.5M records (301 MB), with a fifth of the String values non-ASCII (Apple M1 with 8 cores, best of 5): `read()` took 1,167 ms before step 5, 888 ms on one thread and 208 ms on all eight. The same file gzipped took 1,963 ms before and 1,724 ms after, on any number of threads, because its blocks are decompressed and parsed one after another. Joining the chunks took about 30 ms of that.

## Crate layout

One library crate, organized like polars-io's `csv/read` module. The command-line tool comes later, with the output work. Public names drop polars' `Csv` prefix (`fwf::ReadOptions`), which polars needs only because its prelude puts every format in one namespace.

```
src/
  lib.rs          re-exports; read() and scan()
  error.rs        Error + Position { unit, record, byte, field }
  schema.rs       JSON → Schema { fields }, DataType { String, Float64 }; unknown type = error
  options.rs      ReadOptions: schema, encoding, container, entries; later columns, skip_rows,
                  n_rows, n_threads, chunk_size. A separate ParseOptions waits until trim, null
                  or short-line behaviour becomes configurable.
  input/
    mod.rs        Location, Container, Unit, Input { Slice, Stream }, open(); sniffing the first
                  bytes; mapping files; stdin (redirect → map, pipe → stream); zip on a pipe →
                  temp file
    zip.rs        index via the `zip` crate; stored → Slice, deflate/deflate64 → Stream; CRC-32 check
  encoding.rs     Encoding { Cp1252, Cp850 }: field bytes → UTF-8 through the tables
  tables.rs       the two 128-entry code-page tables, generated by scripts/gen_tables.py and checked in
  framing.rs      per unit: trailing 0x1A, \n vs \r\n, line starts (memchr), chunk boundaries
  field.rs        one field's trimmed value on a line, and where it starts
  builder.rs      column builders → Arrow arrays (String → Utf8, Float64 → Float64, all nullable)
  read_impl.rs    Parser: in-memory units split into chunks parsed in parallel, streams read a
                  block at a time; each chunk filled a column at a time into one RecordBatch
  reader.rs       read(location) → one RecordBatch for all units (concat_batches); later
                  scan() → RecordBatchReader
tests/
  fixtures/…
  read.rs         every fixture with people.schema.json == people.expected.json
scripts/
  gen_tables.py   writes src/tables.rs from Python's codecs
```

| polars-io `csv/read/` | fwf | What changes |
| --- | --- | --- |
| `options.rs` | `options.rs` | No separator, quote or comment options. Adds encoding, trim, null rule and short-line policy. |
| `schema_inference.rs` | `schema.rs` | Reads the JSON schema; nothing is inferred. |
| `parser.rs` | `framing.rs` | Any `\n` ends a record, since nothing is quoted. Adds `\r\n` and `0x1A` handling. |
| `splitfields.rs` | `field.rs` | A field is a slice at a known position, then trim and null checks. |
| `builder.rs` | `builder.rs` | Only `String` and `Float64`. The `String` builder transcodes cp1252/cp850; polars' only validates UTF-8. |
| `read_impl.rs` | `read_impl.rs` | Same chunk-and-rayon shape, but filling a column at a time where polars goes a record at a time; chunk boundaries never need checking. |
| `reader.rs` | `reader.rs` | Produces Arrow `RecordBatch`es, not a `DataFrame`. |
| `utils.rs` (`decompress`), `streaming.rs` | `input/` | Much larger: zip members, deflate64, stdin vs. pipe, units. |
| `CsvEncoding { Utf8, LossyUtf8 }` | `encoding.rs` | The opposite set: cp1252 and cp850, no UTF-8. |
| polars-stream `io_sources/csv/` | stream path in `read_impl.rs` | Same split between reading line batches and parsing them, without a query engine. |

**Read vs. scan.** Polars' `scan` builds a lazy plan for its query engine; we have none. `scan()` returns an iterator of `RecordBatch`es, with column selection and `n_rows` as plain options, and `read()` collects it. One `Parser::parse_chunk` does the parsing, fed two ways: in-memory units are split into chunks and parsed in parallel; streamed units are read a block at a time, cut at the last newline, with the rest carried into the next block.

**Dependencies:** `arrow-array`, `arrow-schema` and `arrow-select` (not the full `arrow` crate), `memchr`, `bytes`, `memmap2`, `flate2`, `zip` without default features (only its index reader), `deflate64`, `crc32fast`, `globset`, `tempfile`, `serde`, `serde_json` and `rayon`.

**Build order**, each step checked against the fixtures. Steps 1–5 are done (as of 2026-09-26); 6 onward is the plan.

1. Done. `schema.rs`: load `people.schema.json`; reject `people.schema.unknown-type.json`.
2. Done. `encoding.rs` + `tables.rs`: tables checked against Python.
3. Done. `framing.rs`, `field.rs`, `builder.rs`, `read_impl.rs`, single-threaded, on plain `.txt`: both `people.*.txt` decode to `people.expected.json`.
4. Done. `input/`: gzip, zip (stored, deflate and deflate64), stdin (pipe and redirect), bytes and readers.
5. Done. Chunked, parallel, column-at-a-time parsing: in-memory units in chunks of about 1 MiB cut after a line ending, streams a block at a time with the rest of the last line carried over, and error positions kept exact across chunks. Unit tests parse the fixtures with chunks as small as one byte.
6. **`scan()` and read options.**
    - `scan(location, &options)` returns an Arrow `RecordBatchReader` yielding one batch per chunk; `read()` collects it. Streams already read a block at a time, but `read()` holds every batch until the end; `scan()` bounds memory on the output side too.
    - Add `ReadOptions` for column selection, `n_rows`, `skip_rows` / header lines (applied per unit) and chunk size / thread count.
    - Decide the open question on several units: one output (maybe with a source-name column) or one per unit.
7. **Outputs.** CSV (`arrow-csv`) and Parquet (`parquet::arrow::ArrowWriter`) to a file or stdout, per Output surface: temp file + rename for files, `BufWriter` and quiet `BrokenPipe` handling for stdout, no binary output to a terminal without `--force`.
8. **Command line.** `fwf convert [INPUT ...] --layout schema.json --encoding cp1252|cp850 [--input-format] [--entry GLOB] [-o OUTPUT] [--format csv|parquet]`, stdin and stdout by default, progress and errors on stderr. Behind a cargo feature so library users don't compile `clap`.
9. **Hardening and extras**, as needed:
    - Edge-case fixtures: `\r\n`, a trailing `0x1A`, header rows, short lines, blank lines and a multi-member gzip.
    - Python / polars: export batches through the Arrow C Stream interface, or a polars IO plugin (`register_io_source`) for `scan_fwf`.
    - More types (`Int64`, `Date`, `Decimal`), `--no-mmap`, and several record types per file (still an open question).
    - Performance, if profiling points there: parsing a stream's blocks in parallel while the next is decompressed, the stored-member CRC pass at open (it delays the first chunk), and a faster `Float64` parser.

## Input surface

Every input resolves to an ordered list of units, one per file or zip member. Each unit is an in-memory `Slice` or a `Stream`, and the parser only ever sees units.

```rust
pub enum Location  { Path(PathBuf), Stdin, Bytes(Bytes), Reader(Box<dyn Read + Send>) }  // From<&str>, &Path, PathBuf
pub enum Container { Auto, Plain, Gzip, Zip }          // Auto = check the first bytes

// ReadOptions::new(schema, encoding).with_container(..).with_entries(["parts/*.txt"])?

pub(crate) struct Unit  { pub name: String, pub input: Input }  // "a.txt", "-", "data.zip!parts/1.txt"
pub(crate) enum   Input { Slice(Bytes), Stream(Box<dyn Read + Send>) }

pub(crate) fn open(location: Location, container: Container, entries: Option<&GlobSet>) -> Result<Vec<Unit>>;
pub fn read(location: impl Into<Location>, options: &ReadOptions) -> Result<RecordBatch>;
```

- `Unit` and `Input` stay crate-private until `scan()` needs them. `open` returns a `Vec`: zip units are slices of one mapped archive, so there are no file handles to hold open.
- `read` parses every unit into one `RecordBatch`, in order, counting records from each unit's start in errors. A `Stream` unit is read a block at a time.
- Units own their data and are `Send + 'static`, so they can move to worker threads later without an API change.
- In-memory inputs are named `<bytes>` and `<reader>`; stdin is `-`.

### Resolution

1. **Get the raw bytes.** A path, or stdin redirected from a file (its metadata reports a regular file), is memory-mapped and wrapped with `Bytes::from_owner(mmap)`. A pipe on stdin, or a `Reader`, stays a stream. An empty file maps to an empty slice.
2. **Identify the format** from the first bytes unless a container is given (`with_container`, later `--input-format`): `PK\x03\x04` is zip, `1F 8B` is gzip, anything else is plain. A stream's first bytes are put back in front of it. File extensions are ignored.
3. **Split into units** as in the table below.

| Source | plain | gzip | zip |
| --- | --- | --- | --- |
| file, or `< file` | `Slice` | `Stream` | one unit per member, read from the mapped archive |
| pipe | `Stream` | `Stream` | copy to a temp file, then handle as a file |
| `Bytes` (library) | `Slice` | `Stream` | handle as a file, over the bytes |
| `Read` (library) | `Stream` | `Stream` | copy to a temp file, then handle as a file |

Gzip always uses `MultiGzDecoder`, so concatenated gzip files are read completely.

### Zip

The `zip` crate is used only to read the archive's index. Each member's bytes are a zero-copy sub-slice of the mapped archive.

- Stored (uncompressed) members become a `Slice` and get the fully parallel path. Their CRC-32 is checked when they're opened.
- Deflated members become a `DeflateDecoder` over the sub-slice, which is a `Stream`. We check each member's size and CRC-32 when its stream ends, since we bypass the crate's own reader.
- Deflate64 members work the same way, using the `deflate64` crate's decoder.
- Directories are skipped. Encrypted members and other compression methods are rejected with an error naming the member and the method number.
- Data that fails its size or CRC-32 is an `Io` error naming the member; `Archive` errors cover the index, encryption, unsupported methods and choosing members.
- Without entries, an archive must hold exactly one file; otherwise the error lists them. Entry globs use `globset` defaults, so `*` also matches `/`. Matching nothing is an error.

### Rules applied per unit

- Apply the header and `skip_rows` to each unit, because each file has its own header.
- A record ends at `\n`, and a `\r` before it is dropped, so `\n`, `\r\n` and mixed endings all work. A final line ending adds no record; a blank line is a record whose fields are all null.
- Strip a single trailing `0x1A` (DOS end-of-file marker) after the last line ending.
- An empty unit yields zero rows, not an error.
- A field's value is trimmed of ASCII whitespace at both ends; an empty value is null. On a short line, fields past its end are null and a field it cuts off keeps what's there.
- `Float64` values use Rust's `f64` parser, so `1e5`, `+1.5`, `inf` and `NaN` are accepted and `1,50` is an error.
- If several chunks of a unit fail, the error reported is the first in the unit.
- Errors carry a `Position`: the unit name, the 1-based record, the field and the byte offset of the problem in the unit, e.g. `data.zip!part-3.txt: record 1204, field "amount" (byte 98765): "1,50" is not a Float64`.

### Encoding

Both encodings are single-byte and ASCII-compatible, so a character is one byte, field positions are byte offsets, and framing, trimming, null checks and `Float64` parsing work on raw bytes. The encoding matters only when a `String` field is converted to UTF-8 for Arrow. `Encoding` is a plain enum passed into the parsing loop.

- There is no default: `ReadOptions::new(schema, encoding)` requires one, because cp1252 and cp850 both decode almost any byte and a wrong guess would be silent.
- Bytes below 0x80 are ASCII, and bytes from 0x80 up come from a 128-entry table. Pure-ASCII fields are borrowed as they are. No crate is needed; `encoding_rs` does not include cp850.
- cp1252 leaves five bytes undefined (`0x81`, `0x8D`, `0x8F`, `0x90`, `0x9D`), and they raise an error. In cp850 those bytes are ü, ì, Å, É and Ø, so the error asks "is the file cp850?". cp850 defines all 256.
- `scripts/gen_tables.py` writes `src/tables.rs` from Python's codecs. Tests check sample bytes, the five undefined cp1252 bytes, and that both fixtures decode to the same text.
- Otherwise a wrong code page fails silently: bytes `4A 6F 73 E9` read as `José` in cp1252 but `JosÚ` in cp850.
- UTF-8 files are out of scope; use polars directly.

### Command line

```
fwf convert [INPUT ...] --layout schema.json
  INPUT            file path, or '-' for stdin (default '-'); the shell expands globs
  --input-format   auto | plain | gzip | zip        (default auto)
  --entry GLOB     zip members to read, repeatable  (default: the only member)
  --encoding       cp1252 | cp850                   (required)
```

`-` may appear at most once. Only `--entry` needs our own glob matching (`globset`). Units are processed one after another, each parsed in parallel internally.

## Output surface (deferred)

Not started. These are notes from the discussion to pick up later.

- **Contract:** the parser produces an ordered stream of Arrow `RecordBatch`es (arrow's `RecordBatchReader`). Every destination consumes it. Custom destinations use it directly, so there is no `Sink` trait.
- **Format vs. destination:** `OutKind { Csv, Parquet }` × `OutLoc { Path, Stdout }`. Both formats are generic over `W: Write + Send`.
- **Parquet:** `parquet::arrow::ArrowWriter`. It works on stdout because the footer is written last. Refuse binary output to a terminal unless `--force`.
- **CSV:** `arrow_csv::Writer`. Add a direct fast path that skips Arrow only if profiling shows the round trip matters.
- **Files:** write to a temp file and rename on success. Stdout can't be atomic; the exit code is the signal.
- **Stdout:** wrap in `BufWriter` (Rust's stdout flushes every line), exit quietly on `BrokenPipe`, and send everything except data to stderr.
- **In-memory use from Python or polars:** export through the Arrow C Stream interface (`__arrow_c_stream__`).

## Test fixtures

`tests/fixtures/generate.py` writes one set of records in every encoding (cp1252, cp850) and container (`.txt`, `.txt.gz`, deflate `.zip`, deflate64 `.zip`), plus `people.cp1252.stored.zip`, `people.cp1252.lzma.zip` (a method to reject) and `people.multi.zip` (a directory, `parts/1.txt`, `parts/2.txt` and `README.txt`, for choosing members). Stdin was checked by hand with pipes and redirects; the tests cover a pipe and a mapped file directly. Every data file must decode to `people.expected.json` (blank fields are null) with `people.schema.json`, whose fields carry an extra `description` key that readers must ignore. There, `amount` is `Float64`, `name` is `String` explicitly, and the other fields have no type. `people.schema.unknown-type.json` misspells `Float64` and must be rejected. The script needs `7z` for deflate64 and writes identical bytes on every run. `.gitattributes` marks the fixtures `-text`, so git never converts their line endings (it would add `\r` on a clone with `core.autocrlf=true`).

## Open questions

- [x] Are layout column positions always required, with no width inference? Yes: every layout gives positions and lengths; types are optional.
- [ ] Can one file hold several record types (header, detail, trailer keyed by a code at a fixed position)? Cheap to leave room for now, expensive to add later.
- [x] What format is the `--layout` file? JSON.
- [x] What values can a field's `type` take, and is an unknown `type` an error? `String` or `Float64` (polars names); none means `String`; unknown is an error.
- [x] Schema files give only position and length. Does the library API still take start–end ranges and widths lists? No, dropped for now.
- [x] A zip arriving on a pipe: copy to a temp file, try to stream it, or reject it? Copy to a temp file (step 4).
- [ ] Several inputs or zip members: one output, or one per unit? `read` returns one batch for all units in order; the output side still needs deciding, and one output may want a source-name column.
- [ ] Default format on stdout: CSV, or require `--format`? Recommended: CSV.
- [ ] `String` columns are Arrow `Utf8`, whose 32-bit offsets cap a column at 2 GiB of text. `read()` panics past that when it joins the chunks. Switch to `LargeUtf8` or `Utf8View` (polars' own string type), or leave large files to `scan()`?
- [x] Are layout start positions 1-based, as most codebooks write them, or 0-based? 1-based.

## Out of scope for v1

Each input item below fits into resolution steps 1–3 later without touching the parser.

- Type guessing and width inference.
- A csv-crate-style row reader (serde, `ByteRecord`) and writer.
- The command-line tool, until the output work starts.
- Types other than `String` and `Float64` (integers, dates, decimals).
- Layouts written as start–end ranges or width lists.
- Parsing several units at once; it mostly helps zips with many deflated members.
- `--no-mmap`. Mapping can crash (SIGBUS) if the file is truncated mid-read, and network filesystems are another reason to want it.
- Loading a whole decompressed file into memory to get exact parallel splits.
- Nested archives, zip methods other than stored, deflate and deflate64, encrypted zips, and standalone zstd or bzip2 files.
- UTF-8 (use polars directly), EBCDIC, Latin-1 and other code pages.
- All output work (see Output surface).
