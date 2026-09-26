# fwf rewrite: design decisions

This file is the maintained design. It started on 2026-09-26 as an export of a [Claude Doc](https://claude.ai/code/artifact/87dd86a4-6d4d-4772-ba14-dc201d729b60), which is no longer updated.

## Context and scope

fwf is a Rust reader for fixed-width files, shaped like polars' CSV reader (columnar, parallel, Arrow output). It is being rewritten from scratch: on 2026-09-26 `main` was reset to a branch cut from the initial commit. The earlier implementation is kept at tag `pre-rewrite` and is not a reference.

The target workflow is {zip, gzip, txt, stdin} → fwf → {csv, parquet, stdout}. Work starts on the input side; outputs come later.

## Decision log

Rows marked Decided or Dropped are the user's calls; rows marked Agreed were proposed and accepted without objection. Anything still unsettled is under Open questions.

| Decision | Status | What we chose | Why |
| --- | --- | --- | --- |
| Column types | Decided | No type guessing. A column with no declared type is a string. | Guessed types are silently wrong on FWF data (e.g. `01234` a ZIP code vs `0000123` a zero-filled amount). Also means input is read strictly forward, with no sample-and-replay. |
| Layout | Decided | Every layout gives each field's position (1-based) and length. Types are optional. Start–end ranges and width lists are dropped for now. | That is what the schemas we receive contain, so no width inference is needed. |
| Schema file | Decided | JSON: `{"fields": [...]}`. Each field has `name`, `position` (1-based), `length` and an optional `type`; any other keys are ignored. | Chosen by the user. |
| Type names | Decided | Polars names. Only `String` and `Float64` for now. No `type` means `String`; an unknown `type` is an error. | Polars names map one-to-one to Arrow types and are what downstream users know. The error catches typos like `Flaot64`. |
| Width unit | Decided | Widths count characters, including in UTF-8. | Chosen by the user. In cp1252 and cp850 characters are bytes. |
| Containers | Decided | Plain text, gzip, and zip with stored, deflate or deflate64 members. | Requested by the user; deflate64 was added on 2026-09-26. |
| Encodings | Decided | `utf-8` (default, strict), `cp1252`, `cp850`. | These cover the files we expect. All three are ASCII-compatible, so framing, trimming and number parsing work on raw bytes. |
| EBCDIC-037 | Dropped | Not supported. | Not needed. Dropping it removes the only encoding where space, digits and newline differ from ASCII. |
| Latin-1 | Dropped | Not supported; use `cp1252`. | Identical except bytes 0x80–0x9F, where Latin-1 has control codes that almost never occur in real data. |
| Scope order | Decided | Input surface first; outputs later. | Keeps the first slice small. |
| Architecture | Agreed | Shared core + columnar reader, laid out like polars-io's `csv/read`. | See Core architecture and Crate layout. |
| Row reader | Dropped | No csv-crate-style row reader (serde, `ByteRecord`) or writer. | Nothing on the path to Arrow and Parquet needs it; add it only if something does. |
| Input model | Agreed | Every input resolves to units of `Slice` or `Stream`. | See Input surface. |

## Core architecture

In FWF the layout gives field boundaries before any byte is read; only record boundaries are found by scanning. So there is no tokenizer, skipped columns cost nothing, parallel splits are always correct, and each column can be parsed in its own tight loop.

- **Core**, shared by everything:
  - `Layout`: columns with 0-based, half-open spans counted in characters, trim, pad byte and null rule, plus an optional type (default: `String`). Each field is given as a 1-based position and a length. Gaps and overlaps are allowed.
  - `Framing`: `Lines { terminator: Lf | CrLf | Auto }` or `Fixed { record_len }` for files with no line terminators. `Fixed` cuts by bytes, so it needs cp1252, cp850 or pure-ASCII UTF-8.
  - `extract(line, &Column)` returns `Null` or `Value(&[u8])`, and applies the policy for lines shorter than the layout.
  - Byte-level parsers for declared types, one specialized loop per column. v1 has only `Float64`; integers, dates and decimals can come later.
- **Columnar reader** (polars shape): split the input into chunks, find line starts, parse each requested column in its own loop per chunk (rayon), then join the chunks in order into Arrow `RecordBatch`es.

There is no csv-crate-style row reader. Ship one crate with cargo features; split out a core crate only if something needs it on its own.

## Crate layout

One library crate, organized like polars-io's `csv/read` module. The command-line tool comes later, with the output work. Public names drop polars' `Csv` prefix (`fwf::ReadOptions`), which polars needs only because its prelude puts every format in one namespace.

```
src/
  lib.rs          re-exports; read() and scan()
  error.rs        Error + Position { unit, record, byte, field }
  schema.rs       JSON → Schema { fields }, DataType { String, Float64 }; unknown type = error
  options.rs      ReadOptions (schema, columns, skip_rows, n_rows, n_threads, chunk_size)
                  ParseOptions (encoding, trim, null rule, short-line policy)
  input/
    mod.rs        Location, Container, InputOptions, Unit, Input { Slice, Stream }, open()
    sniff.rs      first bytes → plain | gzip | zip
    zip.rs        index via the `zip` crate; stored → Slice, deflate/deflate64 → Stream; CRC-32 check
    stdin.rs      redirected file → mmap; pipe → Stream; zip on a pipe → temp file
  encoding.rs     Encoding { Utf8, Cp1252, Cp850 }: character span → byte range, bytes → UTF-8
  tables.rs       the two 128-entry code-page tables, generated from Python's codecs and checked in
  framing.rs      per unit: BOM, \n vs \r\n, trailing 0x1A, line starts (memchr), chunk boundaries
  field.rs        one field from one line: slice, trim, null, short line
  builder.rs      column builders → Arrow arrays
  read_impl.rs    parse_chunk(bytes) → RecordBatch; slice path (parallel) and stream path
  reader.rs       Reader: scan() → impl RecordBatchReader; read() = scan() collected
tests/
  fixtures/…
  read.rs         every fixture with people.schema.json == people.expected.json
```

| polars-io `csv/read/` | fwf | What changes |
| --- | --- | --- |
| `options.rs` | `options.rs` | No separator, quote or comment options. Adds encoding, trim, null rule and short-line policy. |
| `schema_inference.rs` | `schema.rs` | Reads the JSON schema; nothing is inferred. |
| `parser.rs` | `framing.rs` | Any `\n` ends a record, since nothing is quoted. Adds BOM, `\r\n` and `0x1A` handling. |
| `splitfields.rs` | `field.rs` | A field is a slice at a known position, then trim and null checks. |
| `builder.rs` | `builder.rs` | Only `String` and `Float64`. The `String` builder transcodes cp1252/cp850, not just validates UTF-8. |
| `read_impl.rs` | `read_impl.rs` | Same chunk-and-rayon shape; chunk boundaries never need checking. |
| `reader.rs` | `reader.rs` | Produces Arrow `RecordBatch`es, not a `DataFrame`. |
| `utils.rs` (`decompress`), `streaming.rs` | `input/` | Much larger: zip members, deflate64, stdin vs. pipe, units. |
| `CsvEncoding { Utf8, LossyUtf8 }` | `encoding.rs` | New: code pages, and character positions for UTF-8. |
| polars-stream `io_sources/csv/` | stream path in `read_impl.rs` | Same split between reading line batches and parsing them, without a query engine. |

**Read vs. scan.** Polars' `scan` builds a lazy plan for its query engine; we have none. `scan()` returns an iterator of `RecordBatch`es, with column selection and `n_rows` as plain options, and `read()` collects it. One `parse_chunk` does the parsing, fed two ways: in-memory units are split into chunks and parsed in parallel; streamed units are read a block at a time, cut at the last newline, with the rest carried into the next block.

**Dependencies:** `arrow-array` and `arrow-schema` (not the full `arrow` crate), `rayon`, `memchr`, `bytes`, `memmap2`, `flate2`, `zip` without default features (only its index reader), `deflate64`, `globset`, `serde` and `serde_json`.

**Build order**, each step checked against the fixtures:

1. `schema.rs`: load `people.schema.json`; reject `people.schema.unknown-type.json`.
2. `encoding.rs` + `tables.rs`: tables checked against Python; character-to-byte offsets for UTF-8.
3. `framing.rs`, `field.rs`, `builder.rs`, `read_impl.rs`, single-threaded, on plain `.txt`: all three `people.*.txt` decode to `people.expected.json`.
4. `input/`: gzip, zip (deflate and deflate64), stdin (pipe and redirect).
5. Parallel chunks.

## Input surface

Every input resolves to an ordered list of units, one per file or zip member. Each unit is an in-memory `Slice` or a `Stream`, and the parser only ever sees units.

```rust
pub enum Location  { Path(PathBuf), Stdin, Bytes(Bytes), Reader(Box<dyn Read + Send>) }
pub enum Container { Auto, Plain, Gzip, Zip }          // Auto = check the first bytes
pub enum Encoding  { Utf8, Cp1252, Cp850 }

pub struct InputOptions {
    container: Container,
    entries:   Option<GlobSet>,   // zip members; None = the only member, error if several
    encoding:  Encoding,
}

pub struct Unit  { pub name: String, pub input: Input }  // "a.txt", "-", "data.zip!part-3.txt"
pub enum   Input { Slice(Bytes), Stream(Box<dyn Read + Send>) }

pub fn open(loc: Location, opts: &InputOptions) -> Result<impl Iterator<Item = Result<Unit>>>;
```

Units own their data and are `Send + 'static`, so they can move to worker threads later without an API change.

### Resolution

1. **Get the raw bytes.** A path, or stdin redirected from a file (its metadata reports a regular file), is memory-mapped and wrapped with `Bytes::from_owner(mmap)`. A pipe on stdin, or a `Reader`, is wrapped in a `BufReader`. Empty files are special-cased, not mapped.
2. **Identify the format** from the first bytes unless `--input-format` is given: `PK\x03\x04` is zip, `1F 8B` is gzip, anything else is plain. File extensions are ignored.
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

- Stored (uncompressed) members become a `Slice` and get the fully parallel path.
- Deflated members become a `DeflateDecoder` over the sub-slice, which is a `Stream`. We check each member's CRC-32 at its end, since we bypass the crate's own reader.
- Deflate64 members work the same way, using the `deflate64` crate's decoder.
- Directories are skipped. Encrypted members and other compression methods are rejected with an error naming the member.

### Rules applied per unit

- Strip a UTF-8 BOM at the start; left in, it shifts every byte offset in the unit by three.
- Apply the header and `skip_rows` to each unit, because each file has its own header.
- Detect `\n` vs `\r\n` separately for each unit.
- Strip a single trailing `0x1A` (DOS end-of-file marker) after the last line ending.
- An empty unit yields zero rows, not an error.
- Errors carry the unit name, e.g. `data.zip!part-3.txt: record 1204 (byte 98765), column "amount"`.

### Encoding

All three encodings are ASCII-compatible, so framing, trimming, null checks and `Float64` parsing work on raw bytes. The encoding matters in two places: finding a field's byte range (UTF-8 only) and converting a `String` field to UTF-8. It is chosen once per chunk, and the per-column loop is generic over it.

- `utf-8` (default) is strict. Invalid bytes raise an error that gives the position and suggests `--encoding cp850` or `cp1252`.
- `cp1252` and `cp850`: bytes below 0x80 are ASCII, and bytes from 0x80 up come from a 128-entry table. Pure-ASCII fields are copied straight through. No crate is needed; `encoding_rs` does not include CP850.
- CP1252 leaves five bytes undefined (`0x81`, `0x8D`, `0x8F`, `0x90`, `0x9D`), and they raise an error. CP850 defines all 256.
- The tables are tested against a fixture generated from Python's codecs.
- A wrong code page fails silently: bytes `4A 6F 73 E9` read as `José` in CP1252 but `JosÚ` in CP850.
- Widths count characters. In cp1252 and cp850 that is the same as bytes. In UTF-8, a line with non-ASCII characters needs a character-to-byte offset map before slicing; pure-ASCII lines skip it.

### Command line

```
fwf convert [INPUT ...] --layout schema.json
  INPUT            file path, or '-' for stdin (default '-'); the shell expands globs
  --input-format   auto | plain | gzip | zip        (default auto)
  --entry GLOB     zip members to read, repeatable  (default: the only member)
  --encoding       utf-8 | cp1252 | cp850           (default utf-8, strict)
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

`tests/fixtures/generate.py` writes one set of records in every encoding (utf-8, cp1252, cp850) and container (`.txt`, `.txt.gz`, deflate `.zip`, deflate64 `.zip`). Stdin tests pipe or redirect the same files. Every data file must decode to `people.expected.json` (blank fields are null) with `people.schema.json`, whose fields carry an extra `description` key that readers must ignore. There, `amount` is `Float64`, `name` is `String` explicitly, and the other fields have no type. `people.schema.unknown-type.json` misspells `Float64` and must be rejected. The script needs `7z` for deflate64 and writes identical bytes on every run. `.gitattributes` marks the fixtures `-text`, so git never converts their line endings (it would add `\r` on a clone with `core.autocrlf=true`).

## Open questions

- [x] Are layout column positions always required, with no width inference? Yes: every layout gives positions and lengths; types are optional.
- [ ] Strict UTF-8 errors, or replace invalid bytes with `U+FFFD` and report a count? Recommended: strict.
- [x] For UTF-8 input, do widths count bytes or characters? Characters.
- [ ] Can one file hold several record types (header, detail, trailer keyed by a code at a fixed position)? Cheap to leave room for now, expensive to add later.
- [x] What format is the `--layout` file? JSON.
- [x] What values can a field's `type` take, and is an unknown `type` an error? `String` or `Float64` (polars names); none means `String`; unknown is an error.
- [x] Schema files give only position and length. Does the library API still take start–end ranges and widths lists? No, dropped for now.
- [ ] A zip arriving on a pipe: copy to a temp file (current plan), try to stream it, or reject it?
- [ ] Several inputs or zip members: one output, or one per unit? One output needs a source-name column.
- [ ] Default format on stdout: CSV, or require `--format`? Recommended: CSV.
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
- EBCDIC, Latin-1 and other code pages.
- All output work (see Output surface).
