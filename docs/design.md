# fwf rewrite: design decisions

Exported on 2026-09-26 from the [design doc](https://claude.ai/code/artifact/87dd86a4-6d4d-4772-ba14-dc201d729b60).

## Context and scope

fwf is a Rust reader for fixed-width files, shaped like the `csv` crate (streaming rows, serde) and polars' CSV reader (columnar, parallel, Arrow output). It is being rewritten from scratch: on 2026-09-26 `main` was reset to a branch cut from the initial commit. The earlier implementation is kept at tag `pre-rewrite` and is not a reference.

The target workflow is {zip, txt, stdin} → fwf → {csv, parquet, stdout}. Work starts on the input side; outputs come later.

## Decision log

Rows marked Decided or Dropped are the user's calls; rows marked Agreed were proposed and accepted without objection. Anything still unsettled is under Open questions.

| Decision | Status | What we chose | Why |
| --- | --- | --- | --- |
| Column types | Decided | No type guessing. A column with no declared type is a string. | Guessed types are silently wrong on FWF data (e.g. `01234` a ZIP code vs `0000123` a zero-filled amount). Also means input is read strictly forward, with no sample-and-replay. |
| Layout | Decided | Every layout gives each column's position and length, with 1-based start positions. It can also be written as start–end ranges or as a list of widths. Types are optional. | That is what the schemas we receive contain, so no width inference is needed. |
| Encodings | Decided | `utf-8` (default, strict), `cp1252`, `cp850`. | These cover the files we expect. All three are ASCII-compatible, so framing, trimming and number parsing work on raw bytes. |
| EBCDIC-037 | Dropped | Not supported. | Not needed. Dropping it removes the only encoding where space, digits and newline differ from ASCII. |
| Latin-1 | Dropped | Not supported; use `cp1252`. | Identical except bytes 0x80–0x9F, where Latin-1 has control codes that almost never occur in real data. |
| Scope order | Decided | Input surface first; outputs later. | Keeps the first slice small. |
| Architecture | Agreed | Shared core + row reader + columnar reader. | See Core architecture. |
| Input model | Agreed | Every input resolves to units of `Slice` or `Stream`. | See Input surface. |

## Core architecture

In FWF the layout gives field boundaries before any byte is read; only record boundaries are found by scanning. So there is no tokenizer, skipped columns cost nothing, parallel splits are always correct, and each column can be parsed in its own tight loop.

- **Core**, shared by everything:
  - `Layout`: columns with 0-based, half-open byte spans, trim, pad byte and null rule, plus an optional type (default: string). The builder accepts 1-based inclusive start–end, start + width, or a list of widths. Gaps and overlaps are allowed.
  - `Framing`: `Lines { terminator: Lf | CrLf | Auto }` or `Fixed { record_len }` for files with no line terminators.
  - `extract(line, &Column)` returns `Null` or `Value(&[u8])`, and applies the policy for lines shorter than the layout.
  - Byte-level parsers for declared types (integers, floats, implied decimals, fixed-position dates), one specialized loop per column.
- **Row reader and writer** (csv-crate shape): `Reader<R: Read>`, reusable `ByteRecord` and `StringRecord`, serde `deserialize`, and a `Writer` driven by the same `Layout`.
- **Columnar reader** (polars shape): split the input into chunks, find line starts, parse each requested column in its own loop per chunk (rayon), then join the chunks in order into Arrow `RecordBatch`es.

The columnar reader is not built on the row iterator, which would give up the per-column loop. Ship one crate with cargo features; split out a core crate only if something needs it on its own.

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
- Directories are skipped. Encrypted members and other compression methods are rejected with an error naming the member.

### Rules applied per unit

- Strip a UTF-8 BOM at the start; left in, it shifts every byte offset in the unit by three.
- Apply the header and `skip_rows` to each unit, because each file has its own header.
- Detect `\n` vs `\r\n` separately for each unit.
- Strip a single trailing `0x1A` (DOS end-of-file marker) after the last line ending.
- An empty unit yields zero rows, not an error.
- Errors carry the unit name, e.g. `data.zip!part-3.txt: record 1204 (byte 98765), column "amount"`.

### Encoding

The encoding only matters when a string field is converted to UTF-8.

- `utf-8` (default) is strict. Invalid bytes raise an error that gives the position and suggests `--encoding cp850` or `cp1252`.
- `cp1252` and `cp850`: bytes below 0x80 are ASCII, and bytes from 0x80 up come from a 128-entry table. Pure-ASCII fields are copied straight through. No crate is needed; `encoding_rs` does not include CP850.
- CP1252 leaves five bytes undefined (`0x81`, `0x8D`, `0x8F`, `0x90`, `0x9D`), and they raise an error. CP850 defines all 256.
- The tables are tested against a fixture generated from Python's codecs.
- A wrong code page fails silently: bytes `4A 6F 73 E9` read as `José` in CP1252 but `JosÚ` in CP850.
- With single-byte code pages, byte widths and character widths are the same; that question only arises for UTF-8.

### Command line

```
fwf convert [INPUT ...] --layout spec.toml
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
- **CSV:** `arrow_csv::Writer`. Add a row reader → `csv::Writer` fast path only if profiling shows the Arrow round trip matters.
- **Files:** write to a temp file and rename on success. Stdout can't be atomic; the exit code is the signal.
- **Stdout:** wrap in `BufWriter` (Rust's stdout flushes every line), exit quietly on `BrokenPipe`, and send everything except data to stderr.
- **In-memory use from Python or polars:** export through the Arrow C Stream interface (`__arrow_c_stream__`).

## Open questions

- [x] Are layout column positions always required, with no width inference? Yes: every layout gives positions and lengths; types are optional.
- [ ] Strict UTF-8 errors, or replace invalid bytes with `U+FFFD` and report a count? Recommended: strict.
- [ ] For UTF-8 input, do widths count bytes or characters? Recommended: bytes.
- [ ] Can one file hold several record types (header, detail, trailer keyed by a code at a fixed position)? Cheap to leave room for now, expensive to add later.
- [ ] What format is the `--layout` file? TOML is assumed but not designed.
- [ ] A zip arriving on a pipe: copy to a temp file (current plan), try to stream it, or reject it?
- [ ] Several inputs or zip members: one output, or one per unit? One output needs a source-name column.
- [ ] Default format on stdout: CSV, or require `--format`? Recommended: CSV.
- [x] Are layout start positions 1-based, as most codebooks write them, or 0-based? 1-based.

## Out of scope for v1

Each input item below fits into resolution steps 1–3 later without touching the parser.

- Type guessing and width inference.
- Parsing several units at once; it mostly helps zips with many deflated members.
- `--no-mmap`. Mapping can crash (SIGBUS) if the file is truncated mid-read, and network filesystems are another reason to want it.
- Loading a whole decompressed file into memory to get exact parallel splits.
- Nested archives, zip methods other than stored and deflate, encrypted zips, and standalone zstd or bzip2 files.
- EBCDIC, Latin-1 and other code pages.
- All output work (see Output surface).
