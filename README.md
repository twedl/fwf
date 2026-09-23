# fwf

Extract one fixed-width member from a zip archive and write it to Parquet.

A single-purpose CLI, built to be called from Python via `subprocess`. Not published to
crates.io, not a library, not general-purpose.

## Scope

**Does:** read one named member of a zip archive as fixed-width text, slice each record
into fields according to a JSON schema, decode and type the values, and stream the result
to a Parquet file.

**Does not:** infer schemas, detect encodings, join or transform data, read anything but
zip, write anything but Parquet, or handle more than one member per invocation. Looping
over members is the caller's job.

## Usage

```
fwf --zip <PATH> --member <NAME> --schema <PATH> --encoding <ENC> --out <PATH>
```

| Argument     | Required | Meaning                                                      |
| ------------ | -------- | ------------------------------------------------------------ |
| `--zip`      | yes      | Path to the zip archive.                                      |
| `--member`   | yes      | Exact member name within the archive. No globbing.            |
| `--schema`   | yes      | Path to the JSON schema file (see below).                     |
| `--encoding` | yes      | One of `utf8`, `cp1252`, `cp850`, `latin1`.                   |
| `--out`      | yes      | Output Parquet path. Overwritten if it exists.                |

From Python:

```python
import subprocess, polars as pl

subprocess.run([
    "fwf",
    "--zip", "data.zip",
    "--member", "records.dat",
    "--schema", "schema.json",
    "--encoding", "cp1252",
    "--out", "records.parquet",
], check=True)

df = pl.read_parquet("records.parquet")
```

## Inputs

### The zip archive

Read through a streaming reader — the member is never decompressed to disk or held in
memory whole. Exactly one member is processed per run; the archive may contain any number
of others, which are ignored. A missing member is an error naming the members that are
present.

### The schema

A JSON array of objects, one per output column. It is an **output** schema: it declares
the columns of the Parquet file, in order, and where in the record each one is read from.
This is what `json.dump` produces from a Python list of dicts, which is where these come
from. Order determines column order in the output.

An example, not a fixed layout — any set of fields is valid:

```json
[
  {"name": "record_type", "at": [1, 2],   "type": "String"},
  {"name": "id",          "at": [4, 12],  "type": "String"},
  {"name": "region",      "at": [17, 3],  "type": "String"},
  {"name": "count",       "at": [21, 6],  "type": "Int64"},
  {"name": "opened",      "at": [28, 8],  "type": "Date", "format": "%Y%m%d"},
  {"name": "amount",      "at": [76, 13], "type": "Float64"},
  {"name": "retired",     "type": "Date", "format": "%Y%m%d"}
]
```

| Key      | Required           | Meaning                                                  |
| -------- | ------------------ | -------------------------------------------------------- |
| `name`   | yes                | Output column name. Must be unique.                       |
| `at`     | no                 | `[position, length]` — 1-based **character** offset of the field's first character, and its length in **characters**. Omitted, the column is never read. |
| `type`   | yes                | `String`, `Float64`, `Int64`, or `Date` — polars' own dtype names. |
| `format` | `Date` only        | A chrono/`strftime` pattern, such as `%Y%m%d`. Required on `Date`, rejected on every other type. |

Unknown keys, missing `name` or `type`, an unknown type, a duplicate name, a position or
length below 1, an `at` that is not exactly two numbers, a `Date` with no `format`, and a
`format` on a non-`Date` are each an error, reported before any output is written.

Because a column's name is simply the name it is given, renaming is not a separate
feature: write the name you want the Parquet column to have.

**A column with no `at`** is declared and typed but never read, and comes out all null.
That is what lets extracts of layouts that disagree about which fields exist share one
Parquet schema, so the files union cleanly — a field absent from one vintage is declared
there and null-filled, rather than missing from that file's schema.

The schema need not cover the whole record, and typically does not — layouts often leave
a character or two between fields. Unlisted characters are simply not read. Overlapping
fields are also allowed; the same characters may feed two columns.

The implied record length is `max(position - 1 + length)` over the fields that have an
`at` — 88 characters for the example above. It is descriptive only; nothing validates a
record against it.

### The encoding

Always explicit, never guessed. Of the four supported encodings only UTF-8 is detectable;
`cp1252`, `cp850`, and `latin1` accept any byte sequence and differ only in what those
bytes mean, so auto-detection between them is impossible in principle. Naming it is the
caller's responsibility.

### Characters and bytes

Positions and lengths are in **characters**, but a `&str` is indexed by bytes. So each
record is decoded into a buffer that is reused across rows, carrying a character-index to
byte-offset table alongside the text. Extracting a field is then an O(1) slice borrowed
from that buffer, rather than an allocation per field per row.

All four encodings take the same path; the encoding only decides how the buffer is filled.

- **An all-ASCII record** skips the offset table entirely — a character index *is* a byte
  offset there, so a vectorized `is_ascii()` check sends it down a direct-index route. In a
  mostly-ASCII file this is nearly every record, whichever encoding is declared.
- **`cp1252`, `cp850`, `latin1`** decode byte by byte through a 256-entry table, recording
  each character's offset as they go. One byte is always one character in these encodings,
  so the table is a plain byte-to-character map with no decoding state to carry.
- **`utf8`** walks `char_indices` for the boundaries. Here the byte offset of character *N*
  genuinely depends on what precedes it, so nothing can be precomputed across records.

There is an alternative for the three single-byte encodings: because their character numbers
are already byte offsets, the raw bytes could be sliced directly and only the fields the
schema asks for decoded. That does less work whenever a record is much wider than the fields
drawn from it. It is not what the code does, because avoiding an allocation per field then
needs a separate decode buffer, and for layouts where the fields cover most of the record —
the usual shape — the two are close. A layout that pulls a few short fields out of a very
wide record is the case that would justify revisiting it.

The reason to define the schema in characters rather than bytes is that it makes column
positions transcoding-invariant. A file in any single-byte encoding and the same file
converted to UTF-8 have different byte layouts but identical character layouts, so one schema
reads both and produces identical output. A schema never has to be adjusted for the encoding
it happens to be paired with — which matters because the encoding is a separate flag, and a
schema that silently depended on it would be an unpleasant way to lose an afternoon.

A character here is a Unicode scalar value — Rust's `char`. Text decoded from a single-byte
encoding is always precomposed, one scalar per source byte, so the count is unambiguous. Only
a genuinely UTF-8 source could contain a combining sequence where one apparent glyph counts
as two characters, and fixed-width data in practice does not.

## Output

A Parquet file at `--out`, zstd-compressed, written in row-group batches as records are
parsed so memory stays flat regardless of input size. An existing file at that path is
overwritten without prompting.

The write goes to a `.partial` file beside the destination and is renamed into place only
once the footer is written. A run that fails partway therefore leaves an existing file at
`--out` untouched, rather than having truncated it at startup — and never leaves a file
that has row groups but no footer, which no reader can open. The partial file is removed on
failure, and its name is unique, so concurrent runs against the same `--out` do not truncate
or delete each other's work.

Column types follow the schema, and there is nothing to look up: the schema's type names
**are** polars' dtype names, so `Float64` in the schema is `pl.Float64` in the frame. The
only one worth spelling out is `Date`, which is a Date32 — whole days from 1970-01-01,
negative before it.

The vocabulary is polars' rather than the fixed-width world's `Char`/`Num` on purpose.
Every type here is *defined* as the polars expression it reproduces (see Parsing
semantics), so naming them anything else would put a translation step between what the
schema says and what it does.

**Choosing between `Float64` and `Int64`** is the one real decision. `Float64` is lossless
at realistic field lengths — 13 characters tops out at 9,999,999,999,999, well under the
2^53 bound below which Float64 holds every integer exactly — but a numeric field wider than
15 characters could exceed that and lose precision. `Int64` is exact to 19 digits, and
rejects rather than rounds: `12.5` in an `Int64` column is null, not 12. Prefer `Int64` for
a column known to be integral, and `Float64` when a value may carry a decimal point.

A `String` column that is read never contains null — a blank field is `""`. `Float64`,
`Int64`, and `Date` columns are null wherever the text did not parse. A column with no `at`
is null throughout, whatever its type — so a `String` column *can* be null, but only that
way, never from a blank field. All columns are written as nullable Parquet columns
regardless.

### Row groups

Rows are parsed and written in batches of **262,144** — `512 * 512`, which is the default
`row_group_size` in `polars-io`'s `ParquetWriter::finish`. Matching it means the streamed
output has the same row-group layout as a straightforward in-memory write, rather than a
layout that happens to reflect this tool's buffering.

Worth knowing when touching this code: `ParquetWriter::batched()` does **not** inherit that
default. It drops `row_group_size` entirely, and `write_batch` emits one row group per
DataFrame chunk — so on the batched path the row group size *is* the parse batch size, and
each batch must be handed over as a single chunk. The constant is an inline literal in
polars, not an exported item, so it cannot be imported and may drift across versions;
it is worth re-checking on a polars upgrade.

## Parsing semantics

The rule is fidelity to polars. This tool exists because polars cannot decode cp1252,
cp850, or latin1 — not because it slices fixed-width fields differently. So every field is
defined to produce exactly what the equivalent polars expression would, given the same
record as a string. The schema's type names are these expressions' target dtypes, which is
why they are polars' names and not fixed-width ones:

```python
# String
pl.col("raw").str.slice(position - 1, length).str.strip_chars().alias(name)

# Float64
pl.col("raw").str.slice(position - 1, length).str.strip_chars()
  .cast(pl.Float64, strict=False).alias(name)

# Int64
pl.col("raw").str.slice(position - 1, length).str.strip_chars()
  .cast(pl.Int64, strict=False).alias(name)

# Date
pl.col("raw").str.slice(position - 1, length).str.strip_chars()
  .str.to_date(format, strict=False).alias(name)
```

Everything below follows from that, and is stated only because it is easy to get wrong:

**`String`** fields are trimmed with Rust's `str::trim` — which is what `strip_chars()` with
no pattern reduces to (`polars-ops` `namespace.rs:477`). An entirely blank field becomes
`""`, **not** null. A `String` column that is read is therefore never null.

**`Float64`** fields are trimmed and then parsed as a double. Empty or unparseable text
becomes null, matching `strict=False`.

**`Int64`** fields are trimmed and then parsed as i64. A decimal point, an exponent, or a
value past i64's range is unparseable and becomes null — an `Int64` field never rounds or
truncates to reach a number. `0042` is 42; `12.5` is null, not 12.

**`Date`** fields are trimmed and then parsed with the field's own `format`, a chrono
`strftime` pattern — the same patterns polars' `to_date` takes, since both go through
chrono. Text that does not match becomes null. The stored value is a Date32: whole days
from 1970-01-01, negative before it. Each `Date` field carries its own format, so one
member may hold date fields written different ways.

**A column with no `at`** reads nothing and is null in every row, whatever its type. This
is the only way a `String` column becomes null.

**Short lines** are kept as-is, with no length check at all. A field that begins past the
end of the record yields `""` — polars' `substring_ternary_offsets_value` returns an empty
range when the offset is out of bounds, rather than null. A field that is only partly
present yields the characters that are there. For `Float64`, `Int64`, and `Date`, both cases
then parse to null.

**Long lines** are kept and the trailing characters ignored.

**Line endings** may be LF or CRLF; a trailing `\r` is stripped before slicing. Mixed
endings within a file are fine. (`trim` would remove a stray `\r` from the final field
anyway, but stripping it keeps character positions honest.)

**A UTF-8 byte order mark** on the first record is stripped as bytes, before decoding. Left
in place it shifts every field of that record, and `trim` does not remove it — U+FEFF is not
whitespace — so it would otherwise reach Parquet and any join key downstream. Stripping it
pre-decode also covers a BOM'd file read as cp1252, where those three bytes would otherwise
become three visible characters.

**A DOS end-of-file marker** (`0x1A`) at the end of the member is dropped, whether it
trails the last record, sits on a line of its own, or falls between a carriage return and
the end of the member. Like the BOM it survives `trim`, and it is worth handling because
cp850 is the DOS codepage, so the two travel together. Only at the end of the member —
elsewhere `0x1A` is treated as data, since nothing marks it as anything else.

**Records must be newline delimited.** A member of fixed-length records run together with
no terminators is out of scope. Reads are bounded — 8× the widest the schema's last field
could occupy, or 64 KB, whichever is larger — so such a member fails once it passes that
bound, without ever being pulled into memory whole.

Below the bound it cannot be detected, and this is worth being plain about: a small member
with no terminators is indistinguishable from one long record, because records longer than
the schema's last field are legal and common. Such a member is read as a single row. The
bound is what makes the realistic case loud, since an export that lost its terminators is
not a few hundred bytes.

The `max(position - 1 + length)` record length noted earlier is documentation only. With no
length check and no warnings, nothing at runtime computes or consults it.

One caveat on fidelity: float parsing is Rust's `f64::from_str`, and polars' cast uses its
own fast float parser. They agree on everything decimal notation can express and disagree
only at the margins — the exact set of accepted spellings of infinity and NaN, for instance.
No fixed-width numeric field encodes those, so the difference is theoretical.

## Diagnostics and exit codes

One line goes to stderr on a successful run, so a `subprocess` call leaves a trace:

```
wrote records.parquet (1,204,331 rows)
```

No per-column tallies, no ragged-line warnings, nothing about data quality. A polars
expression pipeline reports none of that, and this tool reports none of it either.

The tradeoff is worth stating plainly: a schema with a wrong `position` produces a column
of `""` or nulls and says nothing. Detecting that is the caller's job, and is a
`null_count()` or `str.len_chars().max()` away once the Parquet is loaded — the same check
you would write against a natively-parsed frame.

Exit code is `0` on success and non-zero on failure, so `check=True` behaves. Failures are
things that make the run meaningless: unreadable zip, missing member, malformed schema,
duplicate column name, unknown encoding, unwritable output path, a member with no line
terminators, and text that is not valid UTF-8 under `--encoding utf8`. Bad *values* are
never a failure — they become `""` or null per the rules above.

## Development

```sh
cargo test                      # unit tests plus end-to-end runs of the real binary
cargo build --release           # the binary lands in target/release/fwf
python3 scripts/gen_tables.py   # regenerate src/tables.rs from Python's codecs
```

`src/tables.rs` is generated and should not be hand-edited. `encoding.rs` pins every one of
the 768 entries with a recorded FNV-1a digest per table, so any change from any cause — a
hand edit, a regeneration that came out different — fails the test and has to be
acknowledged deliberately by updating the digest. The property tests alongside it (latin-1
is the identity mapping, cp1252 diverges only across 0x80..=0x9F) say what the tables mean;
the digests are what make drift loud.

Two pins worth knowing about. `polars` is held at 0.53 because the row-group and
string-slicing behaviour documented above was read out of that version's source; moving it
means re-checking those notes. `sysinfo`, which arrives transitively through `polars-utils`,
is pinned in `Cargo.lock` to the newest release that still builds on Rust 1.94 — newer ones
require 1.95.

Two notes on the `Date` type. Polars gates the *logical* type rather than the enum variant,
so `DataType::Date` compiles without the `dtype-date` feature and panics on first use;
the feature is enabled in `Cargo.toml` and is not optional. And a `Date` column holds its
format pre-lexed as `Vec<Item>` rather than calling `NaiveDate::parse_from_str` per row,
which would rebuild `StrftimeItems` and re-lex the format string on every record: over 2M
rows that measured 140ms against 80ms, about 40% of the date column's cost. The pre-lexing
happens once per batch, in `ColumnBuilder::new`.

## Non-goals

Deliberately absent, to keep this small:

- Schema inference, encoding detection, member globbing.
- Any type beyond `String`, `Float64`, `Int64`, and `Date`. The names are polars' own, but
  the set is not open: `Boolean`, `Int32`, `Datetime` and the rest are rejected, because
  each would need its own documented parse rule and none has been asked for. No
  implied-decimal scaling either — scaling is arithmetic rather than a cast, so it has no
  polars string-cast equivalent to be faithful to. Do it in Python.
- Column subsetting or reordering beyond what the schema already expresses. (Renaming needs
  no feature: a column's name is whatever the schema calls it.)
- A separate UTF-8 path that slices via polars expressions rather than parsing by hand.
  The output would be identical — that is the point of the semantics above — so it would
  buy nothing but a second code path. Polars has no fixed-width reader, so it would mean a
  one-column frame plus `str.slice` per field, and its reader wants the member fully
  materialized to parallelize, which a single DEFLATE stream cannot offer anyway. All four
  encodings take the same path.
- Data-quality reporting of any kind: no value counts, no length warnings, no null tallies.
- Non-zip containers, non-Parquet outputs, multi-member runs.
- Configuration files, environment variables, or any input other than the five flags.
