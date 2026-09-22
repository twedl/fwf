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

A JSON array of 4-element arrays — `[name, position, length, type]` — positional, not
keyed. This is what `json.dump` produces from a Python list of tuples, which is where
these come from. Order determines column order in the output.

An example, not a fixed layout — any set of fields is valid:

```json
[
  ["record_type",  1,  2,  "Char"],
  ["id",           4,  12, "Char"],
  ["region",       17, 3,  "Char"],
  ["description",  21, 40, "Char"],
  ["weight",       62, 13, "Num"],
  ["amount",       76, 13, "Num"]
]
```

| Position | Type   | Meaning                                                      |
| -------- | ------ | ------------------------------------------------------------ |
| 0        | string | Output column name. Must be unique.                           |
| 1        | int    | 1-based **character** offset of the field's first character.  |
| 2        | int    | Field length in **characters**.                               |
| 3        | string | `Char` or `Num`.                                              |

Any element that is not a 4-element array, or whose type is not exactly `Char` or `Num`,
is an error.

The schema need not cover the whole record, and typically does not — layouts often leave
a character or two between fields. Unlisted characters are simply not read. Overlapping
fields are also allowed; the same characters may feed two columns.

The implied record length is `max(position - 1 + length)` — 88 characters for the six
fields above. It is descriptive only; nothing validates a record against it.

### The encoding

Always explicit, never guessed. Of the four supported encodings only UTF-8 is detectable;
`cp1252`, `cp850`, and `latin1` accept any byte sequence and differ only in what those
bytes mean, so auto-detection between them is impossible in principle. Naming it is the
caller's responsibility.

### Characters and bytes

Positions and lengths are in **characters**. What that costs at runtime depends entirely on
the encoding, and the four split into two cases.

**`cp1252`, `cp850`, `latin1` — nothing to convert.** These are single-byte encodings: one
character is one byte for all 256 code points, with no exceptions. The schema's character
numbers are therefore *already* byte offsets. There is no conversion step and no scanning —
the record is sliced directly by byte index and each field decoded on its own. This is
exact, not an approximation or a heuristic.

**`utf8` — resolved per record.** Here the byte offset of character *N* depends on which
characters precede it, so no fixed conversion exists to precompute: the same schema position
lands at a different byte offset on every line. Each record is scanned once with
`char_indices` to locate the boundaries the schema asks for, then sliced.

One cheap win on that path — a record that happens to be entirely ASCII has byte offsets
equal to its character offsets, so a vectorized `is_ascii()` check lets such a line take the
direct-index route. In a mostly-ASCII UTF-8 file that is very nearly every line, which makes
the scanning cost proportional to how much non-ASCII text the file actually contains rather
than to its size.

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

Column types follow the schema: `Char` → Utf8, `Num` → Float64.

`Num` is Float64 rather than Int64 because that is what `cast(pl.Float64, strict=False)`
gives — it is fidelity, not a judgement call. It also happens to be lossless at realistic
field lengths: 13 characters tops out at 9,999,999,999,999, well under the 2^53 bound below
which Float64 holds every integer exactly. A `Num` field wider than 15 characters could
exceed that and lose precision. Cast to Int64 in Python if a column is known to be integral.

`Char` columns never contain null — a blank field is `""`. `Num` columns are the only
source of nulls. Both are written as nullable Parquet columns regardless.

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
record as a string:

```python
# Char
pl.col("raw").str.slice(position - 1, length).str.strip_chars().alias(name)

# Num
pl.col("raw").str.slice(position - 1, length).str.strip_chars()
  .cast(pl.Float64, strict=False).alias(name)
```

Everything below follows from that, and is stated only because it is easy to get wrong:

**`Char`** fields are trimmed with Rust's `str::trim` — which is what `strip_chars()` with
no pattern reduces to (`polars-ops` `namespace.rs:477`). An entirely blank field becomes
`""`, **not** null. A `Char` column is therefore never null.

**`Num`** fields are trimmed and then parsed as Float64. Empty or unparseable text becomes
null, matching `strict=False`. Null in a `Num` column is the only null this tool produces.

**Short lines** are kept as-is, with no length check at all. A field that begins past the
end of the record yields `""` — polars' `substring_ternary_offsets_value` returns an empty
range when the offset is out of bounds, rather than null. A field that is only partly
present yields the characters that are there. For `Num`, both cases then cast to null.

**Long lines** are kept and the trailing characters ignored.

**Line endings** may be LF or CRLF; a trailing `\r` is stripped before slicing. Mixed
endings within a file are fine. (`trim` would remove a stray `\r` from the final field
anyway, but stripping it keeps character positions honest.)

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
duplicate column name, unknown encoding, unwritable output path. Bad *values* are never a
failure — they become `""` or null per the rules above.

## Development

```sh
cargo test                      # unit tests plus end-to-end runs of the real binary
cargo build --release           # the binary lands in target/release/fwf
python3 scripts/gen_tables.py   # regenerate src/tables.rs from Python's codecs
```

`src/tables.rs` is generated and should not be hand-edited. The tests in `encoding.rs`
check the properties that matter — latin-1 is the identity mapping, cp1252 diverges from it
only across 0x80..=0x9F, and the three tables genuinely disagree in the high range — so a
regeneration that went wrong would not pass quietly.

Two pins worth knowing about. `polars` is held at 0.53 because the row-group and
string-slicing behaviour documented above was read out of that version's source; moving it
means re-checking those notes. `sysinfo`, which arrives transitively through `polars-utils`,
is pinned in `Cargo.lock` to the newest release that still builds on Rust 1.94 — newer ones
require 1.95.

## Non-goals

Deliberately absent, to keep this small:

- Schema inference, encoding detection, member globbing.
- Any type beyond `Char` and `Num` — no booleans, dates, or implied-decimal scaling, and
  no integer type. Cast in Python if needed.
- Column subsetting or renaming beyond what the schema already expresses.
- A separate UTF-8 path that slices via polars expressions rather than parsing by hand.
  The output would be identical — that is the point of the semantics above — so it would
  buy nothing but a second code path. Polars has no fixed-width reader, so it would mean a
  one-column frame plus `str.slice` per field, and its reader wants the member fully
  materialized to parallelize, which a single DEFLATE stream cannot offer anyway. All four
  encodings take the same path.
- Data-quality reporting of any kind: no value counts, no length warnings, no null tallies.
- Non-zip containers, non-Parquet outputs, multi-member runs.
- Configuration files, environment variables, or any input other than the five flags.
