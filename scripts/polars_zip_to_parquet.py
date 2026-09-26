#!/usr/bin/env python3
# /// script
# dependencies = ["polars>=1.44"]
# ///
"""Convert a zipped cp1252 or cp850 fixed-width file to Parquet with polars.

The Python pipeline to compare fwf against. Polars reads only UTF-8, so the
zip's only member is first decoded to a UTF-8 file in the temp directory
(about as big as the member), then read a line at a time, sliced into fields
and written with zstd level 3, as fwf does.

Usage: uv run scripts/polars_zip_to_parquet.py ZIP LAYOUT ENCODING OUT

LAYOUT is an fwf JSON schema. Values follow fwf's field rules: ASCII
whitespace is trimmed, a blank value is null, and a field with no type is a
String. Each byte decodes to one character, so the schema's byte positions
are polars' character offsets. Unlike fwf, a trailing DOS end-of-file byte
(0x1A) is read as one more record.
"""

import json
import sys
import tempfile
import zipfile
from pathlib import Path

import polars as pl

TYPES = {"String": pl.String, "Float64": pl.Float64}
ASCII_WHITESPACE = " \t\n\x0c\r"


def decode(zip_path, encoding, out):
    """Write the zip's only member to `out` as UTF-8."""
    with zipfile.ZipFile(zip_path) as z:
        (member,) = z.namelist()
        with z.open(member) as src, open(out, "wb") as dst:
            while block := src.read(1 << 20):
                dst.write(block.decode(encoding).encode("utf-8"))


def column(field):
    value = (
        pl.col("line")
        .str.slice(field["position"] - 1, field["length"])
        .str.strip_chars(ASCII_WHITESPACE)
    )
    value = pl.when(value != "").then(value)
    return value.cast(TYPES[field.get("type", "String")]).alias(field["name"])


def main():
    if len(sys.argv) != 5:
        sys.exit(__doc__)
    zip_path, layout, encoding, out = sys.argv[1:]
    fields = json.loads(Path(layout).read_text())["fields"]
    with tempfile.TemporaryDirectory() as tmp:
        utf8 = Path(tmp) / "member.txt"
        decode(zip_path, encoding, utf8)
        frame = pl.scan_lines(utf8).select([column(f) for f in fields])

        # Change the data here, before it's written. `frame` is a LazyFrame
        # with the schema's columns, already trimmed and typed. For example,
        # with tests/fixtures/people.schema.json:
        #
        #   frame = frame.drop("code")
        #   frame = frame.with_columns(pl.col("born").str.to_date("%Y%m%d"))
        #   frame = frame.with_columns(cents=(pl.col("amount") * 100).round().cast(pl.Int64))
        #   frame = frame.filter(pl.col("amount") > 0)
        #   frame = frame.rename({"id": "person_id"})

        frame.sink_parquet(out, compression="zstd", compression_level=3)


if __name__ == "__main__":
    main()
