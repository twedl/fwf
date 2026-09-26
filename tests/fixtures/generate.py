#!/usr/bin/env python3
"""Generate the FWF test fixtures in this directory.

One set of records (ROWS) is written in every combination of encoding
(cp1252, cp850) and container (plain .txt, .txt.gz, deflate .zip,
deflate64 .zip). Stdin tests pipe or redirect these same files.

Both encodings use one byte per character, so widths are bytes.

Every data file decodes to the records in people.expected.json, where a
blank field is null, using the schema in people.schema.json. Each schema
field has a name, a 1-based position and a length, plus a description that
readers must ignore. `amount` is Float64, `name` is String explicitly, and
the rest have no type, so they default to String.

people.schema.unknown-type.json misspells Float64; readers must reject it.

Needs 7z (p7zip) for the deflate64 zips, since Python's zipfile can't write
them. Output is deterministic: running this again leaves git clean.
"""

import gzip
import json
import os
import shutil
import subprocess
import tempfile
import zipfile
import zlib
from pathlib import Path

HERE = Path(__file__).resolve().parent

ENCODINGS = ["cp1252", "cp850"]

# (name, width, alignment, type, description): "<" pads on the right, ">" on
# the left. A type of None leaves "type" out of the schema.
COLUMNS = [
    ("id", 6, "<", None, "Record id, zero-padded"),
    ("name", 20, "<", "String", "Full name"),
    ("city", 15, "<", None, "City of residence; blank if unknown"),
    ("born", 8, "<", None, "Date of birth, YYYYMMDD"),
    ("amount", 10, ">", "Float64", "Balance, right-aligned"),
    ("code", 2, "<", None, "Country code; blank if unknown"),
]

# Only characters that exist in both cp1252 and cp850. None = blank field.
ROWS = [
    ("000001", "José García", "Málaga", "19850312", "1234.50", "ES"),
    ("000002", "Zoë Müller", "Zürich", "19900101", "99.99", "CH"),
    ("000003", "Françoise Lefèvre", "Besançon", "19771231", "0.00", "FR"),
    ("000004", "Åsa Ström", "Göteborg", "20010704", "1000000.00", "SE"),
    ("000005", "Þór Guðmundsson", "Reykjavík", "19650228", "-42.10", "IS"),
    ("000006", "Maximilian Straßberg", "Düsseldorf", "19881111", "7.00", "DE"),
    ("000007", "Ana Brandão", None, "19930615", "350.25", "PT"),
    ("000008", "John Smith", "Leeds", "19991231", "12.00", None),
]

# Zip timestamps are local time without a zone, so pin both the time and TZ.
ZIP_TIME = (2026, 1, 1, 0, 0, 0)
ZIP_EPOCH = 1767225600  # 2026-01-01T00:00:00Z


def render_line(row):
    fields = []
    for (name, width, align, _, _), value in zip(COLUMNS, row, strict=True):
        value = value or ""
        assert len(value) <= width, f"{name}={value!r} is wider than {width}"
        fields.append(f"{value:{align}{width}}")
    return "".join(fields) + "\n"


def write_json_lines(path, head, items, tail):
    """One item per line, so diffs show which column or row changed."""
    body = ",\n".join("  " + json.dumps(item, ensure_ascii=False) for item in items)
    path.write_text(f"{head}\n{body}\n{tail}\n", encoding="utf-8")


def schema_fields(rename_type=None):
    fields = []
    position = 1
    for name, width, _, type_, description in COLUMNS:
        field = {"name": name, "position": position, "length": width}
        if type_:
            field["type"] = (rename_type or {}).get(type_, type_)
        field["description"] = description
        fields.append(field)
        position += width
    return fields


def write_schemas():
    write_json_lines(HERE / "people.schema.json", '{"fields": [', schema_fields(), "]}")
    typo = schema_fields(rename_type={"Float64": "Flaot64"})
    write_json_lines(HERE / "people.schema.unknown-type.json", '{"fields": [', typo, "]}")


def write_expected():
    records = []
    for row in ROWS:
        record = {}
        for (name, _, _, type_, _), value in zip(COLUMNS, row, strict=True):
            if value is not None and type_ == "Float64":
                value = float(value)
            record[name] = value
        records.append(record)
    write_json_lines(HERE / "people.expected.json", "[", records, "]")


def write_deflate_zip(path, member, data):
    info = zipfile.ZipInfo(member, date_time=ZIP_TIME)
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o100644 << 16
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr(info, data)


def write_deflate64_zip(path, member, data):
    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp, member)
        src.write_bytes(data)
        src.chmod(0o644)
        os.utime(src, (ZIP_EPOCH, ZIP_EPOCH))
        subprocess.run(
            ["7z", "a", "-tzip", "-mm=Deflate64", "-mtc=off", "-bso0", "-bsp0", "out.zip", member],
            cwd=tmp,
            env={**os.environ, "TZ": "UTC"},
            check=True,
        )
        shutil.move(Path(tmp, "out.zip"), path)


def check_zip(path, member, data, method):
    """Python can't decompress deflate64, so check its method, size and CRC."""
    with zipfile.ZipFile(path) as zf:
        [info] = zf.infolist()
        assert info.filename == member, info.filename
        assert info.compress_type == method, f"{path.name}: method {info.compress_type}"
        assert info.file_size == len(data) and info.CRC == zlib.crc32(data), path.name
        if method == zipfile.ZIP_DEFLATED:
            assert zf.read(member) == data, path.name


def main():
    write_schemas()
    write_expected()
    text = "".join(render_line(row) for row in ROWS)
    for encoding in ENCODINGS:
        member = f"people.{encoding}.txt"
        data = text.encode(encoding)  # strict: fails if a character is missing
        (HERE / member).write_bytes(data)
        (HERE / f"{member}.gz").write_bytes(gzip.compress(data, mtime=0))
        deflate = HERE / f"people.{encoding}.deflate.zip"
        deflate64 = HERE / f"people.{encoding}.deflate64.zip"
        write_deflate_zip(deflate, member, data)
        write_deflate64_zip(deflate64, member, data)
        assert gzip.decompress((HERE / f"{member}.gz").read_bytes()) == data
        check_zip(deflate, member, data, zipfile.ZIP_DEFLATED)
        check_zip(deflate64, member, data, 9)  # 9 = Deflate64


if __name__ == "__main__":
    main()
