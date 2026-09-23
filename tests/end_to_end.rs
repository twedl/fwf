//! End-to-end tests: build a zip, run the real binary, read the Parquet back.
//!
//! These go through the CLI rather than a library API, so what is tested is the
//! contract the Python caller actually depends on — including exit codes.
//!
//! Every run gets its own zip and its own output path. An earlier version shared
//! one `out.parquet` across runs and read it lazily at assert time, which made
//! any test comparing two runs compare the second run against itself.

use std::cell::Cell;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Output;

use polars::prelude::*;
use tempfile::TempDir;

/// A layout with a one-character gap between each field, as real ones have.
///
/// ```text
/// city    _ prov _ amount
/// 1....8  9 10.11 12 13......22
/// ```
const SCHEMA: &str = r#"[
    {"name": "city",   "at": [1, 8],   "type": "String"},
    {"name": "prov",   "at": [10, 2],  "type": "String"},
    {"name": "amount", "at": [13, 10], "type": "Float64"}
]"#;

/// The typed columns, plus one the record does not carry at all.
///
/// ```text
/// city    _ count _ opened
/// 1....8  9 10.13 14 15....22    region: declared, never read
/// ```
const TYPED_SCHEMA: &str = r#"[
    {"name": "city",   "at": [1, 8],  "type": "String"},
    {"name": "count",  "at": [10, 4], "type": "Int64"},
    {"name": "opened", "at": [15, 8], "type": "Date", "format": "%Y%m%d"},
    {"name": "region", "type": "String"}
]"#;

struct Fixture {
    dir: TempDir,
    counter: Cell<usize>,
}

impl Fixture {
    fn new() -> Self {
        Fixture { dir: tempfile::tempdir().unwrap(), counter: Cell::new(0) }
    }

    fn next(&self) -> usize {
        let n = self.counter.get();
        self.counter.set(n + 1);
        n
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn zip(&self, members: &[(&str, &[u8])]) -> PathBuf {
        let path = self.path(&format!("data{}.zip", self.next()));
        // `::zip` spelled out: polars' prelude also exports a `zip`.
        let mut writer = ::zip::ZipWriter::new(File::create(&path).unwrap());
        for (name, body) in members {
            writer
                .start_file(*name, ::zip::write::SimpleFileOptions::default())
                .unwrap();
            writer.write_all(body).unwrap();
        }
        writer.finish().unwrap();
        path
    }

    fn schema(&self, json: &str) -> PathBuf {
        let path = self.path(&format!("schema{}.json", self.next()));
        std::fs::write(&path, json).unwrap();
        path
    }

    fn run(&self, zip: &Path, member: &str, schema: &Path, encoding: &str) -> Run {
        let out = self.path(&format!("out{}.parquet", self.next()));
        self.run_to(&out, zip, member, schema, encoding)
    }

    fn run_to(&self, out: &Path, zip: &Path, member: &str, schema: &Path, encoding: &str) -> Run {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_fwf"))
            .args(["--zip", zip.to_str().unwrap()])
            .args(["--member", member])
            .args(["--schema", schema.to_str().unwrap()])
            .args(["--encoding", encoding])
            .args(["--out", out.to_str().unwrap()])
            .output()
            .unwrap();
        // Read eagerly, while this run's output is still the file on disk.
        let frame = (output.status.success() && out.exists()).then(|| read(out));
        Run { output, out: out.to_path_buf(), frame }
    }

    /// Any `.partial` files still sitting in the working directory. The name
    /// carries a random middle, so it is matched by suffix rather than predicted.
    /// `the_leftover_detector_sees_leftovers` guards this against matching nothing.
    fn partials(&self) -> Vec<PathBuf> {
        std::fs::read_dir(self.dir.path())
            .unwrap()
            .filter_map(|entry| entry.ok().map(|e| e.path()))
            .filter(|path| path.to_string_lossy().ends_with(".partial"))
            .collect()
    }

    /// The common case: one member, cp1252, the standard schema.
    fn parse(&self, body: &[u8]) -> DataFrame {
        self.parse_as("cp1252", body)
    }

    fn parse_as(&self, encoding: &str, body: &[u8]) -> DataFrame {
        self.parse_with(SCHEMA, encoding, body)
    }

    fn parse_with(&self, schema: &str, encoding: &str, body: &[u8]) -> DataFrame {
        let zip = self.zip(&[("records.dat", body)]);
        let schema = self.schema(schema);
        self.run(&zip, "records.dat", &schema, encoding).frame().clone()
    }
}

struct Run {
    output: Output,
    out: PathBuf,
    frame: Option<DataFrame>,
}

impl Run {
    fn frame(&self) -> &DataFrame {
        assert!(
            self.output.status.success(),
            "expected success, got {}\nstderr: {}",
            self.output.status,
            self.stderr()
        );
        self.frame.as_ref().expect("a successful run must leave a Parquet file")
    }

    fn stderr(&self) -> String {
        String::from_utf8_lossy(&self.output.stderr).into_owned()
    }

    fn failed(&self) -> bool {
        !self.output.status.success()
    }
}

fn read(path: &Path) -> DataFrame {
    ParquetReader::new(File::open(path).unwrap()).finish().unwrap()
}

fn strings(frame: &DataFrame, name: &str) -> Vec<Option<String>> {
    frame
        .column(name)
        .unwrap()
        .str()
        .unwrap()
        .into_iter()
        .map(|value| value.map(str::to_owned))
        .collect()
}

fn floats(frame: &DataFrame, name: &str) -> Vec<Option<f64>> {
    frame.column(name).unwrap().f64().unwrap().into_iter().collect()
}

fn ints(frame: &DataFrame, name: &str) -> Vec<Option<i64>> {
    frame.column(name).unwrap().i64().unwrap().into_iter().collect()
}

/// Dates come back as the days-since-epoch a Date32 physically holds, which is
/// what the column is — reading it as anything else would test the conversion
/// rather than the stored value.
fn dates(frame: &DataFrame, name: &str) -> Vec<Option<i32>> {
    frame.column(name).unwrap().date().unwrap().physical().into_iter().collect()
}

fn some(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_string())).collect()
}

fn names(frame: &DataFrame) -> Vec<String> {
    frame.get_column_names().iter().map(|n| n.to_string()).collect()
}

#[test]
fn round_trips_a_cp1252_member() {
    let fixture = Fixture::new();
    // MONTRÉAL is 8 characters and, in cp1252, 8 bytes.
    let body: &[u8] = b"MONTR\xc9AL QC    1234.50\nTORONTO  ON       42.00\n";
    let frame = fixture.parse(body);

    assert_eq!(frame.shape(), (2, 3));
    assert_eq!(strings(&frame, "city"), some(&["MONTRÉAL", "TORONTO"]));
    assert_eq!(strings(&frame, "prov"), some(&["QC", "ON"]));
    assert_eq!(floats(&frame, "amount"), vec![Some(1234.50), Some(42.00)]);
}

/// cp850 is a reason this tool exists, and its high range expands to three UTF-8
/// bytes — so a mistake in the offset bookkeeping would shift every later field.
#[test]
fn round_trips_a_cp850_member() {
    // 0xB0 is U+2591, three bytes once encoded. The record stays 22 characters.
    let frame = Fixture::new().parse_as("cp850", b"CAF\xb0     ON       1.50\n");
    assert_eq!(strings(&frame, "city"), some(&["CAF\u{2591}"]));
    assert_eq!(strings(&frame, "prov"), some(&["ON"]), "a later field must not shift");
    assert_eq!(floats(&frame, "amount"), vec![Some(1.50)]);
}

#[test]
fn round_trips_a_latin1_member() {
    // 0xE9 is é in latin-1, two bytes once encoded.
    let frame = Fixture::new().parse_as("latin1", b"CAF\xe9     ON       1.50\n");
    assert_eq!(strings(&frame, "city"), some(&["CAFé"]));
    assert_eq!(strings(&frame, "prov"), some(&["ON"]), "a later field must not shift");
}

/// The same byte means different things in each single-byte encoding, so the
/// same member read three ways must give three different answers.
#[test]
fn the_encoding_flag_actually_selects_the_table() {
    let fixture = Fixture::new();
    let body: &[u8] = b"\x82        ON       1.50\n";
    let city = |encoding| strings(&fixture.parse_as(encoding, body), "city");

    assert_eq!(city("cp1252"), some(&["\u{201a}"]));
    assert_eq!(city("cp850"), some(&["é"]));
    assert_eq!(city("latin1"), some(&["\u{0082}"]));
}

#[test]
fn column_order_and_types_follow_the_schema() {
    let frame = Fixture::new().parse(b"TORONTO  ON       42.00\n");
    assert_eq!(names(&frame), vec!["city", "prov", "amount"]);
    assert_eq!(frame.dtypes(), vec![DataType::String, DataType::String, DataType::Float64]);
}

#[test]
fn a_blank_char_field_is_an_empty_string_not_null() {
    let frame = Fixture::new().parse(b"         ON       42.00\n");
    assert_eq!(strings(&frame, "city"), vec![Some(String::new())]);
    assert_eq!(frame.column("city").unwrap().null_count(), 0);
}

#[test]
fn an_unparseable_num_field_is_null() {
    let frame = Fixture::new().parse(b"TORONTO  ON     ***.**\nTORONTO  ON          \n");
    assert_eq!(floats(&frame, "amount"), vec![None, None]);
}

#[test]
fn typed_columns_land_with_the_declared_dtypes() {
    let frame = Fixture::new().parse_with(TYPED_SCHEMA, "cp1252", b"TORONTO  0042 19700102\n");
    assert_eq!(names(&frame), vec!["city", "count", "opened", "region"]);
    assert_eq!(
        frame.dtypes(),
        vec![DataType::String, DataType::Int64, DataType::Date, DataType::String]
    );
}

#[test]
fn an_int64_field_parses_whole_numbers_and_nulls_the_rest() {
    let body = [
        "TORONTO  0042 19700102",
        "TORONTO  -7   19700102",
        // A decimal point and a blank are both rejections, not truncations.
        "TORONTO  12.5 19700102",
        "TORONTO       19700102",
    ]
    .join("\n");
    let frame = Fixture::new().parse_with(TYPED_SCHEMA, "cp1252", body.as_bytes());
    assert_eq!(ints(&frame, "count"), vec![Some(42), Some(-7), None, None]);
}

#[test]
fn a_date_field_parses_with_its_format_and_nulls_what_it_cannot() {
    let body = [
        "TORONTO  0001 19700101",
        "TORONTO  0002 19700102",
        "TORONTO  0003 19691231",
        "TORONTO  0004 not-date",
        "TORONTO  0005         ",
    ]
    .join("\n");
    let frame = Fixture::new().parse_with(TYPED_SCHEMA, "cp1252", body.as_bytes());
    assert_eq!(dates(&frame, "opened"), vec![Some(0), Some(1), Some(-1), None, None]);
}

/// Each Date field carries its own format, so one member can hold date fields
/// that disagree about how a date is written — as layouts of any age do.
#[test]
fn date_formats_are_per_field() {
    let schema = r#"[
        {"name": "long",  "at": [1, 8],  "type": "Date", "format": "%Y%m%d"},
        {"name": "short", "at": [10, 6], "type": "Date", "format": "%y%m%d"}
    ]"#;
    let frame = Fixture::new().parse_with(schema, "cp1252", b"19700102 700103\n");
    assert_eq!(dates(&frame, "long"), vec![Some(1)]);
    assert_eq!(dates(&frame, "short"), vec![Some(2)]);
}

#[test]
fn a_column_the_record_does_not_carry_is_all_null() {
    let body = ["TORONTO  0042 19700102", "OTTAWA   0007 19700103"].join("\n");
    let frame = Fixture::new().parse_with(TYPED_SCHEMA, "cp1252", body.as_bytes());

    assert_eq!(strings(&frame, "region"), vec![None, None]);
    assert_eq!(frame.column("region").unwrap().null_count(), 2);
    // The columns that are read are untouched by the one that is not.
    assert_eq!(strings(&frame, "city"), some(&["TORONTO", "OTTAWA"]));
    assert_eq!(ints(&frame, "count"), vec![Some(42), Some(7)]);
}

/// Nothing in such a schema says how long a record is, so the read bound falls
/// back to its floor instead of being computed as zero — which would fail every
/// record for want of a terminator.
#[test]
fn a_schema_of_only_absent_columns_still_runs() {
    let schema = r#"[{"name": "a", "type": "String"}, {"name": "b", "type": "Int64"}]"#;
    let frame = Fixture::new().parse_with(schema, "cp1252", b"anything at all\nand more\n");

    assert_eq!(frame.height(), 2);
    assert_eq!(strings(&frame, "a"), vec![None, None]);
    assert_eq!(ints(&frame, "b"), vec![None, None]);
}

#[test]
fn a_short_line_gives_empty_strings_and_nulls_without_failing() {
    let frame = Fixture::new().parse(b"TORONTO\n");
    assert_eq!(strings(&frame, "city"), some(&["TORONTO"]));
    assert_eq!(strings(&frame, "prov"), vec![Some(String::new())]);
    assert_eq!(floats(&frame, "amount"), vec![None]);
}

#[test]
fn a_long_line_ignores_the_trailing_characters() {
    let frame = Fixture::new().parse(b"TORONTO  ON       42.00 and then some more\n");
    assert_eq!(strings(&frame, "prov"), some(&["ON"]));
    assert_eq!(floats(&frame, "amount"), vec![Some(42.00)]);
}

#[test]
fn handles_crlf_and_a_missing_final_newline() {
    let frame = Fixture::new().parse(b"TORONTO  ON       42.00\r\nLONDON   ON       7.000");
    assert_eq!(frame.height(), 2);
    assert_eq!(strings(&frame, "prov"), some(&["ON", "ON"]));
    assert_eq!(floats(&frame, "amount"), vec![Some(42.0), Some(7.0)]);
}

#[test]
fn strips_a_utf8_byte_order_mark() {
    let fixture = Fixture::new();
    let body = "\u{feff}TORONTO  ON       42.00\n".as_bytes();

    let frame = fixture.parse_as("utf8", body);
    assert_eq!(strings(&frame, "city"), some(&["TORONTO"]), "the BOM must not shift the record");
    assert_eq!(floats(&frame, "amount"), vec![Some(42.00)]);

    // Read as a single-byte encoding the same three bytes would decode to three
    // visible characters, so they have to go before decoding, not after.
    let frame = fixture.parse_as("cp1252", body);
    assert_eq!(strings(&frame, "city"), some(&["TORONTO"]));
}

#[test]
fn ignores_a_trailing_dos_end_of_file_marker() {
    let fixture = Fixture::new();

    // Trailing the last record, with no newline after it.
    let frame = fixture
        .parse_as("cp850", b"TORONTO  ON       42.00\r\nLONDON   ON       7.000\r\n\x1a");
    assert_eq!(frame.height(), 2, "the 0x1A marker must not become a row");
    assert_eq!(strings(&frame, "city"), some(&["TORONTO", "LONDON"]));

    // Given a line of its own, newline and all.
    let frame = fixture.parse_as("cp850", b"TORONTO  ON       42.00\r\n\x1a\r\n");
    assert_eq!(frame.height(), 1, "a marker on its own line must not become a row");
    assert_eq!(strings(&frame, "city"), some(&["TORONTO"]));

    // Sitting between a carriage return and the end of the member, so the CR
    // still has to come off after the marker does.
    let frame = fixture.parse_as("cp850", b"TORONTO  ON       42.00\r\x1a");
    assert_eq!(strings(&frame, "city"), some(&["TORONTO"]));
    assert_eq!(floats(&frame, "amount"), vec![Some(42.00)], "a stray CR must not survive");
}

/// The reason positions are counted in characters: the same record in two
/// encodings has different byte layouts but must produce identical output.
#[test]
fn cp1252_and_utf8_produce_identical_output() {
    let fixture = Fixture::new();
    let schema = fixture.schema(SCHEMA);

    let cp1252: &[u8] = b"MONTR\xc9AL QC    1234.50\n";
    let utf8 = "MONTRÉAL QC    1234.50\n".as_bytes();
    assert_ne!(cp1252.len(), utf8.len(), "the byte layouts must actually differ");

    let from_cp1252 = fixture.run(&fixture.zip(&[("a.dat", cp1252)]), "a.dat", &schema, "cp1252");
    let from_utf8 = fixture.run(&fixture.zip(&[("b.dat", utf8)]), "b.dat", &schema, "utf8");

    // Guard against this comparing one run with itself, which is what an earlier
    // version of the fixture did by writing both runs to the same path.
    assert_ne!(from_cp1252.out, from_utf8.out, "each run needs its own output file");
    assert_eq!(from_cp1252.frame(), from_utf8.frame());
    assert_eq!(strings(from_cp1252.frame(), "city"), some(&["MONTRÉAL"]));
}

#[test]
fn reads_the_named_member_and_ignores_the_others() {
    let fixture = Fixture::new();
    let zip = fixture.zip(&[
        ("first.dat", b"AAAAAAA  ZZ        1.00\n" as &[u8]),
        ("second.dat", b"BBBBBBB  YY        2.00\n"),
        ("third.dat", b"CCCCCCC  XX        3.00\n"),
    ]);
    let schema = fixture.schema(SCHEMA);
    let frame = fixture.run(&zip, "second.dat", &schema, "cp1252").frame().clone();
    assert_eq!(strings(&frame, "city"), some(&["BBBBBBB"]));
}

#[test]
fn an_empty_member_produces_an_empty_frame_with_the_right_columns() {
    let frame = Fixture::new().parse(b"");
    assert_eq!(frame.height(), 0);
    assert_eq!(names(&frame), vec!["city", "prov", "amount"]);
}

#[test]
fn overwrites_an_existing_output_file() {
    let fixture = Fixture::new();
    let out = fixture.path("out.parquet");
    std::fs::write(&out, b"stale contents").unwrap();

    let zip = fixture.zip(&[("records.dat", b"TORONTO  ON       42.00\n" as &[u8])]);
    let schema = fixture.schema(SCHEMA);
    let run = fixture.run_to(&out, &zip, "records.dat", &schema, "cp1252");
    assert_eq!(run.frame().height(), 1);
}

/// Output is written beside the destination and renamed on success, so a run
/// that dies partway must not take an existing good extract down with it.
#[test]
fn a_failed_run_leaves_an_earlier_output_intact() {
    let fixture = Fixture::new();
    let schema = fixture.schema(SCHEMA);
    let out = fixture.path("keep.parquet");

    let good = fixture.zip(&[("good.dat", b"TORONTO  ON       42.00\n" as &[u8])]);
    assert_eq!(fixture.run_to(&out, &good, "good.dat", &schema, "cp1252").frame().height(), 1);

    // Line 2 is not valid UTF-8, so this run fails after the output is opened.
    let bad = fixture.zip(&[("bad.dat", b"GOOD     ON       1.00\nCAF\xe9\n" as &[u8])]);
    assert!(fixture.run_to(&out, &bad, "bad.dat", &schema, "utf8").failed());

    let survivor = read(&out);
    assert_eq!(survivor.height(), 1, "the earlier extract must survive");
    assert_eq!(strings(&survivor, "city"), some(&["TORONTO"]));

    assert!(fixture.partials().is_empty(), "partials left: {:?}", fixture.partials());
}

/// 0x1A is a marker only at the end of the member. Anywhere else it is data, and
/// the record holding it is a record like any other.
#[test]
fn a_dos_marker_mid_member_is_data_not_a_terminator() {
    let frame = Fixture::new()
        .parse_as("cp850", b"TORONTO  ON       42.00\n\x1a\nLONDON   ON       7.000\n");
    assert_eq!(frame.height(), 3, "the middle line is a record, not a marker");
    assert_eq!(strings(&frame, "city"), some(&["TORONTO", "\u{1a}", "LONDON"]));
}

/// The two tests below assert that no `.partial` file is left behind, which is
/// worth nothing if the matcher cannot recognise one. This pins that down.
#[test]
fn the_leftover_detector_sees_leftovers() {
    let fixture = Fixture::new();
    assert!(fixture.partials().is_empty(), "nothing has run yet");

    std::fs::write(fixture.path("out.parquetAbC123.partial"), b"debris").unwrap();
    assert_eq!(fixture.partials().len(), 1, "a stray partial file must be visible");
}

/// The case that matters most: a failure *after* a row group has been written,
/// where the file on disk holds data but no footer and no reader could open it.
#[test]
fn a_failure_after_a_row_group_leaves_nothing_behind() {
    let fixture = Fixture::new();
    let schema = fixture.schema(SCHEMA);
    let out = fixture.path("big.parquet");

    // One row past a batch boundary, then a line that is not valid UTF-8.
    let mut body = b"TORONTO  ON       42.00\n".repeat(262_200);
    body.extend_from_slice(b"CAF\xe9\n");
    let zip = fixture.zip(&[("big.dat", body.as_slice())]);

    let run = fixture.run_to(&out, &zip, "big.dat", &schema, "utf8");
    assert!(run.failed(), "stderr: {}", run.stderr());
    assert!(!out.exists(), "a half-written Parquet must not appear at the destination");
    assert!(fixture.partials().is_empty(), "partials left: {:?}", fixture.partials());
}

/// Fixed-length records with no terminators are out of scope, and once the
/// member is big enough to be sure of that, it fails rather than collapsing
/// into a single row.
#[test]
fn a_member_with_no_line_terminators_fails() {
    let fixture = Fixture::new();
    // Comfortably past the read bound, so this cannot be one long record.
    let body = b"TORONTO  ON       42.00".repeat(4_000);
    let zip = fixture.zip(&[("records.dat", body.as_slice())]);
    let schema = fixture.schema(SCHEMA);
    let run = fixture.run(&zip, "records.dat", &schema, "cp1252");

    assert!(run.failed(), "expected failure, stderr: {}", run.stderr());
    assert!(run.stderr().contains("no line terminator"), "stderr was: {}", run.stderr());
}

#[test]
fn a_single_unterminated_record_is_still_fine() {
    let frame = Fixture::new().parse(b"TORONTO  ON       42.00");
    assert_eq!(frame.height(), 1);
    assert_eq!(strings(&frame, "city"), some(&["TORONTO"]));
}

/// Records may run well past the last field the schema extracts, and such a
/// record with no trailing newline is still just one record — not a member that
/// has lost its terminators.
#[test]
fn a_long_unterminated_record_is_not_mistaken_for_a_missing_terminator() {
    let mut body = b"TORONTO  ON       42.00".to_vec();
    body.extend(std::iter::repeat_n(b' ', 500));

    let frame = Fixture::new().parse(&body);
    assert_eq!(frame.height(), 1);
    assert_eq!(strings(&frame, "city"), some(&["TORONTO"]));
    assert_eq!(floats(&frame, "amount"), vec![Some(42.00)]);
}

/// Below the read bound a member with no terminators cannot be told apart from
/// one long record. This records where that boundary sits rather than implying
/// the detection is exact.
#[test]
fn a_small_member_with_no_terminators_reads_as_one_record() {
    let frame = Fixture::new().parse(b"TORONTO  ON       42.00".repeat(5).as_slice());
    assert_eq!(frame.height(), 1, "known limit: too small to distinguish");
}

#[test]
fn reports_the_row_count_on_stderr() {
    let fixture = Fixture::new();
    let zip = fixture.zip(&[("records.dat", b"TORONTO  ON       42.00\n" as &[u8])]);
    let schema = fixture.schema(SCHEMA);
    let run = fixture.run(&zip, "records.dat", &schema, "cp1252");
    assert!(run.stderr().contains("1 rows"), "stderr was: {}", run.stderr());
    assert!(run.stderr().contains(".parquet"), "stderr was: {}", run.stderr());
}

#[test]
fn a_missing_member_fails_and_names_what_the_archive_holds() {
    let fixture = Fixture::new();
    let zip = fixture.zip(&[("actual.dat", b"x" as &[u8])]);
    let schema = fixture.schema(SCHEMA);
    let run = fixture.run(&zip, "wrong.dat", &schema, "cp1252");

    assert!(run.failed());
    let stderr = run.stderr();
    assert!(stderr.contains("wrong.dat"), "stderr was: {stderr}");
    assert!(stderr.contains("actual.dat"), "stderr was: {stderr}");
}

#[test]
fn a_bad_schema_fails_before_writing_anything() {
    let fixture = Fixture::new();
    let zip = fixture.zip(&[("records.dat", b"x" as &[u8])]);
    let schema = fixture.schema(
        r#"[{"name": "a", "at": [1, 4], "type": "String"},
            {"name": "a", "at": [6, 4], "type": "Float64"}]"#,
    );
    let out = fixture.path("never.parquet");
    let run = fixture.run_to(&out, &zip, "records.dat", &schema, "cp1252");

    assert!(run.failed());
    assert!(run.stderr().contains("duplicate column name"), "stderr was: {}", run.stderr());
    assert!(!out.exists(), "no output should have been created");
}

#[test]
fn invalid_utf8_under_the_utf8_encoding_fails_with_the_line_number() {
    let fixture = Fixture::new();
    let zip = fixture.zip(&[("records.dat", b"GOOD     ON       1.00\nCAF\xe9\n" as &[u8])]);
    let schema = fixture.schema(SCHEMA);
    let run = fixture.run(&zip, "records.dat", &schema, "utf8");

    assert!(run.failed());
    let stderr = run.stderr();
    assert!(stderr.contains("line 2"), "stderr was: {stderr}");
    assert!(stderr.contains("UTF-8"), "stderr was: {stderr}");
}

#[test]
fn an_unknown_encoding_is_rejected_by_the_cli() {
    let fixture = Fixture::new();
    let zip = fixture.zip(&[("records.dat", b"x" as &[u8])]);
    let schema = fixture.schema(SCHEMA);
    assert!(fixture.run(&zip, "records.dat", &schema, "cp437").failed());
}
