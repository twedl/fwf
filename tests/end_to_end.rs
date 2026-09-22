//! End-to-end tests: build a zip, run the real binary, read the Parquet back.
//!
//! These go through the CLI rather than a library API, so what is tested is the
//! contract the Python caller actually depends on — including exit codes.

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
const SCHEMA: &str =
    r#"[["city", 1, 8, "Char"], ["prov", 10, 2, "Char"], ["amount", 13, 10, "Num"]]"#;

struct Fixture {
    dir: TempDir,
}

impl Fixture {
    fn new() -> Self {
        Fixture { dir: tempfile::tempdir().unwrap() }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn zip(&self, members: &[(&str, &[u8])]) -> PathBuf {
        let path = self.path("data.zip");
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
        let path = self.path("schema.json");
        std::fs::write(&path, json).unwrap();
        path
    }

    fn run(&self, zip: &Path, member: &str, schema: &Path, encoding: &str) -> Run {
        let out = self.path("out.parquet");
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_fwf"))
            .args(["--zip", zip.to_str().unwrap()])
            .args(["--member", member])
            .args(["--schema", schema.to_str().unwrap()])
            .args(["--encoding", encoding])
            .args(["--out", out.to_str().unwrap()])
            .output()
            .unwrap();
        Run { output, out }
    }

    /// The common case: one member, cp1252, the standard schema.
    fn parse(&self, body: &[u8]) -> DataFrame {
        let zip = self.zip(&[("records.dat", body)]);
        let schema = self.schema(SCHEMA);
        self.run(&zip, "records.dat", &schema, "cp1252").frame()
    }
}

struct Run {
    output: Output,
    out: PathBuf,
}

impl Run {
    fn frame(&self) -> DataFrame {
        assert!(
            self.output.status.success(),
            "expected success, got {}\nstderr: {}",
            self.output.status,
            self.stderr()
        );
        ParquetReader::new(File::open(&self.out).unwrap()).finish().unwrap()
    }

    fn stderr(&self) -> String {
        String::from_utf8_lossy(&self.output.stderr).into_owned()
    }
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
    assert_eq!(from_cp1252.frame(), from_utf8.frame());
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
    let frame = fixture.run(&zip, "second.dat", &schema, "cp1252").frame();
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
    std::fs::write(fixture.path("out.parquet"), b"stale contents").unwrap();
    let frame = fixture.parse(b"TORONTO  ON       42.00\n");
    assert_eq!(frame.height(), 1);
}

#[test]
fn reports_the_row_count_on_stderr() {
    let fixture = Fixture::new();
    let zip = fixture.zip(&[("records.dat", b"TORONTO  ON       42.00\n" as &[u8])]);
    let schema = fixture.schema(SCHEMA);
    let run = fixture.run(&zip, "records.dat", &schema, "cp1252");
    assert!(run.stderr().contains("1 rows"), "stderr was: {}", run.stderr());
    assert!(run.stderr().contains("out.parquet"), "stderr was: {}", run.stderr());
}

#[test]
fn a_missing_member_fails_and_names_what_the_archive_holds() {
    let fixture = Fixture::new();
    let zip = fixture.zip(&[("actual.dat", b"x" as &[u8])]);
    let schema = fixture.schema(SCHEMA);
    let run = fixture.run(&zip, "wrong.dat", &schema, "cp1252");

    assert!(!run.output.status.success());
    let stderr = run.stderr();
    assert!(stderr.contains("wrong.dat"), "stderr was: {stderr}");
    assert!(stderr.contains("actual.dat"), "stderr was: {stderr}");
}

#[test]
fn a_bad_schema_fails_before_writing_anything() {
    let fixture = Fixture::new();
    let zip = fixture.zip(&[("records.dat", b"x" as &[u8])]);
    let schema = fixture.schema(r#"[["a", 1, 4, "Char"], ["a", 6, 4, "Num"]]"#);
    let run = fixture.run(&zip, "records.dat", &schema, "cp1252");

    assert!(!run.output.status.success());
    assert!(run.stderr().contains("duplicate column name"), "stderr was: {}", run.stderr());
    assert!(!fixture.path("out.parquet").exists(), "no output should have been created");
}

#[test]
fn invalid_utf8_under_the_utf8_encoding_fails_with_the_line_number() {
    let fixture = Fixture::new();
    let zip = fixture.zip(&[("records.dat", b"GOOD     ON       1.00\nCAF\xe9\n" as &[u8])]);
    let schema = fixture.schema(SCHEMA);
    let run = fixture.run(&zip, "records.dat", &schema, "utf8");

    assert!(!run.output.status.success());
    let stderr = run.stderr();
    assert!(stderr.contains("line 2"), "stderr was: {stderr}");
    assert!(stderr.contains("UTF-8"), "stderr was: {stderr}");
}

#[test]
fn an_unknown_encoding_is_rejected_by_the_cli() {
    let fixture = Fixture::new();
    let zip = fixture.zip(&[("records.dat", b"x" as &[u8])]);
    let schema = fixture.schema(SCHEMA);
    let run = fixture.run(&zip, "records.dat", &schema, "cp437");
    assert!(!run.output.status.success());
}
