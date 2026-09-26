use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};

use fwf::{Encoding, Format, ReadOptions, Schema};

fn fixture(name: &str) -> PathBuf {
    [env!("CARGO_MANIFEST_DIR"), "tests", "fixtures", name]
        .iter()
        .collect()
}

/// `fwf` with space-separated `args`, run in the fixtures directory.
fn fwf(args: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_fwf"));
    command
        .current_dir(fixture(""))
        .args(args.split_whitespace());
    command
}

fn people() -> ReadOptions {
    let schema = Schema::from_json(&fs::read(fixture("people.schema.json")).unwrap()).unwrap();
    ReadOptions::new(schema, Encoding::Cp1252)
}

/// What the library writes for the cp1252 people records read with `options`.
fn expected(options: &ReadOptions, format: Format) -> Vec<u8> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("expected");
    let batches = fwf::scan(fixture("people.cp1252.txt"), options).unwrap();
    fwf::write(batches, format, &*path).unwrap();
    fs::read(path).unwrap()
}

/// `out`'s stdout, after checking the command succeeded with nothing on stderr.
fn stdout(out: Output) -> Vec<u8> {
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success() && stderr.is_empty(), "{stderr}");
    out.stdout
}

#[test]
fn writes_csv_to_stdout() {
    let out = fwf("people.cp1252.txt --layout people.schema.json --encoding cp1252").output();
    assert_eq!(stdout(out.unwrap()), expected(&people(), Format::Csv));
}

#[test]
fn reads_stdin_without_an_input() {
    let mut child = fwf("--layout people.schema.json --encoding cp1252 -o -")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let gzip = fs::read(fixture("people.cp1252.txt.gz")).unwrap();
    child.stdin.take().unwrap().write_all(&gzip).unwrap();
    let out = child.wait_with_output().unwrap();
    assert_eq!(stdout(out), expected(&people(), Format::Csv));
}

#[test]
fn an_output_ending_in_parquet_is_parquet_unless_format_says_otherwise() {
    let dir = tempfile::tempdir().unwrap();
    let cases = [
        ("a.parquet", "", Format::Parquet),
        ("b.PARQUET", "", Format::Parquet),
        ("c.csv", "", Format::Csv),
        ("d.out", "--format parquet", Format::Parquet),
        ("e.parquet", "--format csv", Format::Csv),
    ];
    for (name, flags, format) in cases {
        let path = dir.path().join(name);
        let args =
            format!("people.cp1252.txt --layout people.schema.json --encoding cp1252 {flags} -o");
        assert!(stdout(fwf(&args).arg(&path).output().unwrap()).is_empty());
        let written = fs::read(&path).unwrap();
        assert_eq!(written, expected(&people(), format), "{name}");
    }
}

#[test]
fn an_entry_chooses_a_zip_member() {
    let args = "people.multi.zip --layout people.schema.json --encoding cp850 --entry parts/2.txt";
    let out = fwf(args).output().unwrap();
    assert_eq!(stdout(out), expected(&people(), Format::Csv));
}

#[test]
fn skip_rows_and_n_rows_choose_the_records() {
    let args = "people.cp1252.txt --layout people.schema.json --encoding cp1252 \
                --skip-rows 2 --n-rows 3";
    let options = people().with_skip_rows(2).with_n_rows(3);
    assert_eq!(
        stdout(fwf(args).output().unwrap()),
        expected(&options, Format::Csv)
    );
}

#[test]
fn errors_go_to_stderr_and_fail() {
    let cases = [
        (
            "people.cp1252.txt --layout missing.json --encoding cp1252",
            "fwf: missing.json: No such file or directory",
        ),
        (
            "people.cp1252.txt --layout people.schema.unknown-type.json --encoding cp1252",
            "fwf: people.schema.unknown-type.json: field \"amount\": unknown type",
        ),
        (
            "people.cp850.txt --layout people.schema.json --encoding cp1252",
            "fwf: people.cp850.txt: record 2, field \"name\"",
        ),
        (
            "people.multi.zip --layout people.schema.json --encoding cp850",
            "fwf: people.multi.zip: ",
        ),
    ];
    for (args, message) in cases {
        let out = fwf(args).output().unwrap();
        let stderr = String::from_utf8(out.stderr).unwrap();
        assert_eq!(out.status.code(), Some(1), "{args}: {stderr}");
        assert!(stderr.starts_with(message), "{args}: {stderr}");
    }
}

#[test]
fn a_closed_stdout_pipe_exits_quietly() {
    // More CSV than the 1 MiB output buffer and the pipe hold.
    let dir = tempfile::tempdir().unwrap();
    let big = dir.path().join("big.txt");
    let people = fs::read(fixture("people.cp1252.txt")).unwrap();
    fs::write(&big, people.repeat(5_000)).unwrap();

    let mut child = fwf("--layout people.schema.json --encoding cp1252")
        .arg(&big)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // Read a little, then close the pipe, as `head` does.
    let mut pipe = child.stdout.take().unwrap();
    pipe.read_exact(&mut [0; 100]).unwrap();
    drop(pipe);
    stdout(child.wait_with_output().unwrap());
}
