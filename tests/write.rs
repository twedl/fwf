use std::fs::{self, File};
use std::io;
use std::path::PathBuf;

use arrow_array::RecordBatchReader;
use arrow_select::concat::concat_batches;
use fwf::{Encoding, Error, Format, Location, ReadOptions, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;

fn fixture(name: &str) -> PathBuf {
    [env!("CARGO_MANIFEST_DIR"), "tests", "fixtures", name]
        .iter()
        .collect()
}

fn people() -> ReadOptions {
    let schema = Schema::from_json(&fs::read(fixture("people.schema.json")).unwrap()).unwrap();
    ReadOptions::new(schema, Encoding::Cp1252)
}

fn scan_people() -> Box<dyn RecordBatchReader + Send> {
    fwf::scan(fixture("people.cp1252.txt"), &people()).unwrap()
}

const PEOPLE_CSV: &str = "\
id,name,city,born,amount,code
000001,José García,Málaga,19850312,1234.5,ES
000002,Zoë Müller,Zürich,19900101,99.99,CH
000003,Françoise Lefèvre,Besançon,19771231,0.0,FR
000004,Åsa Ström,Göteborg,20010704,1000000.0,SE
000005,Þór Guðmundsson,Reykjavík,19650228,-42.1,IS
000006,Maximilian Straßberg,Düsseldorf,19881111,7.0,DE
000007,Ana Brandão,,19930615,350.25,PT
000008,John Smith,Leeds,19991231,12.0,
";

#[test]
fn csv_has_a_header_and_empty_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("people.csv");
    fs::write(&out, "old").unwrap();
    fwf::write(scan_people(), Format::Csv, &*out).unwrap();
    assert_eq!(fs::read_to_string(&out).unwrap(), PEOPLE_CSV);
}

#[test]
fn csv_keeps_the_batches_in_order() {
    // 800 batches of one record, written a few per thread at a time.
    let text = fs::read(fixture("people.cp1252.txt")).unwrap();
    let location = Location::Bytes(text.repeat(100).into());
    let batches = fwf::scan(location, &people().with_chunk_size(1)).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("people.csv");
    fwf::write(batches, Format::Csv, &*out).unwrap();

    let (header, records) = PEOPLE_CSV.split_at(PEOPLE_CSV.find('\n').unwrap() + 1);
    assert_eq!(
        fs::read_to_string(&out).unwrap(),
        header.to_owned() + &records.repeat(100)
    );
}

#[test]
fn parquet_reads_back_as_the_records() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("people.parquet");
    // Several batches, about two records each.
    let options = people().with_chunk_size(100);
    let batches = fwf::scan(fixture("people.cp1252.txt"), &options).unwrap();
    fwf::write(batches, Format::Parquet, &*out).unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&out).unwrap()).unwrap();
    let compression = builder.metadata().row_group(0).column(0).compression();
    assert!(
        matches!(compression, Compression::ZSTD(_)),
        "{compression:?}"
    );
    let reader = builder.build().unwrap();
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let expected = fwf::read(fixture("people.cp1252.txt"), &people()).unwrap();
    assert_eq!(concat_batches(&schema, &batches).unwrap(), expected);
}

#[test]
fn a_failed_write_leaves_the_file_as_it_was() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("people.csv");
    fs::write(&out, "old").unwrap();
    for format in [Format::Csv, Format::Parquet] {
        // cp850 read as cp1252 fails on its first ü.
        let batches = fwf::scan(fixture("people.cp850.txt"), &people()).unwrap();
        let err = fwf::write(batches, format, &*out).unwrap_err();
        assert!(
            matches!(err, Error::InvalidByte { .. }),
            "{format:?}: {err:?}"
        );
        assert_eq!(fs::read_to_string(&out).unwrap(), "old", "{format:?}");
        let files = fs::read_dir(dir.path()).unwrap().count();
        assert_eq!(files, 1, "{format:?}: the temp file is removed");
    }
}

#[test]
fn an_empty_input_still_gets_a_header() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("empty.csv");
    let batches = fwf::scan(Location::Bytes(Default::default()), &people()).unwrap();
    fwf::write(batches, Format::Csv, &*out).unwrap();
    assert_eq!(
        fs::read_to_string(&out).unwrap(),
        "id,name,city,born,amount,code\n"
    );
}

#[test]
fn a_missing_directory_is_a_write_error() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("missing").join("people.csv");
    let err = fwf::write(scan_people(), Format::Csv, &*out).unwrap_err();
    let Error::Write {
        destination,
        source,
    } = &err
    else {
        panic!("{err:?}")
    };
    assert_eq!(*destination, out.display().to_string());
    assert_eq!(source.kind(), io::ErrorKind::NotFound);
}

#[cfg(unix)]
#[test]
fn the_file_gets_a_new_files_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("people.csv");
    fwf::write(scan_people(), Format::Csv, &*out).unwrap();
    let mode = |path| fs::metadata(path).unwrap().permissions().mode();
    let plain = dir.path().join("plain");
    File::create(&plain).unwrap();
    assert_eq!(mode(&out), mode(&plain));
}
