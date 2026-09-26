use std::io::Cursor;
use std::path::PathBuf;

use arrow_array::{Array, Float64Array, RecordBatch, StringArray};
use fwf::{Container, Encoding, Error, Location, ReadOptions, Schema};
use serde_json::{Map, Value};

fn fixture(name: &str) -> PathBuf {
    [env!("CARGO_MANIFEST_DIR"), "tests", "fixtures", name]
        .iter()
        .collect()
}

fn read_fixture(name: &str) -> Vec<u8> {
    std::fs::read(fixture(name)).unwrap()
}

fn people(encoding: Encoding) -> ReadOptions {
    let schema = Schema::from_json(&read_fixture("people.schema.json")).unwrap();
    ReadOptions::new(schema, encoding)
}

fn expected() -> Value {
    serde_json::from_slice(&read_fixture("people.expected.json")).unwrap()
}

/// The batch as JSON rows, the shape of people.expected.json.
fn rows(batch: &RecordBatch) -> Value {
    let rows = (0..batch.num_rows()).map(|row| {
        let mut record = Map::new();
        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
            let any = column.as_any();
            let value = if column.is_null(row) {
                Value::Null
            } else if let Some(strings) = any.downcast_ref::<StringArray>() {
                Value::from(strings.value(row))
            } else {
                Value::from(any.downcast_ref::<Float64Array>().unwrap().value(row))
            };
            record.insert(field.name().clone(), value);
        }
        Value::Object(record)
    });
    Value::Array(rows.collect())
}

#[test]
fn every_encoding_and_container_reads_to_the_expected_records() {
    let expected = expected();
    let mut files: Vec<(String, Encoding)> = Vec::new();
    for encoding in [Encoding::Cp1252, Encoding::Cp850] {
        for kind in ["txt", "txt.gz", "deflate.zip", "deflate64.zip"] {
            files.push((format!("people.{encoding}.{kind}"), encoding));
        }
    }
    files.push(("people.cp1252.stored.zip".into(), Encoding::Cp1252));
    for (file, encoding) in files {
        let batch = fwf::read(fixture(&file), &people(encoding)).unwrap();
        assert_eq!(rows(&batch), expected, "{file}");
    }
}

#[test]
fn bytes_and_readers_read_like_files() {
    // A reader is streamed; a zip from a reader is first copied to a temp file.
    for file in ["people.cp1252.txt.gz", "people.cp1252.deflate.zip"] {
        let data = read_fixture(file);
        let options = people(Encoding::Cp1252);
        let from_bytes = fwf::read(Location::Bytes(data.clone().into()), &options).unwrap();
        let from_reader = fwf::read(Location::Reader(Box::new(Cursor::new(data))), &options);
        assert_eq!(rows(&from_bytes), expected(), "{file} from bytes");
        assert_eq!(
            rows(&from_reader.unwrap()),
            expected(),
            "{file} from a reader"
        );
    }
}

#[test]
fn entries_choose_zip_members() {
    let multi = fixture("people.multi.zip");
    let err = fwf::read(&*multi, &people(Encoding::Cp850)).unwrap_err();
    assert!(
        err.to_string().ends_with(
            "people.multi.zip: holds 3 files (parts/1.txt, parts/2.txt, README.txt); \
             choose which to read with entries"
        ),
        "{err}"
    );

    let options = people(Encoding::Cp850)
        .with_entries(["parts/*.txt"])
        .unwrap();
    let Value::Array(once) = expected() else {
        unreachable!()
    };
    let twice = Value::Array(once.iter().chain(&once).cloned().collect());
    assert_eq!(rows(&fwf::read(&*multi, &options).unwrap()), twice);

    // Errors name the member.
    let options = people(Encoding::Cp1252)
        .with_entries(["parts/2.txt"])
        .unwrap();
    let err = fwf::read(&*multi, &options).unwrap_err();
    let Error::InvalidByte { position, .. } = &err else {
        panic!("{err:?}")
    };
    assert!(
        position.unit.ends_with("people.multi.zip!parts/2.txt"),
        "{err}"
    );

    let options = people(Encoding::Cp850).with_entries(["*.csv"]).unwrap();
    let err = fwf::read(&*multi, &options).unwrap_err();
    assert!(
        err.to_string().contains("no file matches the entries"),
        "{err}"
    );

    let err = people(Encoding::Cp850)
        .with_entries(["parts/[1.txt"])
        .unwrap_err();
    assert!(matches!(err, Error::InvalidEntryPattern { .. }), "{err:?}");
}

#[test]
fn unsupported_zip_members_are_named() {
    let err = fwf::read(fixture("people.cp1252.lzma.zip"), &people(Encoding::Cp1252));
    let err = err.unwrap_err().to_string();
    assert!(
        err.ends_with(
            "people.cp1252.lzma.zip!people.cp1252.txt: uses compression method 14; \
             only stored (0), deflate (8) and deflate64 (9) are supported"
        ),
        "{err}"
    );
}

#[test]
fn corrupt_members_fail_their_crc() {
    // A stored member is checked when it's opened.
    let mut data = read_fixture("people.cp1252.stored.zip");
    let at = data.windows(6).position(|w| w == b"000001").unwrap();
    data[at] = b'9';
    let err = fwf::read(Location::Bytes(data.into()), &people(Encoding::Cp1252)).unwrap_err();
    assert_eq!(
        err.to_string(),
        "<bytes>!people.cp1252.txt: data doesn't match the archive's size and CRC-32"
    );

    // A compressed member is checked when its stream ends: change the CRC the
    // archive records for it, in both of its headers.
    let crc = crc32fast::hash(&read_fixture("people.cp1252.txt")).to_le_bytes();
    let mut data = read_fixture("people.cp1252.deflate.zip");
    let places: Vec<usize> = (0..data.len() - 3)
        .filter(|&i| data[i..i + 4] == crc)
        .collect();
    assert_eq!(
        places.len(),
        2,
        "the CRC sits in the local and central headers"
    );
    for at in places {
        data[at] ^= 0xFF;
    }
    let err = fwf::read(Location::Bytes(data.into()), &people(Encoding::Cp1252)).unwrap_err();
    assert!(
        matches!(&err, Error::Io { unit, .. } if unit == "<bytes>!people.cp1252.txt"),
        "{err:?}"
    );
}

#[test]
fn a_container_can_be_forced() {
    let options = people(Encoding::Cp1252).with_container(Container::Gzip);
    let err = fwf::read(fixture("people.cp1252.txt"), &options).unwrap_err();
    assert!(matches!(err, Error::Io { .. }), "{err:?}");
}

#[test]
fn cp850_read_as_cp1252_names_the_bad_byte() {
    // cp850 writes the ü in "Zoë Müller" as 0x81, which cp1252 leaves undefined.
    let err = fwf::read(fixture("people.cp850.txt"), &people(Encoding::Cp1252)).unwrap_err();
    let Error::InvalidByte { position, byte } = &err else {
        panic!("{err:?}")
    };
    assert_eq!(
        (
            position.record,
            position.field.as_str(),
            position.byte,
            *byte
        ),
        (2, "name", 73, 0x81)
    );
    assert!(
        err.to_string()
            .ends_with("is not valid cp1252; is the file cp850?")
    );
}

#[test]
fn a_missing_file_is_an_io_error() {
    let err = fwf::read(fixture("no-such-file.txt"), &people(Encoding::Cp1252)).unwrap_err();
    assert!(matches!(err, Error::Io { .. }), "{err:?}");
}
