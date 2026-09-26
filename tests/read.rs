use std::path::PathBuf;

use arrow_array::{Array, Float64Array, RecordBatch, StringArray};
use fwf::{Encoding, Error, ReadOptions, Schema};
use serde_json::{Map, Value};

fn fixture(name: &str) -> PathBuf {
    [env!("CARGO_MANIFEST_DIR"), "tests", "fixtures", name]
        .iter()
        .collect()
}

fn people_schema() -> Schema {
    Schema::from_json(&std::fs::read(fixture("people.schema.json")).unwrap()).unwrap()
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
fn every_encoding_reads_to_the_expected_records() {
    let expected: Value =
        serde_json::from_slice(&std::fs::read(fixture("people.expected.json")).unwrap()).unwrap();
    for (file, encoding) in [
        ("people.utf-8.txt", Encoding::Utf8),
        ("people.cp1252.txt", Encoding::Cp1252),
        ("people.cp850.txt", Encoding::Cp850),
    ] {
        let options = ReadOptions::new(people_schema()).with_encoding(encoding);
        let batch = fwf::read(fixture(file), &options).unwrap();
        assert_eq!(rows(&batch), expected, "{file}");
    }
}

#[test]
fn the_wrong_encoding_names_the_bad_byte() {
    // cp850 writes é as 0x82, which isn't valid UTF-8.
    let options = ReadOptions::new(people_schema());
    let err = fwf::read(fixture("people.cp850.txt"), &options).unwrap_err();
    let Error::InvalidByte { position, byte, .. } = &err else {
        panic!("{err:?}")
    };
    assert_eq!(
        (
            position.record,
            position.field.as_str(),
            position.byte,
            *byte
        ),
        (1, "name", 9, 0x82)
    );
}

#[test]
fn a_missing_file_is_an_io_error() {
    let options = ReadOptions::new(people_schema());
    let err = fwf::read(fixture("no-such-file.txt"), &options).unwrap_err();
    assert!(matches!(err, Error::Io { .. }), "{err:?}");
}
