use std::path::PathBuf;

use arrow_array::{Array, Float64Array, RecordBatch, StringArray};
use fwf::{Encoding, Error, ReadOptions, Schema};
use serde_json::{Map, Value};

fn fixture(name: &str) -> PathBuf {
    [env!("CARGO_MANIFEST_DIR"), "tests", "fixtures", name]
        .iter()
        .collect()
}

fn people(encoding: Encoding) -> ReadOptions {
    let json = std::fs::read(fixture("people.schema.json")).unwrap();
    ReadOptions::new(Schema::from_json(&json).unwrap(), encoding)
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
        ("people.cp1252.txt", Encoding::Cp1252),
        ("people.cp850.txt", Encoding::Cp850),
    ] {
        let batch = fwf::read(fixture(file), &people(encoding)).unwrap();
        assert_eq!(rows(&batch), expected, "{file}");
    }
}

#[test]
fn cp850_read_as_cp1252_names_the_bad_byte() {
    // cp850 writes the ü in "Zoë Müller" as 0x81, which cp1252 leaves undefined.
    let err = fwf::read(fixture("people.cp850.txt"), &people(Encoding::Cp1252)).unwrap_err();
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
