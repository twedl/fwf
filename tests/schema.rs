use fwf::{DataType, Error, Field, Schema};

fn load(name: &str) -> fwf::Result<Schema> {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    Schema::from_json(&std::fs::read(path).unwrap())
}

fn field(name: &str, start: usize, len: usize, dtype: DataType) -> Field {
    Field {
        name: name.into(),
        start,
        len,
        dtype,
    }
}

#[test]
fn loads_people_schema() {
    let schema = load("people.schema.json").unwrap();
    assert_eq!(
        schema.fields(),
        [
            field("id", 0, 6, DataType::String),
            field("name", 6, 20, DataType::String),
            field("city", 26, 15, DataType::String),
            field("born", 41, 8, DataType::String),
            field("amount", 49, 10, DataType::Float64),
            field("code", 59, 2, DataType::String),
        ]
    );
}

#[test]
fn rejects_unknown_type() {
    let err = load("people.schema.unknown-type.json").unwrap_err();
    assert!(
        matches!(&err, Error::UnknownType { field, type_name } if field == "amount" && type_name == "Flaot64"),
        "{err:?}"
    );
    assert_eq!(
        err.to_string(),
        r#"field "amount": unknown type "Flaot64" (expected "String" or "Float64")"#
    );
}
