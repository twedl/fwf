use std::sync::Arc;

use arrow_array::{Array, RecordBatch, RecordBatchOptions};
use arrow_schema::{Field as ArrowField, Schema as ArrowSchema};

use crate::builder::Builder;
use crate::{Position, ReadOptions, Result, field, framing};

/// Parses one unit's bytes into a record batch, a record at a time, pushing
/// each field into its column's builder.
pub(crate) fn parse(unit: &str, bytes: &[u8], options: &ReadOptions) -> Result<RecordBatch> {
    let fields = options.schema.fields();
    let mut builders: Vec<Builder> = fields.iter().map(|f| Builder::new(f.dtype)).collect();
    let mut scratch = String::new();
    let mut records = 0;
    for (line_start, line) in framing::lines(framing::strip_eof_marker(bytes)) {
        records += 1;
        for (field, builder) in fields.iter().zip(&mut builders) {
            let (at, value) = field::value(line, field.start, field.len);
            if value.is_empty() {
                builder.append_null();
                continue;
            }
            builder.append(value, options.encoding, &mut scratch, |offset| Position {
                unit: unit.to_owned(),
                record: records,
                field: field.name.clone(),
                byte: line_start + at + offset,
            })?;
        }
    }

    let columns: Vec<_> = builders.iter_mut().map(Builder::finish).collect();
    let arrow_fields: Vec<ArrowField> = (fields.iter().zip(&columns))
        .map(|(f, column)| ArrowField::new(&f.name, column.data_type().clone(), true))
        .collect();
    // The row count keeps a schema with no fields from failing.
    let batch_options = RecordBatchOptions::new().with_row_count(Some(records));
    let schema = Arc::new(ArrowSchema::new(arrow_fields));
    Ok(
        RecordBatch::try_new_with_options(schema, columns, &batch_options)
            .expect("each field has its column"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Encoding, Schema};

    fn read(json: &str, bytes: &[u8]) -> Result<RecordBatch> {
        let schema = Schema::from_json(json.as_bytes()).unwrap();
        parse(
            "test.txt",
            bytes,
            &ReadOptions::new(schema, Encoding::Cp1252),
        )
    }

    #[test]
    fn reports_where_a_float_fails() {
        let json = r#"{"fields": [
            {"name": "name", "position": 1, "length": 4},
            {"name": "amount", "position": 5, "length": 6, "type": "Float64"}
        ]}"#;
        let err = read(json, b"ab     1.5\ncd    1,50\n").unwrap_err();
        assert_eq!(
            err.to_string(),
            r#"test.txt: record 2, field "amount" (byte 17): "1,50" is not a Float64"#
        );
        // The bad value is shown in the file's encoding: 0xBD is ½ in cp1252.
        let err = read(json, b"ab    1\xBD\n").unwrap_err();
        assert!(
            err.to_string().ends_with(r#""1½" is not a Float64"#),
            "{err}"
        );
    }

    #[test]
    fn a_schema_with_no_fields_still_counts_records() {
        let batch = read(r#"{"fields": []}"#, b"a\nb\n").unwrap();
        assert_eq!((batch.num_columns(), batch.num_rows()), (0, 2));
    }
}
