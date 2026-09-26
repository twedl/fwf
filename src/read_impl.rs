use std::sync::Arc;

use arrow_array::{Array, RecordBatch, RecordBatchOptions};
use arrow_schema::{Field as ArrowField, Schema as ArrowSchema};

use crate::builder::Builder;
use crate::field::Spans;
use crate::{Position, ReadOptions, Result, framing};

/// Parses one unit's bytes into a record batch, a record at a time, pushing
/// each field into its column's builder.
pub(crate) fn parse(unit: &str, bytes: &[u8], options: &ReadOptions) -> Result<RecordBatch> {
    let fields = options.schema.fields();
    let encoding = options.encoding;
    let spans = Spans::new(fields);
    let mut builders: Vec<Builder> = fields.iter().map(|f| Builder::new(f.dtype)).collect();
    let mut offsets = Vec::with_capacity(spans.boundaries().len());
    let mut scratch = String::new();
    let mut records = 0;
    let (body_start, body) = framing::unit_body(bytes, encoding);
    for (line_start, line) in framing::lines(body) {
        records += 1;
        offsets.clear();
        encoding.byte_offsets(line, spans.boundaries(), &mut offsets);
        for (i, (field, builder)) in fields.iter().zip(&mut builders).enumerate() {
            let (at, value) = spans.value(i, line, &offsets);
            if value.is_empty() {
                builder.append_null();
                continue;
            }
            builder.append(value, encoding, &mut scratch, |offset| Position {
                unit: unit.to_owned(),
                record: records,
                field: field.name.clone(),
                byte: body_start + line_start + at + offset,
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

    fn read(json: &str, bytes: &[u8], encoding: Encoding) -> Result<RecordBatch> {
        let schema = Schema::from_json(json.as_bytes()).unwrap();
        parse(
            "test.txt",
            bytes,
            &ReadOptions::new(schema).with_encoding(encoding),
        )
    }

    const NAME_AMOUNT: &str = r#"{"fields": [
        {"name": "name", "position": 1, "length": 4},
        {"name": "amount", "position": 5, "length": 6, "type": "Float64"}
    ]}"#;

    #[test]
    fn reports_where_a_float_fails() {
        let err = read(NAME_AMOUNT, b"ab     1.5\ncd    1,50\n", Encoding::Utf8).unwrap_err();
        assert_eq!(
            err.to_string(),
            r#"test.txt: record 2, field "amount" (byte 17): "1,50" is not a Float64"#
        );
    }

    #[test]
    fn positions_count_the_bom() {
        let err = read(NAME_AMOUNT, b"\xEF\xBB\xBFJos\xE9   1.5\n", Encoding::Utf8).unwrap_err();
        assert_eq!(
            err.to_string(),
            r#"test.txt: record 1, field "name" (byte 6): byte 0xE9 is not valid UTF-8; is the file cp1252 or cp850?"#
        );
    }

    #[test]
    fn a_schema_with_no_fields_still_counts_records() {
        let batch = read(r#"{"fields": []}"#, b"a\nb\n", Encoding::Utf8).unwrap();
        assert_eq!((batch.num_columns(), batch.num_rows()), (0, 2));
    }
}
