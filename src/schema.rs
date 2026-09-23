//! The output schema, read from a JSON array of column objects.
//!
//! Each object declares one column of the Parquet file: its name, its type, and
//! where in the record to read it from. A column with no `at` is declared but
//! not read — it is written, all null, at its place in the column order.

use std::collections::HashSet;
use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

/// The output type of a column, named as polars names it. The vocabulary is
/// deliberately polars' own rather than a fixed-width one: every type here is
/// defined as the polars expression it reproduces, so a schema that says
/// `Float64` says what it does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Kind {
    String,
    Float64,
    Int64,
    /// Parsed with this chrono format string, the same one polars' `to_date`
    /// takes.
    Date(String),
}

/// Where a field sits in the record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Span {
    /// 1-based character position of the field's first character.
    pub position: usize,
    /// Field length in characters.
    pub length: usize,
}

impl Span {
    /// 0-based character index of the first character.
    pub fn start(&self) -> usize {
        self.position - 1
    }

    /// 0-based character index one past the last character.
    pub fn end(&self) -> usize {
        self.position - 1 + self.length
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    /// Where to read the value, or `None` for a column the record does not
    /// carry. Such a column is still declared in the output, and filled with
    /// nulls — which is what lets extracts of layouts that disagree about which
    /// fields exist share one Parquet schema.
    pub at: Option<Span>,
    pub kind: Kind,
}

/// One column object, exactly as it appears in the file. Kept separate from
/// `Field` so serde does the shape checking and this module does the meaning.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    name: String,
    at: Option<(i64, i64)>,
    #[serde(rename = "type")]
    kind: String,
    format: Option<String>,
}

pub fn load(path: &Path) -> Result<Vec<Field>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading schema {}", path.display()))?;
    parse(&text).with_context(|| format!("in schema {}", path.display()))
}

pub fn parse(text: &str) -> Result<Vec<Field>> {
    let rows: Vec<Row> = serde_json::from_str(text).context(
        "expected a JSON array of {\"name\": …, \"at\": [position, length], \"type\": …} objects",
    )?;

    if rows.is_empty() {
        bail!("schema contains no fields");
    }

    let mut seen = HashSet::new();
    let mut fields = Vec::with_capacity(rows.len());

    for row in rows {
        let name = row.name;
        let at = match row.at {
            Some((position, length)) => {
                if position < 1 {
                    bail!("field `{name}`: position must be 1 or greater, got {position}");
                }
                if length < 1 {
                    bail!("field `{name}`: length must be 1 or greater, got {length}");
                }
                Some(Span { position: position as usize, length: length as usize })
            }
            None => None,
        };
        // `format` is paired with the type here rather than checked after it, so
        // that a `format` on the wrong type and an unknown type each report the
        // thing that is actually wrong.
        let kind = match (row.kind.as_str(), row.format) {
            ("String", None) => Kind::String,
            ("Float64", None) => Kind::Float64,
            ("Int64", None) => Kind::Int64,
            ("Date", Some(format)) => Kind::Date(format),
            ("Date", None) => {
                bail!("field `{name}`: a Date field needs a `format`, such as \"%Y%m%d\"")
            }
            (kind @ ("String" | "Float64" | "Int64"), Some(_)) => {
                bail!("field `{name}`: `format` applies only to a Date field, not `{kind}`")
            }
            (other, _) => bail!(
                "field `{name}`: type must be \"String\", \"Float64\", \"Int64\" or \"Date\", \
                 got \"{other}\""
            ),
        };
        if !seen.insert(name.clone()) {
            bail!("duplicate column name `{name}`");
        }
        fields.push(Field { name, at, kind });
    }

    Ok(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(position: usize, length: usize) -> Option<Span> {
        Some(Span { position, length })
    }

    #[test]
    fn parses_a_well_formed_schema() {
        let fields = parse(
            r#"[
                {"name": "id",     "at": [1, 8],   "type": "String"},
                {"name": "count",  "at": [10, 4],  "type": "Int64"},
                {"name": "amount", "at": [15, 13], "type": "Float64"},
                {"name": "opened", "at": [29, 8],  "type": "Date", "format": "%Y%m%d"}
            ]"#,
        )
        .unwrap();
        assert_eq!(
            fields,
            vec![
                Field { name: "id".into(), at: at(1, 8), kind: Kind::String },
                Field { name: "count".into(), at: at(10, 4), kind: Kind::Int64 },
                Field { name: "amount".into(), at: at(15, 13), kind: Kind::Float64 },
                Field {
                    name: "opened".into(),
                    at: at(29, 8),
                    kind: Kind::Date("%Y%m%d".into())
                },
            ]
        );
    }

    /// A column with no `at` is the whole point of the `Option`: it is declared,
    /// typed, and never read.
    #[test]
    fn a_column_without_at_is_declared_but_not_read() {
        let fields = parse(
            r#"[
                {"name": "id",     "at": [1, 8], "type": "String"},
                {"name": "region", "type": "String"},
                {"name": "opened", "type": "Date", "format": "%Y%m%d"}
            ]"#,
        )
        .unwrap();
        assert_eq!(fields[1], Field { name: "region".into(), at: None, kind: Kind::String });
        assert_eq!(fields[2].at, None);
        assert_eq!(fields[2].kind, Kind::Date("%Y%m%d".into()));
    }

    #[test]
    fn start_and_end_are_zero_based_half_open() {
        let span = Span { position: 9, length: 2 };
        assert_eq!((span.start(), span.end()), (8, 10));
    }

    #[test]
    fn rejects_bad_schemas() {
        let cases = [
            (r#"[]"#, "no fields"),
            (r#"[{"name": "a", "at": [0, 4], "type": "String"}]"#, "position must be"),
            (r#"[{"name": "a", "at": [1, 0], "type": "String"}]"#, "length must be"),
            (r#"[{"name": "a", "at": [1, 4], "type": "Text"}]"#, "must be \"String\""),
            (
                r#"[{"name": "a", "at": [1, 4], "type": "String"}, {"name": "a", "type": "Float64"}]"#,
                "duplicate column name",
            ),
            // A Date with no format would silently parse nothing at all.
            (r#"[{"name": "a", "at": [1, 8], "type": "Date"}]"#, "needs a `format`"),
            // A format on anything else is a misunderstanding worth naming.
            (
                r#"[{"name": "a", "at": [1, 4], "type": "Float64", "format": "%Y%m%d"}]"#,
                "applies only to a Date field",
            ),
            // An unknown type reports the type, not the format that rode along.
            (
                r#"[{"name": "a", "at": [1, 4], "type": "Text", "format": "%Y%m%d"}]"#,
                "must be \"String\"",
            ),
            (r#"[{"name": "a", "at": [1, 4, 9], "type": "String"}]"#, "expected a JSON array"),
            (r#"[{"name": "a", "at": [1, 4], "type": "String", "fmt": "x"}]"#, "unknown field"),
            // The old positional form is rejected outright, not half-understood.
            (r#"[["a", 1, 4, "String"]]"#, "expected a JSON array"),
            (r#"{"a": 1}"#, "expected a JSON array"),
        ];
        for (input, want) in cases {
            let err = format!("{:#}", parse(input).unwrap_err());
            assert!(err.contains(want), "input {input}\n  got: {err}\n want: {want}");
        }
    }
}
