use serde::Deserialize;

use crate::{Error, Result};

/// The type a field is parsed into, named as in polars.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    String,
    Float64,
}

/// One field of a record: where it sits in the line and what it parses into.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    /// 0-based offset of the field's first character.
    pub start: usize,
    /// Width in characters.
    pub len: usize,
    pub dtype: DataType,
}

/// The fields of a fixed-width file, in schema order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    fields: Vec<Field>,
}

impl Schema {
    /// Parses a JSON schema: `{"fields": [{"name", "position", "length", "type"?}, ...]}`.
    ///
    /// `position` is 1-based. A missing `type` means `String`. Keys other than
    /// these are ignored.
    pub fn from_json(json: &[u8]) -> Result<Schema> {
        let raw: RawSchema = serde_json::from_slice(json).map_err(Error::SchemaJson)?;
        let mut fields: Vec<Field> = Vec::with_capacity(raw.fields.len());
        for field in raw.fields {
            let dtype = match field.dtype.as_deref() {
                None | Some("String") => DataType::String,
                Some("Float64") => DataType::Float64,
                Some(other) => {
                    return Err(Error::UnknownType {
                        type_name: other.to_owned(),
                        field: field.name,
                    });
                }
            };
            if field.position == 0 {
                return Err(Error::ZeroPosition { field: field.name });
            }
            if field.length == 0 {
                return Err(Error::ZeroLength { field: field.name });
            }
            if fields.iter().any(|f| f.name == field.name) {
                return Err(Error::DuplicateName { field: field.name });
            }
            fields.push(Field {
                name: field.name,
                start: field.position as usize - 1,
                len: field.length as usize,
                dtype,
            });
        }
        Ok(Schema { fields })
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
    }
}

#[derive(Deserialize)]
struct RawSchema {
    fields: Vec<RawField>,
}

#[derive(Deserialize)]
struct RawField {
    name: String,
    // u32 keeps start + len from overflowing usize.
    position: u32,
    length: u32,
    #[serde(rename = "type")]
    dtype: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: &str) -> Result<Schema> {
        Schema::from_json(json.as_bytes())
    }

    #[test]
    fn ignores_unknown_top_level_keys() {
        let json = r#"{"version": 2, "fields": [{"name": "a", "position": 1, "length": 1}]}"#;
        assert_eq!(parse(json).unwrap().fields().len(), 1);
    }

    #[test]
    fn type_names_are_case_sensitive() {
        let json = r#"{"fields": [{"name": "a", "position": 1, "length": 1, "type": "float64"}]}"#;
        assert!(matches!(parse(json), Err(Error::UnknownType { .. })));
    }

    #[test]
    fn rejects_position_zero() {
        let json = r#"{"fields": [{"name": "a", "position": 0, "length": 1}]}"#;
        assert!(matches!(parse(json), Err(Error::ZeroPosition { field }) if field == "a"));
    }

    #[test]
    fn rejects_length_zero() {
        let json = r#"{"fields": [{"name": "a", "position": 1, "length": 0}]}"#;
        assert!(matches!(parse(json), Err(Error::ZeroLength { field }) if field == "a"));
    }

    #[test]
    fn rejects_duplicate_names() {
        let json = r#"{"fields": [
            {"name": "a", "position": 1, "length": 1},
            {"name": "a", "position": 2, "length": 1}
        ]}"#;
        assert!(matches!(parse(json), Err(Error::DuplicateName { field }) if field == "a"));
    }

    #[test]
    fn allows_gaps_and_overlaps() {
        let json = r#"{"fields": [
            {"name": "date", "position": 1, "length": 8},
            {"name": "year", "position": 1, "length": 4},
            {"name": "code", "position": 20, "length": 2}
        ]}"#;
        assert_eq!(parse(json).unwrap().fields().len(), 3);
    }

    #[test]
    fn rejects_missing_length() {
        let json = r#"{"fields": [{"name": "a", "position": 1}]}"#;
        assert!(matches!(parse(json), Err(Error::SchemaJson(_))));
    }

    #[test]
    fn rejects_negative_position() {
        let json = r#"{"fields": [{"name": "a", "position": -1, "length": 1}]}"#;
        assert!(matches!(parse(json), Err(Error::SchemaJson(_))));
    }
}
