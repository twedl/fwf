//! The field layout, read from a JSON array of `[name, position, length, type]`.

use std::collections::HashSet;
use std::path::Path;

use anyhow::{bail, Context, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Char,
    Num,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    /// 1-based character position of the field's first character.
    pub position: usize,
    /// Field length in characters.
    pub length: usize,
    pub kind: Kind,
}

impl Field {
    /// 0-based character index of the first character.
    pub fn start(&self) -> usize {
        self.position - 1
    }

    /// 0-based character index one past the last character.
    pub fn end(&self) -> usize {
        self.position - 1 + self.length
    }
}

pub fn load(path: &Path) -> Result<Vec<Field>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading schema {}", path.display()))?;
    parse(&text).with_context(|| format!("in schema {}", path.display()))
}

pub fn parse(text: &str) -> Result<Vec<Field>> {
    let rows: Vec<(String, i64, i64, String)> = serde_json::from_str(text)
        .context("expected a JSON array of [name, position, length, type] arrays")?;

    if rows.is_empty() {
        bail!("schema contains no fields");
    }

    let mut seen = HashSet::new();
    let mut fields = Vec::with_capacity(rows.len());

    for (name, position, length, kind) in rows {
        if position < 1 {
            bail!("field `{name}`: position must be 1 or greater, got {position}");
        }
        if length < 1 {
            bail!("field `{name}`: length must be 1 or greater, got {length}");
        }
        let kind = match kind.as_str() {
            "Char" => Kind::Char,
            "Num" => Kind::Num,
            other => bail!("field `{name}`: type must be \"Char\" or \"Num\", got \"{other}\""),
        };
        if !seen.insert(name.clone()) {
            bail!("duplicate column name `{name}`");
        }
        fields.push(Field {
            name,
            position: position as usize,
            length: length as usize,
            kind,
        });
    }

    Ok(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_well_formed_schema() {
        let fields = parse(r#"[["id", 1, 8, "Char"], ["amount", 10, 13, "Num"]]"#).unwrap();
        assert_eq!(
            fields,
            vec![
                Field { name: "id".into(), position: 1, length: 8, kind: Kind::Char },
                Field { name: "amount".into(), position: 10, length: 13, kind: Kind::Num },
            ]
        );
    }

    #[test]
    fn start_and_end_are_zero_based_half_open() {
        let f = Field { name: "x".into(), position: 9, length: 2, kind: Kind::Char };
        assert_eq!((f.start(), f.end()), (8, 10));
    }

    #[test]
    fn rejects_bad_schemas() {
        let cases = [
            (r#"[]"#, "no fields"),
            (r#"[["a", 0, 4, "Char"]]"#, "position must be"),
            (r#"[["a", 1, 0, "Char"]]"#, "length must be"),
            (r#"[["a", 1, 4, "Text"]]"#, "must be \"Char\" or \"Num\""),
            (r#"[["a", 1, 4, "Char"], ["a", 6, 4, "Num"]]"#, "duplicate column name"),
            (r#"[["a", 1, 4]]"#, "expected a JSON array"),
            (r#"{"a": 1}"#, "expected a JSON array"),
        ];
        for (input, want) in cases {
            let err = format!("{:#}", parse(input).unwrap_err());
            assert!(err.contains(want), "input {input}\n  got: {err}\n want: {want}");
        }
    }
}
