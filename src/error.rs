use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    /// The schema isn't valid JSON, or a field is missing `name`, `position` or `length`.
    SchemaJson(serde_json::Error),
    /// A field's `type` is not one of the supported polars type names.
    UnknownType { field: String, type_name: String },
    /// A field's `position` is 0; positions start at 1.
    ZeroPosition { field: String },
    /// A field's `length` is 0.
    ZeroLength { field: String },
    /// Two fields have the same name.
    DuplicateName { field: String },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::SchemaJson(e) => write!(f, "invalid schema: {e}"),
            Error::UnknownType { field, type_name } => write!(
                f,
                "field {field:?}: unknown type {type_name:?} (expected \"String\" or \"Float64\")"
            ),
            Error::ZeroPosition { field } => write!(
                f,
                "field {field:?}: position is 0, but positions start at 1"
            ),
            Error::ZeroLength { field } => write!(f, "field {field:?}: length is 0"),
            Error::DuplicateName { field } => write!(f, "field {field:?} appears more than once"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::SchemaJson(e) => Some(e),
            _ => None,
        }
    }
}
