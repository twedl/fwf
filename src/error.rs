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
    /// The input couldn't be read.
    Io {
        unit: String,
        source: std::io::Error,
    },
    /// A byte cp1252 leaves undefined. (cp850 defines all 256.)
    InvalidByte { position: Position, byte: u8 },
    /// A `Float64` field doesn't hold a number.
    InvalidFloat { position: Position, value: String },
}

/// Where in the input a value failed to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Position {
    /// The file the record came from.
    pub unit: String,
    /// 1-based record number within the unit.
    pub record: usize,
    /// The field being read.
    pub field: String,
    /// Offset of the problem within the unit, in bytes.
    pub byte: usize,
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Position {
            unit,
            record,
            field,
            byte,
        } = self;
        write!(f, "{unit}: record {record}, field {field:?} (byte {byte})")
    }
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
            Error::Io { unit, source } => write!(f, "{unit}: {source}"),
            // cp1252's undefined bytes are common letters in cp850 (ü, ì, Å, É, Ø).
            Error::InvalidByte { position, byte } => write!(
                f,
                "{position}: byte 0x{byte:02X} is not valid cp1252; is the file cp850?"
            ),
            Error::InvalidFloat { position, value } => {
                write!(f, "{position}: {value:?} is not a Float64")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::SchemaJson(e) => Some(e),
            Error::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}
