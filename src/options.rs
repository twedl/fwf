use globset::{Glob, GlobSet, GlobSetBuilder};

use crate::{Container, Encoding, Error, Result, Schema};

/// How to read a fixed-width file.
#[derive(Debug, Clone)]
pub struct ReadOptions {
    pub(crate) schema: Schema,
    pub(crate) encoding: Encoding,
    pub(crate) container: Container,
    pub(crate) entries: Option<GlobSet>,
}

impl ReadOptions {
    /// Reads records laid out by `schema`. There is no default encoding: cp1252
    /// and cp850 both decode almost any byte, so a wrong guess would be silent.
    pub fn new(schema: Schema, encoding: Encoding) -> ReadOptions {
        ReadOptions {
            schema,
            encoding,
            container: Container::Auto,
            entries: None,
        }
    }

    /// Overrides detecting the container from the input's first bytes.
    pub fn with_container(mut self, container: Container) -> ReadOptions {
        self.container = container;
        self
    }

    /// Reads the zip members whose names match any of these globs, in archive
    /// order. Without entries, a zip must hold exactly one file.
    pub fn with_entries<'a>(
        mut self,
        patterns: impl IntoIterator<Item = &'a str>,
    ) -> Result<ReadOptions> {
        let mut set = GlobSetBuilder::new();
        for pattern in patterns {
            let glob = Glob::new(pattern).map_err(|e| Error::InvalidEntryPattern {
                pattern: pattern.to_owned(),
                message: e.kind().to_string(),
            })?;
            set.add(glob);
        }
        self.entries = Some(set.build().expect("each glob already compiled"));
        Ok(self)
    }
}
