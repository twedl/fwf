use std::sync::Arc;

use rayon::{ThreadPool, ThreadPoolBuilder};

use crate::{Container, Encoding, Error, Result, Schema};

/// About 1 MiB, inside the 1–4 MB range where filling a column at a time
/// measured fastest: small enough that a chunk's lines and columns stay in
/// cache.
const CHUNK_SIZE: usize = 1 << 20;

/// How to read a fixed-width file.
#[derive(Debug, Clone)]
pub struct ReadOptions {
    pub(crate) schema: Schema,
    pub(crate) encoding: Encoding,
    pub(crate) container: Container,
    pub(crate) entry: Option<String>,
    /// Indexes into the schema's fields, in output order.
    pub(crate) columns: Option<Vec<usize>>,
    pub(crate) skip_rows: usize,
    pub(crate) n_rows: Option<usize>,
    pub(crate) chunk_size: usize,
    pub(crate) pool: Option<Arc<ThreadPool>>,
}

impl ReadOptions {
    /// Reads records laid out by `schema`. There is no default encoding: cp1252
    /// and cp850 both decode almost any byte, so a wrong guess would be silent.
    pub fn new(schema: Schema, encoding: Encoding) -> ReadOptions {
        ReadOptions {
            schema,
            encoding,
            container: Container::Auto,
            entry: None,
            columns: None,
            skip_rows: 0,
            n_rows: None,
            chunk_size: CHUNK_SIZE,
            pool: None,
        }
    }

    /// Overrides detecting the container from the input's first bytes.
    pub fn with_container(mut self, container: Container) -> ReadOptions {
        self.container = container;
        self
    }

    /// Reads the zip member with exactly this name. Without an entry, a zip
    /// must hold exactly one file.
    pub fn with_entry(mut self, name: impl Into<String>) -> ReadOptions {
        self.entry = Some(name.into());
        self
    }

    /// Reads only these fields, in this order. A name the schema doesn't have,
    /// or one given twice, is an error.
    pub fn with_columns<'a>(
        mut self,
        names: impl IntoIterator<Item = &'a str>,
    ) -> Result<ReadOptions> {
        let fields = self.schema.fields();
        let mut columns = Vec::new();
        for name in names {
            let unknown = || Error::UnknownColumn {
                name: name.to_owned(),
            };
            let column =
                (fields.iter().position(|field| field.name == name)).ok_or_else(unknown)?;
            if columns.contains(&column) {
                return Err(Error::DuplicateName {
                    field: name.to_owned(),
                });
            }
            columns.push(column);
        }
        self.columns = Some(columns);
        Ok(self)
    }

    /// Skips the first `n` lines of the input, such as a header. Skipped lines
    /// aren't parsed, and records in errors are still counted from the first
    /// line.
    pub fn with_skip_rows(mut self, n: usize) -> ReadOptions {
        self.skip_rows = n;
        self
    }

    /// Stops after `n` records. Nothing past them is parsed.
    pub fn with_n_rows(mut self, n: usize) -> ReadOptions {
        self.n_rows = Some(n);
        self
    }

    /// Parses the input in chunks of about `bytes` (default 1 MiB), cut after a
    /// line ending; each chunk becomes one record batch. A stream is read a
    /// block of this size at a time.
    ///
    /// # Panics
    ///
    /// If `bytes` is 0.
    pub fn with_chunk_size(mut self, bytes: usize) -> ReadOptions {
        assert!(bytes > 0, "chunk size must be at least 1 byte");
        self.chunk_size = bytes;
        self
    }

    /// Parses on a pool of `n` threads instead of rayon's global pool. The pool
    /// starts here, and every read with these options shares it.
    pub fn with_n_threads(mut self, n: usize) -> ReadOptions {
        let pool = ThreadPoolBuilder::new().num_threads(n).build();
        // rayon's global pool panics too if it can't start its threads.
        self.pool = Some(Arc::new(pool.expect("rayon can start its threads")));
        self
    }
}
