//! Extract one fixed-width member from a zip archive and write it to Parquet.
//!
//! See README.md for the contract this implements.

mod encoding;
mod record;
mod schema;
mod tables;
mod writer;

use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::Parser;

use crate::encoding::Encoding;
use crate::record::Record;
use crate::writer::Writer;

/// A UTF-8 byte order mark. Stripped from the first record as bytes, before
/// decoding: left in place it shifts every field of that record, and under a
/// single-byte encoding it decodes to three visible characters rather than one.
const BOM: [u8; 3] = [0xef, 0xbb, 0xbf];

/// DOS end-of-file marker. Some members produced on DOS-era systems end with a
/// stray 0x1A, which `trim` does not remove — it is not whitespace — so without
/// this it would reach Parquet as a final one-character row.
const DOS_EOF: u8 = 0x1a;

/// Longest single record to read before giving up, as a multiple of the widest
/// the schema's last field could occupy. Bounds memory when a member has no line
/// terminators, while leaving generous room for records that run well past the
/// last field the schema extracts — those are legal and must still parse.
///
/// Below this bound a member with no terminators cannot be told apart from one
/// long record, and is read as one. Only the bound makes it detectable, which is
/// enough in practice: an export with no terminators is not a few hundred bytes.
const MAX_RECORD_MULTIPLE: usize = 8;
const MIN_RECORD_CAP: usize = 64 * 1024;

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Extract one fixed-width member from a zip archive and write it to Parquet."
)]
struct Args {
    /// Path to the zip archive.
    #[arg(long, value_name = "PATH")]
    zip: PathBuf,

    /// Exact member name within the archive.
    #[arg(long, value_name = "NAME")]
    member: String,

    /// Path to the JSON schema: [{"name": .., "at": [position, length], "type": ..}, ...].
    #[arg(long, value_name = "PATH")]
    schema: PathBuf,

    /// Character encoding of the member.
    #[arg(long, value_enum, value_name = "ENC")]
    encoding: Encoding,

    /// Output Parquet path. Overwritten if it exists.
    #[arg(long, value_name = "PATH")]
    out: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let fields = schema::load(&args.schema)?;

    // Only used to bound a single read. Nothing validates a record against it.
    // Converted to bytes via the encoding, so the margin means the same thing
    // whether a character is one byte or four. Columns the record does not carry
    // have no span and so say nothing about how long a record is; a schema of
    // nothing but those falls back to the floor below.
    let record_length = fields
        .iter()
        .filter_map(|field| field.at.as_ref().map(schema::Span::end))
        .max()
        .unwrap_or(0);
    let cap = record_length
        .saturating_mul(args.encoding.max_bytes_per_char())
        .saturating_mul(MAX_RECORD_MULTIPLE)
        .max(MIN_RECORD_CAP) as u64;

    let file = File::open(&args.zip)
        .with_context(|| format!("opening {}", args.zip.display()))?;
    let mut archive = zip::ZipArchive::new(file)
        .with_context(|| format!("reading {} as a zip archive", args.zip.display()))?;

    // Checked up front so a miss can name what the archive does hold. Collected
    // to owned strings to release the borrow before `by_name` takes it mutably.
    let names: Vec<String> = archive.file_names().map(str::to_owned).collect();
    if !names.iter().any(|name| name == &args.member) {
        bail!(
            "no member `{}` in {}; archive contains: {}",
            args.member,
            args.zip.display(),
            summarize(&names)
        );
    }
    let member = archive
        .by_name(&args.member)
        .with_context(|| format!("reading member `{}`", args.member))?;

    let mut reader = BufReader::new(member);
    let mut writer = Writer::create(&args.out, fields)?;
    let mut record = Record::new();
    let mut line: Vec<u8> = Vec::new();
    let mut line_number = 0usize;

    loop {
        line.clear();
        // Not `.lines()`: these bytes are not UTF-8 until we decode them. The
        // `take` bounds one record, so a member with no terminators fails here
        // instead of being pulled into memory whole and parsed as a single row.
        let read = reader.by_ref().take(cap).read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        let terminated = line.last() == Some(&b'\n');
        if terminated {
            line.pop();
        } else if read as u64 == cap {
            bail!(
                "`{}` reached {cap} bytes with no line terminator. Records must be \
                 newline delimited; a member of fixed-length records run together \
                 with no terminators is not supported.",
                args.member
            );
        }

        // Whether anything follows this record. Peeking the buffer answers that
        // directly, rather than inferring it from whether a terminator was
        // found — which only tells you about the last read, not the member.
        let at_end = reader.fill_buf()?.is_empty();

        if at_end {
            // At the very end of a member a carriage return and a DOS end-of-file
            // marker can sit in either order — `...\r\x1a` and `\x1a\r` both occur
            // — and neither is data. Anywhere else, a lone 0x1A is data and stays.
            while matches!(line.last(), Some(&(DOS_EOF | b'\r'))) {
                line.pop();
            }
            // Nothing left once that came off: this was the member's tail, not a
            // record.
            if line.is_empty() {
                break;
            }
        } else if line.last() == Some(&b'\r') {
            line.pop();
        }

        line_number += 1;
        if line_number == 1 && line.starts_with(&BOM) {
            line.drain(..BOM.len());
        }

        record
            .fill(&line, args.encoding)
            .with_context(|| format!("line {line_number}"))?;
        writer.push(&record)?;
    }

    let rows = writer.finish()?;
    eprintln!("wrote {} ({} rows)", args.out.display(), thousands(rows));
    Ok(())
}

/// Archive listings go into an error message that a Python caller captures from
/// stderr, so a typo against a large archive must not emit megabytes of names.
fn summarize(names: &[String]) -> String {
    const MAX: usize = 20;
    if names.len() <= MAX {
        return names.join(", ");
    }
    format!(
        "{}, and {} more",
        names[..MAX].join(", "),
        names.len() - MAX
    )
}

fn thousands(n: usize) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, digit) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{summarize, thousands};

    #[test]
    fn groups_digits_in_threes() {
        for (n, want) in [
            (0usize, "0"),
            (7, "7"),
            (999, "999"),
            (1_000, "1,000"),
            (1_204_331, "1,204,331"),
        ] {
            assert_eq!(thousands(n), want);
        }
    }

    #[test]
    fn lists_every_name_when_there_are_few() {
        let names = vec!["a.dat".to_string(), "b.dat".to_string()];
        assert_eq!(summarize(&names), "a.dat, b.dat");
    }

    #[test]
    fn caps_a_long_archive_listing() {
        let names: Vec<String> = (0..500).map(|i| format!("member{i:03}.dat")).collect();
        let summary = summarize(&names);
        assert!(summary.ends_with("and 480 more"), "got: {summary}");
        assert!(summary.len() < 400, "listing should stay short, got {} bytes", summary.len());
    }
}
