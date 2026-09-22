//! Extract one fixed-width member from a zip archive and write it to Parquet.
//!
//! See README.md for the contract this implements.

mod encoding;
mod record;
mod schema;
mod tables;
mod writer;

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::Parser;

use crate::encoding::Encoding;
use crate::record::Record;
use crate::writer::Writer;

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

    /// Path to the JSON schema: [[name, position, length, "Char"|"Num"], ...].
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
            names.join(", ")
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
        // Not `.lines()`: the bytes are not UTF-8 until we decode them ourselves.
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        line_number += 1;
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        if line.last() == Some(&b'\r') {
            line.pop();
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
    use super::thousands;

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
}
