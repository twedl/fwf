//! The `fwf` command: converts a cp1252 or cp850 fixed-width file to CSV or
//! Parquet.

use std::fs;
use std::io::{self, IsTerminal};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Parser;
use fwf::{Container, Destination, Encoding, Error, Format, Location, ReadOptions, Schema};

/// Converts a cp1252 or cp850 fixed-width file to CSV or Parquet.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// The file to read: plain text, gzip or zip. '-' is stdin.
    #[arg(default_value = "-")]
    input: PathBuf,

    /// JSON schema giving each field's name, position, length and type.
    #[arg(long, value_name = "FILE")]
    layout: PathBuf,

    /// The input's code page.
    #[arg(long)]
    encoding: Encoding,

    /// How the input is packaged; auto decides from its first bytes.
    #[arg(long, value_name = "FORMAT", default_value = "auto")]
    input_format: Container,

    /// The zip member to read, by its exact name [default: the archive's only file]
    #[arg(long, value_name = "NAME")]
    entry: Option<String>,

    /// Skips the first N lines, such as a header. Record numbers in errors
    /// still count them.
    #[arg(long, value_name = "N", default_value_t = 0)]
    skip_rows: usize,

    /// Stops after N records, counted after the skipped lines.
    #[arg(long, value_name = "N")]
    n_rows: Option<usize>,

    /// The file to write. '-' is stdout.
    #[arg(short, long, default_value = "-")]
    output: PathBuf,

    /// The output format [default: parquet if OUTPUT ends in .parquet, otherwise csv]
    #[arg(long)]
    format: Option<Format>,

    /// Writes Parquet to stdout even when it's a terminal.
    #[arg(long)]
    force: bool,
}

fn main() -> ExitCode {
    if let Err(e) = run(Args::parse()) {
        eprintln!("fwf: {e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let extension = args.output.extension().unwrap_or_default();
    let format = match args.format {
        Some(format) => format,
        None if extension.eq_ignore_ascii_case("parquet") => Format::Parquet,
        None => Format::Csv,
    };
    let to_stdout = args.output == Path::new("-");
    if format == Format::Parquet && to_stdout && io::stdout().is_terminal() && !args.force {
        return Err("won't write Parquet to a terminal; use -o FILE or a pipe, or --force".into());
    }

    let layout = args.layout.display();
    let json = fs::read(&args.layout).map_err(|e| format!("{layout}: {e}"))?;
    let schema = Schema::from_json(&json).map_err(|e| format!("{layout}: {e}"))?;
    let mut options = ReadOptions::new(schema, args.encoding)
        .with_container(args.input_format)
        .with_skip_rows(args.skip_rows);
    if let Some(entry) = args.entry {
        options = options.with_entry(entry);
    }
    if let Some(n) = args.n_rows {
        options = options.with_n_rows(n);
    }

    let location = if args.input == Path::new("-") {
        Location::Stdin
    } else {
        Location::Path(args.input)
    };
    let destination = if to_stdout {
        Destination::Stdout
    } else {
        Destination::Path(args.output)
    };
    match fwf::write(fwf::scan(location, &options)?, format, destination) {
        // The reader of stdout stopped reading, as `head` does.
        Err(Error::Write { source, .. }) if source.kind() == io::ErrorKind::BrokenPipe => Ok(()),
        result => Ok(result?),
    }
}
