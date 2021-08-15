use std::error::Error;
use std::fs::OpenOptions;
use std::io::{prelude::*, stdout};
use std::process::{self, Command};

use docopt::Docopt;
use rsgen_avro::{Generator, Source};
use serde::Deserialize;

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

const USAGE: &'static str = "
Usage:
  rsgen-avro [options] <schema-file-pattern> <output-file>
  rsgen-avro (-h | --help)
  rsgen-avro (-V | --version)

Options:
  --fmt             Run rustfmt on the resulting <output-file>
  --nullable        Replace null fields with their default value when deserializing.
  --precision=P     Precision for f32/f64 default values that aren't round numbers [default: 3].
  --variant-access  Derive the traits in the variant_access_traits crate on union types.
  --union-deser     Custom deserialization for avro-rs multi-valued union types.
  -V, --version     Show version.
  -h, --help        Show this screen.
";

#[derive(Debug, Deserialize)]
struct CmdArgs {
    arg_schema_file_pattern: String,
    arg_output_file: String,
    flag_fmt: bool,
    flag_nullable: bool,
    flag_precision: Option<usize>,
    flag_variant_access: bool,
    flag_union_deser: bool,
    flag_version: bool,
}

fn run() -> Result<(), Box<dyn Error>> {
    let args: CmdArgs = Docopt::new(USAGE).and_then(|d| d.deserialize())?;

    if args.flag_version {
        println!("{}", VERSION);
        return Ok(());
    }

    let mut out: Box<dyn Write> = if &args.arg_output_file == "-" {
        Box::new(stdout())
    } else {
        Box::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&args.arg_output_file)?,
        )
    };

    let source = Source::GlobPattern(&args.arg_schema_file_pattern);

    let g = Generator::builder()
        .precision(args.flag_precision.unwrap())
        .nullable(args.flag_nullable)
        .use_variant_access(args.flag_variant_access)
        .use_avro_rs_unions(args.flag_union_deser)
        .build()?;

    g.gen(&source, &mut out)?;

    if args.flag_fmt && &args.arg_output_file != "-" {
        Command::new("rustfmt")
            .arg(&args.arg_output_file)
            .status()?;
    }

    Ok(())
}

fn main() {
    run().unwrap_or_else(|e| {
        eprintln!("{}", e);
        process::exit(1);
    });
}
