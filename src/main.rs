use std::error::Error;
use std::fs::OpenOptions;
use std::io::{prelude::*, stdout};
use std::path::PathBuf;
use std::process::{self, Command};

use clap::Parser;
use rsgen_avro::{Generator, ImplementAvroSchema, Source};

/// Generate Rust types from Avro schemas
#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    /// Glob pattern to select Avro schema files
    pub glob_pattern: String,

    /// The file where Rust types will be written, '-' for stdout
    pub output_file: PathBuf,

    /// Run rustfmt on the resulting <output-file>
    #[clap(long)]
    pub fmt: bool,

    /// Replace null fields with their default value when deserializing
    #[clap(long)]
    pub nullable: bool,

    /// Precision for f32/f64 default values that aren't round numbers
    #[clap(long, value_name = "P", default_value_t = 3)]
    pub precision: usize,

    /// Custom deserialization for apache-avro multi-valued union types
    #[clap(long)]
    pub union_deser: bool,

    /// Use chrono::NaiveDateTime for date/timestamps logical types
    #[clap(long)]
    pub chrono_dates: bool,

    /// Derive builders for generated record structs
    #[clap(long)]
    pub derive_builders: bool,

    /// Implement AvroSchema for generated record structs
    #[clap(long, value_name = "METHOD", default_value_t, value_enum)]
    pub impl_schemas: ImplementAvroSchema,

    /// Extract Derives for generated record structs, comma separated, e.g. `std::fmt::Display,std::string::ToString`
    #[clap(long, value_name = "DERIVES", value_delimiter = ',')]
    pub extra_derives: Vec<String>,
}

fn run() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let mut out: Box<dyn Write> = if args.output_file == PathBuf::from("-") {
        Box::new(stdout())
    } else {
        Box::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&args.output_file)?,
        )
    };

    let source = Source::GlobPattern(&args.glob_pattern);

    let g = Generator::builder()
        .precision(args.precision)
        .nullable(args.nullable)
        .use_avro_rs_unions(args.union_deser)
        .use_chrono_dates(args.chrono_dates)
        .derive_builders(args.derive_builders)
        .implement_avro_schema(args.impl_schemas)
        .extra_derives(args.extra_derives)
        .build()?;

    g.generate(&source, &mut out)?;

    if args.fmt && args.output_file != PathBuf::from("-") {
        Command::new("rustfmt").arg(&args.output_file).status()?;
    }

    Ok(())
}

fn main() {
    run().unwrap_or_else(|e| {
        eprintln!("{e}");
        process::exit(1);
    });
}
