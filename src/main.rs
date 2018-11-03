#[macro_use]
extern crate serde_derive;
extern crate docopt;
extern crate rsgen_avro;

use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::stdout;
use std::path::Path;
use std::process;
use std::process::Command;

use docopt::Docopt;
use rsgen_avro::{Generator, Source};

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

const USAGE: &'static str = "
Usage:
  rsgen-avro [options] <schema-file-or-dir> <output-file> 
  rsgen-avro (-h | --help)
  rsgen-avro (-V | --version)

Options:
  --fmt          Run rustfmt on the resulting <output-file>
  --nullable     Replace null fields with their default value
                 when deserializing.
  --precision=P  Precision for f32/f64 default values
                 that aren't round numbers [default: 3].
  --append       Open <output-file> in append mode.
                 By default it is truncated.
  --add-imports  Add 'extern crate ...' at the top of <output-file>.
  -V, --version  Show version.
  -h, --help     Show this screen.
";

#[derive(Debug, Deserialize)]
struct CmdArgs {
    arg_schema_file_or_dir: String,
    arg_output_file: String,
    flag_fmt: bool,
    flag_nullable: bool,
    flag_precision: Option<usize>,
    flag_append: bool,
    flag_add_imports: bool,
    flag_version: bool,
}

fn main() {
    let args: CmdArgs = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());

    if args.flag_version {
        println!("{}", VERSION);
        process::exit(0);
    }

    let p = Path::new(&args.arg_schema_file_or_dir);
    if !p.exists() {
        eprintln!("Doesn't exist: {:?}", p);
        process::exit(1);
    }

    let source = if p.is_dir() {
        Source::DirPath(p)
    } else {
        Source::FilePath(p)
    };

    let mut out: Box<Write> = if &args.arg_output_file == "-" {
        Box::new(stdout())
    } else {
        let mut open_opts = OpenOptions::new();
        if args.flag_append {
            open_opts.write(true).create(true).append(true)
        } else {
            open_opts.write(true).create(true).truncate(true)
        };
        Box::new(open_opts.open(&args.arg_output_file).unwrap_or_else(|e| {
            eprintln!("Output file error: {}", e);
            process::exit(1);
        }))
    };

    let g = Generator::builder()
        .precision(args.flag_precision.unwrap())
        .add_imports(args.flag_add_imports)
        .nullable(args.flag_nullable)
        .build()
        .unwrap_or_else(|e| {
            eprintln!("Problem during prepartion: {}", e);
            process::exit(1);
        });

    g.gen(&source, &mut out).unwrap_or_else(|e| {
        eprintln!("Problem during code generation: {}", e);
        process::exit(1);
    });

    if args.flag_fmt && &args.arg_output_file != "-" {
        Command::new("rustfmt")
            .arg(&args.arg_output_file)
            .status()
            .unwrap_or_else(|e| {
                eprintln!("Problem with rustfmt: {}", e);
                process::exit(1);
            });
    }
}
