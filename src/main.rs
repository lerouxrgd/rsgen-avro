#[macro_use]
extern crate serde_derive;
extern crate docopt;
extern crate rsgen_avro;

use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::stdout;
use std::process;

use docopt::Docopt;
use rsgen_avro::{Generator, Source};

/*
const USAGE: &'static str = "
Naval Fate.

Usage:
  rsgen-avro ship new <name>...
  rsgen-avro ship <name> move <x> <y> [--speed=<kn>]
  rsgen-avro ship shoot <x> <y>
  rsgen-avro mine (set|remove) <x> <y> [--moored | --drifting]
  rsgen-avro (-h | --help)
  rsgen-avro --version

Options:
  -h --help     Show this screen.
  --version     Show version.
  --speed=<kn>  Speed in knots [default: 10].
  --moored      Moored (anchored) mine.
  --drifting    Drifting mine.
";

#[derive(Debug, Deserialize)]
struct Args {
    flag_speed: isize,
    flag_drifting: bool,
    flag_moored: bool,
    arg_name: Vec<String>,
    arg_x: Option<i32>,
    arg_y: Option<i32>,
    cmd_ship: bool,
    cmd_new: bool,
    cmd_shoot: bool,
    cmd_mine: bool,
    cmd_set: bool,
    cmd_remove: bool,
}
*/

const USAGE: &'static str = "
Usage:
  rsgen-avro (--schema=FILE | --schemas=DIR) --output=FILE [--append --no-extern -p <p>]
  rsgen-avro (-h | --help)
  rsgen-avro --version

Options:
  -h --help       Show this screen.
  --version       Show version.
  --schema=FILE   File containing an Avro schema in json format.
  --schemas=DIR   Directory containing Avro schemas in json format.
  --output=FILE   File where Rust code will be generated. Use '-' for stdout.
  -p <p>          Precision for f32/f64 default values that aren't round numbers [default: 3].
  --append        Only appends to output. By default, tries to overwrite.
  --no-extern     Don't add 'extern crate ...' at the top. By default, will be added.
";

#[derive(Debug, Deserialize)]
struct CmdArgs {
    flag_schema: String,
    flag_schemas: String,
    flag_output: String,
    flag_p: Option<usize>,
    flag_append: bool,
    flag_no_extern: bool,
}

fn main() {
    let args: CmdArgs = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());

    let source = if &args.flag_schema != "" {
        Source::FilePath(&args.flag_schema)
    } else if &args.flag_schemas != "" {
        Source::DirPath(&args.flag_schemas)
    } else {
        eprintln!("Wrong schema source: {:?}", &args);
        process::exit(1);
    };

    let mut out: Box<Write> = if &args.flag_output == "-" {
        Box::new(stdout())
    } else {
        let mut open_opts = OpenOptions::new();
        if args.flag_append {
            open_opts.write(true).create(true).append(true)
        } else {
            open_opts.write(true).create(true).truncate(true)
        };
        Box::new(open_opts.open(&args.flag_output).unwrap_or_else(|e| {
            eprintln!("Output file error: {}", e);
            process::exit(1);
        }))
    };

    let g = Generator::builder()
        .precision(args.flag_p.unwrap_or(3))
        .no_extern(args.flag_no_extern)
        .build()
        .unwrap_or_else(|e| {
            eprintln!("Problem during prepartion: {}", e);
            process::exit(1);
        });

    g.gen(&source, &mut out).unwrap_or_else(|e| {
        eprintln!("Problem during code generation: {}", e);
        process::exit(1);
    });
}
