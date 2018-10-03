#[macro_use]
extern crate serde_derive;
extern crate docopt;
extern crate rsgen_avro;

use docopt::Docopt;
use std::process;

/*
const USAGE: &'static str = "
Naval Fate.

Usage:
  avrogen ship new <name>...
  avrogen ship <name> move <x> <y> [--speed=<kn>]
  avrogen ship shoot <x> <y>
  avrogen mine (set|remove) <x> <y> [--moored | --drifting]
  avrogen (-h | --help)
  avrogen --version

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
  avrogen code --schemas=PATH --output=PATH
  avrogen (-h | --help)
  avrogen --version

Options:
  -h --help       Show this screen.
  --version       Show version.
  --schemas=PATH  Directory containing Avro schemas in json format.
  --output=PATH   Directory where code will be generated.
";

#[derive(Debug, Deserialize)]
struct CmdArgs {
    cmd_code: bool,
    flag_schemas: String,
    flag_output: String,
}

fn main() {
    let args: CmdArgs = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());
    if args.cmd_code {
        // TODO use &args.flag_schemas, &args.flag_output
    } else {
        println!("{}", USAGE);
        process::exit(1);
    }
}
