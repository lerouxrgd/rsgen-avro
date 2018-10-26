//! # rsgen-avro
//!
//! **[Apache Avro](https://avro.apache.org/)** is a data serialization system which provides
//! rich data structures and a compact, fast, binary data format.
//!
//! All data in Avro is schematized, as in the following example:
//!
//! ```text
//! {
//!     "type": "record",
//!     "name": "test",
//!     "fields": [
//!         {"name": "a", "type": "long", "default": 42},
//!         {"name": "b", "type": "string"}
//!     ]
//! }
//! ```
//!
//! **The [avro-rs](https://github.com/flavray/avro-rs)** crate provides a way to
//! read and write Avro data with both Avro-specialized and Rust serde-compatible types.
//!
//! **[rsgen-avro]** provides a way to generate Rust serde-compatible types based on Avro schemas.
//!
//! # Installing the client
//!
//! # Using the library
//!
//! Add to your Cargo.toml:
//!
//! ```text
//! [dependencies]
//! rsgen-avro = "x.y.z"
//! ```
//!
//! To use the library,  just add at the top of the crate:
//!
//! ```
//! extern crate rsgen_avro;
//! ```
//!

extern crate avro_rs;
extern crate by_address;
extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate heck;
#[macro_use]
extern crate lazy_static;
extern crate serde_json;
extern crate tera;

mod templates;

use std::collections::VecDeque;
use std::fs::File;
use std::io::prelude::*;

use avro_rs::{schema::RecordField, Schema};
use failure::Error;

use templates::*;

/// Describes errors happened while generating Rust code.
#[derive(Fail, Debug)]
#[fail(display = "Rsgen failure: {}", _0)]
pub struct RsgenError(String);

impl RsgenError {
    pub fn new<S>(msg: S) -> RsgenError
    where
        S: Into<String>,
    {
        RsgenError(msg.into())
    }
}

pub enum Source<'a> {
    Schema(&'a Schema),
    SchemaStr(&'a str),
    FilePath(&'a str),
    DirPath(&'a str),
}

pub struct Generator {
    templater: Templater,
}

impl Generator {
    pub fn new() -> Result<Generator, Error> {
        GeneratorBuilder::new().build()
    }

    pub fn builder() -> GeneratorBuilder {
        GeneratorBuilder::new()
    }

    pub fn gen(&self, source: &Source, output: &mut Box<Write>) -> Result<(), Error> {
        match source {
            Source::Schema(schema) => self.gen_in_order(schema, output)?,

            Source::SchemaStr(raw_schema) => {
                let schema = Schema::parse_str(&raw_schema)?;
                self.gen_in_order(&schema, output)?
            }

            Source::FilePath(schema_file) => {
                let mut raw_schema = String::new();
                File::open(&schema_file)?.read_to_string(&mut raw_schema)?;
                let schema = Schema::parse_str(&raw_schema)?;
                self.gen_in_order(&schema, output)?
            }

            Source::DirPath(schemas_dir) => {
                for entry in std::fs::read_dir(schemas_dir)? {
                    let path = entry?.path();
                    if !path.is_dir() {
                        let mut raw_schema = String::new();
                        File::open(&path)?.read_to_string(&mut raw_schema)?;
                        let schema = Schema::parse_str(&raw_schema)?;
                        self.gen_in_order(&schema, output)?
                    }
                }
            }
        }

        Ok(())
    }

    fn gen_in_order(&self, schema: &Schema, output: &mut Box<Write>) -> Result<(), Error> {
        let mut gs = GenState::new();
        let mut deps = deps_stack(schema);

        while let Some(s) = deps.pop() {
            match s {
                Schema::Fixed { .. } => {
                    let code = &self.templater.str_fixed(&s)?;
                    output.write_all(code.as_bytes())?
                }
                Schema::Enum { .. } => {
                    let code = &self.templater.str_enum(&s)?;
                    output.write_all(code.as_bytes())?
                }
                Schema::Record { .. } => {
                    let code = &self.templater.str_record(&s, &gs)?;
                    output.write_all(code.as_bytes())?
                }
                Schema::Array(inner) => {
                    let type_str = array_type(inner, &gs)?;
                    gs.put_type(&s, type_str)
                }
                Schema::Map(inner) => {
                    let type_str = map_type(inner, &gs)?;
                    gs.put_type(&s, type_str)
                }
                Schema::Union(union) => {
                    if let [Schema::Null, inner] = union.variants() {
                        let type_str = option_type(inner, &gs)?;
                        gs.put_type(&s, type_str)
                    }
                }
                _ => Err(RsgenError::new(format!("Not a valid root schema: {:?}", s)))?,
            }
        }
        Ok(())
    }
}

fn deps_stack(schema: &Schema) -> Vec<&Schema> {
    let mut deps = Vec::new();
    let mut q = VecDeque::new();

    q.push_back(schema);
    while !q.is_empty() {
        let s = q.pop_front().unwrap();

        match s {
            Schema::Fixed { .. } => deps.push(s),
            Schema::Enum { .. } => deps.push(s),
            Schema::Record { fields, .. } => {
                deps.push(s);

                for RecordField { schema: sr, .. } in fields {
                    match sr {
                        Schema::Fixed { .. } => deps.push(sr),
                        Schema::Enum { .. } => deps.push(sr),
                        Schema::Record { .. } => q.push_back(sr),

                        Schema::Map(sc) | Schema::Array(sc) => match &**sc {
                            Schema::Fixed { .. }
                            | Schema::Enum { .. }
                            | Schema::Record { .. }
                            | Schema::Map(..)
                            | Schema::Array(..)
                            | Schema::Union(..) => q.push_back(&**sc),
                            _ => (),
                        },

                        Schema::Union(union) => {
                            if let [Schema::Null, sc] = union.variants() {
                                match sc {
                                    Schema::Fixed { .. }
                                    | Schema::Enum { .. }
                                    | Schema::Record { .. }
                                    | Schema::Map(..)
                                    | Schema::Array(..)
                                    | Schema::Union(..) => q.push_back(sc),
                                    _ => (),
                                }
                            }
                        }
                        _ => (),
                    };
                }
            }

            Schema::Map(sc) | Schema::Array(sc) => match &**sc {
                Schema::Fixed { .. }
                | Schema::Enum { .. }
                | Schema::Record { .. }
                | Schema::Map(..)
                | Schema::Array(..)
                | Schema::Union(..) => q.push_back(&**sc),
                _ => deps.push(s),
            },

            Schema::Union(union) => {
                if let [Schema::Null, sc] = union.variants() {
                    match sc {
                        Schema::Fixed { .. }
                        | Schema::Enum { .. }
                        | Schema::Record { .. }
                        | Schema::Map(..)
                        | Schema::Array(..)
                        | Schema::Union(..) => q.push_back(sc),
                        _ => deps.push(s),
                    }
                }
            }
            _ => (),
        }
    }

    deps
}

pub struct GeneratorBuilder {
    precision: Option<usize>,
}

impl GeneratorBuilder {
    pub fn new() -> GeneratorBuilder {
        GeneratorBuilder { precision: None }
    }

    pub fn precision(mut self, precision: usize) -> GeneratorBuilder {
        self.precision = Some(precision);
        self
    }

    pub fn build(self) -> Result<Generator, Error> {
        let precision = self.precision.unwrap_or(3);
        let mut templater = Templater::new()?;
        templater.precision = precision;
        Ok(Generator { templater })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::stdout;

    #[test]
    fn record() {
        let raw_schema = r#"
{"namespace": "example.avro",
 "type": "record",
 "name": "User",
 "fields": [
   {"name": "name", "type": "string", "default": ""},
   {"name": "favorite_number",  "type": "int", "default": 7},
   {"name": "likes_pizza", "type": "boolean", "default": false},
   {"name": "oye", "type": "float", "default": 1.1},
   {"name": "aa-i32",
    "type": {"type": "array", "items": {"type": "array", "items": "int"}},
    "default": [[0], [12, -1]]}
 ]
}
"#;

        let mut out: Box<Write> = Box::new(stdout());

        let schema = Schema::parse_str(&raw_schema).unwrap();
        let source = Source::Schema(&schema);

        let g = Generator::builder().precision(2).build().unwrap();
        g.gen(&source, &mut out).unwrap();
    }

    #[test]
    fn deps() {
        let raw_schema = r#"
{"type": "record",
 "name": "User",
 "fields": [
   {"name": "name", "type": "string", "default": "unknown"},
   {"name": "address",
    "type": {
      "type": "record",
      "name": "Address",
      "fields": [
        {"name": "city", "type": "string", "default": "unknown"},
        {"name": "country",
         "type": {"type": "enum", "name": "Country", "symbols": ["FR", "JP"]}
        }
      ]
    }
   },
   {"name": "likes_pizza", "type": "boolean", "default": false}
 ]
}
"#;

        let schema = Schema::parse_str(&raw_schema).unwrap();
        let deps = deps_stack(&schema);

        let depss = deps
            .iter()
            .map(|s| format!("{:?}", s))
            .collect::<Vec<String>>()
            .as_slice()
            .join("\n\n");
        println!("{}", depss);
    }
}
