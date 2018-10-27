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
//! **[rsgen-avro]** provides a way to generate Rust serde-compatible types based on Avro schemas. Both a binary tool and library crate are available.
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
//! Add at the top of the crate:
//!
//! ```rust
//! extern crate rsgen_avro;
//! ```
//!
//! Then, the basic usage is:
//!
//! ```rust
//! let raw_schema = r#"
//! {
//!     "type": "record",
//!     "name": "test",
//!     "fields": [
//!         {"name": "a", "type": "long", "default": 42},
//!         {"name": "b", "type": "string"}
//!     ]
//! }
//! "#;
//!
//! let schema = Schema::parse_str(&raw_schema).unwrap();
//! let source = Source::Schema(&schema);
//!
//! let mut out: Box<Write> = Box::new(stdout());
//!
//! let g = Generator::new().unwrap();
//! g.gen(&source, &mut out).unwrap();
//! ```
//!
//! This will generate the following output:
//!
//! ```text
//! #[serde(default)]
//! #[derive(Debug, Deserialize, Serialize)]
//! pub struct Test {
//!     pub a: i64,
//!     pub b: String,
//! }
//!
//! impl Default for Test {
//!     fn default() -> Test {
//!         Test {
//!             a: 42,
//!             b: String::default(),
//!         }
//!     }
//! }
//! ```
//!
//! Various `Schema` sources can be used with the `.gen(..)` method:
//!
//! ```rust
//! pub enum Source<'a> {
//!     Schema(&'a Schema),
//!     SchemaStr(&'a str),
//!     FilePath(&'a str),
//!     DirPath(&'a str),
//! }
//! ```
//!
//! Note also that the `Generator` can be customized with a builder:
//!
//! ```rust
//! let g = Generator::builder().precision(2).build().unwrap();
//! ```

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

/// Represents a schema input source.
pub enum Source<'a> {
    /// An Avro schema enum from `avro-rs` crate.
    Schema(&'a Schema),
    /// An Avro schema string in json format.
    SchemaStr(&'a str),
    /// Path to a file containing an Avro schema in json format.
    FilePath(&'a str),
    /// Path to a directory containing multiple files in Avro schema.
    DirPath(&'a str),
}

/// The main component of this library.
/// It is stateless can be reused many times.
pub struct Generator {
    templater: Templater,
    no_extern: bool,
}

impl Generator {
    /// Create a new `Generator` through a builder with default config.
    pub fn new() -> Result<Generator, Error> {
        GeneratorBuilder::new().build()
    }

    /// Returns a fluid builder for custom `Generator` instantiation.
    pub fn builder() -> GeneratorBuilder {
        GeneratorBuilder::new()
    }

    /// Generates Rust code from an Avro schema `Source`.
    /// Writes all generated types to the ouput.
    pub fn gen(&self, source: &Source, output: &mut Box<Write>) -> Result<(), Error> {
        if !self.no_extern {
            output.write_all(HEADER.as_bytes())?;
        }

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

    /// Given an Avro `schema`:
    /// * Find its ordered, nested dependencies with `deps_stack(schema)`
    /// * Pops sub-schemas and generate appropriate Rust types
    /// * Keeps tracks of nested schema->name with `GenState` mapping
    /// * Appends generated Rust types to the output
    fn gen_in_order(&self, schema: &Schema, output: &mut Box<Write>) -> Result<(), Error> {
        let mut gs = GenState::new();
        let mut deps = deps_stack(schema);

        while let Some(s) = deps.pop() {
            match s {
                // Simply generate code
                Schema::Fixed { .. } => {
                    let code = &self.templater.str_fixed(&s)?;
                    output.write_all(code.as_bytes())?
                }
                Schema::Enum { .. } => {
                    let code = &self.templater.str_enum(&s)?;
                    output.write_all(code.as_bytes())?
                }

                // Generate code with potentially nested types
                Schema::Record { .. } => {
                    let code = &self.templater.str_record(&s, &gs)?;
                    output.write_all(code.as_bytes())?
                }

                // Register inner type for it to be used as a nested type later
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

/// Utility function to find the ordered, nested dependencies of an Avro `schema`.
/// Explores nested `schema`s in a breadth-first fashion, pushing them on a stack
/// at the same time in order to have them ordered.
/// It is similar to traversing the `schema` tree in a post-order fashion.
fn deps_stack(schema: &Schema) -> Vec<&Schema> {
    let mut deps = Vec::new();
    let mut q = VecDeque::new();

    q.push_back(schema);
    while !q.is_empty() {
        let s = q.pop_front().unwrap();

        match s {
            // No nested schemas, add them to the result stack
            Schema::Fixed { .. } => deps.push(s),
            Schema::Enum { .. } => deps.push(s),

            // Explore the record fields for potentially nested schemas
            Schema::Record { fields, .. } => {
                deps.push(s);

                for RecordField { schema: sr, .. } in fields {
                    match sr {
                        // No nested schemas, add them to the result stack
                        Schema::Fixed { .. } => deps.push(sr),
                        Schema::Enum { .. } => deps.push(sr),

                        // Push to the exploration queue for further checks
                        Schema::Record { .. } => q.push_back(sr),

                        // Push to the exploration queue, depending on the inner schema format
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

            // Depending on the inner schema type ...
            Schema::Map(sc) | Schema::Array(sc) => match &**sc {
                // ... Needs further checks, push to the exploration queue
                Schema::Fixed { .. }
                | Schema::Enum { .. }
                | Schema::Record { .. }
                | Schema::Map(..)
                | Schema::Array(..)
                | Schema::Union(..) => q.push_back(&**sc),
                // ... Not nested, can be pushed to the result stack
                _ => deps.push(s),
            },
            Schema::Union(union) => {
                if let [Schema::Null, sc] = union.variants() {
                    match sc {
                        // ... Needs further checks, push to the exploration queue
                        Schema::Fixed { .. }
                        | Schema::Enum { .. }
                        | Schema::Record { .. }
                        | Schema::Map(..)
                        | Schema::Array(..)
                        | Schema::Union(..) => q.push_back(sc),
                        // ... Not nested, can be pushed to the result stack
                        _ => deps.push(s),
                    }
                }
            }

            // Ignore all other schema formats
            _ => (),
        }
    }

    deps
}

/// A builder class to customize `Generator`.
pub struct GeneratorBuilder {
    precision: usize,
    no_extern: bool,
}

impl GeneratorBuilder {
    /// Creates a new `GeneratorBuilder`.
    pub fn new() -> GeneratorBuilder {
        GeneratorBuilder {
            precision: 3,
            no_extern: false,
        }
    }

    /// Sets the precision for default values of f32/f64 fields.
    pub fn precision(mut self, precision: usize) -> GeneratorBuilder {
        self.precision = precision;
        self
    }

    /// Disables `extern crate ...` from the first generated lines.
    pub fn no_extern(mut self, no_extern: bool) -> GeneratorBuilder {
        self.no_extern = no_extern;
        self
    }

    /// Create a `Generator` with the builder parameters.
    pub fn build(self) -> Result<Generator, Error> {
        let mut templater = Templater::new()?;
        templater.precision = self.precision;
        Ok(Generator {
            templater,
            no_extern: self.no_extern,
        })
    }
}

#[cfg(test)]
mod tests {
    extern crate gag;
    use super::*;
    use std::io::stdout;

    #[test]
    fn simple() {
        let raw_schema = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "a", "type": "long", "default": 42},
                {"name": "b", "type": "string"}
            ]
        }
        "#;

        let schema = Schema::parse_str(&raw_schema).unwrap();
        let source = Source::Schema(&schema);

        use self::gag::BufferRedirect;
        let mut buf = BufferRedirect::stdout().unwrap();
        let mut out: Box<Write> = Box::new(stdout());

        let g = Generator::new().unwrap();
        g.gen(&source, &mut out).unwrap();

        let mut res = String::new();
        buf.read_to_string(&mut res).unwrap();
        eprintln!("======>>> {}", res);
    }

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
