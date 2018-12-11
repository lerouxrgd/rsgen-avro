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
//! The **[avro-rs](https://github.com/flavray/avro-rs)** crate provides a way to
//! read and write Avro data with both Avro-specialized and Rust serde-compatible types.
//!
//! **[rsgen-avro](https://github.com/lerouxrgd/rsgen-avro)** provides a way to generate Rust serde-compatible types based on Avro schemas. Both a command line tool and library crate are available.
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
//! # extern crate avro_rs;
//! # extern crate rsgen_avro;
//! use std::io::{stdout, Write};
//! use avro_rs::Schema;
//! use rsgen_avro::{Source, Generator};
//!
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
//! use serde::{Deserialize, Serialize};
//!
//! #[serde(default)]
//! #[derive(Debug, PartialEq, PartialOrd, Clone, Deserialize, Serialize)]
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
//! Various `Schema` sources can be used with `Generator`'s `.gen(..)` method:
//!
//! ```rust
//! # extern crate avro_rs;
//! # use avro_rs::Schema;
//! # use std::path::Path;
//! pub enum Source<'a> {
//!     Schema(&'a Schema),
//!     SchemaStr(&'a str),
//!     FilePath(&'a Path),
//!     DirPath(&'a Path),
//! }
//! ```
//!
//! Note also that the `Generator` can be customized with a builder:
//!
//! ```rust
//! # use rsgen_avro::Generator;
//! let g = Generator::builder().precision(2).build().unwrap();
//! ```

#[macro_use]
extern crate failure;
#[macro_use]
extern crate lazy_static;

mod templates;

use std::collections::VecDeque;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

use avro_rs::{schema::RecordField, Schema};
use failure::Error;

use crate::templates::*;

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
    FilePath(&'a Path),
    /// Path to a directory containing multiple files in Avro schema.
    DirPath(&'a Path),
}

/// The main component of this library.
/// It is stateless can be reused many times.
pub struct Generator {
    templater: Templater,
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
        output.write_all(self.templater.str_serde()?.as_bytes())?;

        if self.templater.nullable {
            output.write_all(DESER_NULLABLE.as_bytes())?;
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
    nullable: bool,
}

impl GeneratorBuilder {
    /// Creates a new `GeneratorBuilder`.
    pub fn new() -> GeneratorBuilder {
        GeneratorBuilder {
            precision: 3,
            nullable: false,
        }
    }

    /// Sets the precision for default values of f32/f64 fields.
    pub fn precision(mut self, precision: usize) -> GeneratorBuilder {
        self.precision = precision;
        self
    }

    /// Puts default value when deserializing `null` field.
    /// Doesn't apply to union fields ["null", "Foo"], which are `Option<Foo>`.
    pub fn nullable(mut self, nullable: bool) -> GeneratorBuilder {
        self.nullable = nullable;
        self
    }

    /// Create a `Generator` with the builder parameters.
    pub fn build(self) -> Result<Generator, Error> {
        let mut templater = Templater::new()?;
        templater.precision = self.precision;
        templater.nullable = self.nullable;
        Ok(Generator { templater })
    }
}

#[cfg(test)]
mod tests {
    extern crate gag;
    extern crate matches;

    use std::io::stdout;

    use self::gag::BufferRedirect;
    use avro_rs::schema::Name;
    use serde::{Deserialize, Serialize};

    use super::*;

    macro_rules! assert_schema_gen (
        ($generator:expr, $expected:expr, $raw_schema:expr) => (
            let schema = Schema::parse_str($raw_schema).unwrap();
            let source = Source::Schema(&schema);

            let mut buf = BufferRedirect::stdout().unwrap();
            let mut out: Box<Write> = Box::new(stdout());

            $generator.gen(&source, &mut out).unwrap();

            let mut res = String::new();
            buf.read_to_string(&mut res).unwrap();
            buf.into_inner();

            assert_eq!($expected, &res);
        );
    );

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

        let expected = "use serde::{Deserialize, Serialize};

#[serde(default)]
#[derive(Debug, PartialEq, PartialOrd, Clone, Deserialize, Serialize)]
pub struct Test {
    pub a: i64,
    pub b: String,
}

impl Default for Test {
    fn default() -> Test {
        Test {
            a: 42,
            b: String::default(),
        }
    }
}
";

        let g = Generator::new().unwrap();
        assert_schema_gen!(g, expected, raw_schema);
    }

    #[test]
    fn complex() {
        let raw_schema = r#"
{
  "type": "record",
  "name": "User",
  "doc": "Hi there.",
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

        let expected = r#"use serde::{Deserialize, Serialize};

/// Hi there.
#[serde(default)]
#[derive(Debug, PartialEq, PartialOrd, Clone, Deserialize, Serialize)]
pub struct User {
    #[serde(rename = "aa-i32")]
    pub aa_i32: Vec<Vec<i32>>,
    pub favorite_number: i32,
    pub likes_pizza: bool,
    pub name: String,
    pub oye: f32,
}

impl Default for User {
    fn default() -> User {
        User {
            aa_i32: vec![vec![0], vec![12, -1]],
            favorite_number: 7,
            likes_pizza: false,
            name: "".to_owned(),
            oye: 1.100,
        }
    }
}
"#;

        let g = Generator::new().unwrap();
        assert_schema_gen!(g, expected, raw_schema);
    }

    #[test]
    fn nullable_gen() {
        let raw_schema = r#"
{
  "type": "record",
  "name": "test",
  "fields": [
    {"name": "a", "type": "long", "default": 42},
    {"name": "b-b", "type": "string", "default": "na"},
    {"name": "c", "type": ["null", "int"], "default": null}
  ]
}
"#;

        let expected = r#"use serde::{Deserialize, Deserializer, Serialize};

macro_rules! deser(
    ($name:ident, $rtype:ty, $val:expr) => (
        fn $name<'de, D>(deserializer: D) -> Result<$rtype, D::Error>
        where
            D: Deserializer<'de>,
        {
            let opt = Option::deserialize(deserializer)?;
            Ok(opt.unwrap_or_else(|| $val))
        }
    );
);

#[serde(default)]
#[derive(Debug, PartialEq, PartialOrd, Clone, Deserialize, Serialize)]
pub struct Test {
    #[serde(deserialize_with = "nullable_test_a")]
    pub a: i64,
    #[serde(rename = "b-b", deserialize_with = "nullable_test_b_b")]
    pub b_b: String,
    pub c: Option<i32>,
}
deser!(nullable_test_a, i64, 42);
deser!(nullable_test_b_b, String, "na".to_owned());

impl Default for Test {
    fn default() -> Test {
        Test {
            a: 42,
            b_b: "na".to_owned(),
            c: None,
        }
    }
}
"#;
        let g = Generator::builder().nullable(true).build().unwrap();
        assert_schema_gen!(g, expected, raw_schema);
    }

    #[test]
    fn nullable_code() {
        use serde::{Deserialize, Deserializer};

        macro_rules! deser(
            ($name:ident, $rtype:ty, $val:expr) => (
                fn $name<'de, D>(deserializer: D) -> Result<$rtype, D::Error>
                where
                    D: Deserializer<'de>,
                {
                    let opt = Option::deserialize(deserializer)?;
                    Ok(opt.unwrap_or_else(|| $val))
                }
            );
        );

        #[serde(default)]
        #[derive(Debug, PartialEq, Deserialize, Serialize)]
        pub struct Test {
            #[serde(deserialize_with = "nullable_test_a")]
            pub a: i64,
            #[serde(rename = "b-b", deserialize_with = "nullable_test_b_b")]
            pub b_b: String,
            pub c: Option<i32>,
        }
        deser!(nullable_test_a, i64, 42);
        deser!(nullable_test_b_b, String, "na".to_owned());

        impl Default for Test {
            fn default() -> Test {
                Test {
                    a: 42,
                    b_b: "na".to_owned(),
                    c: None,
                }
            }
        }

        let json = r#"{"a": null, "b-b": null, "c": null}"#;
        let res: Test = serde_json::from_str(json).unwrap();
        assert_eq!(Test::default(), res);
    }

    #[test]
    fn deps() {
        use self::matches::assert_matches;

        let raw_schema = r#"
{
  "type": "record",
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
    }
  ]
}
"#;

        let schema = Schema::parse_str(&raw_schema).unwrap();
        let mut deps = deps_stack(&schema);

        let s = deps.pop().unwrap();
        assert_matches!(s, Schema::Enum{ name: Name { ref name, ..}, ..} if name == "Country");

        let s = deps.pop().unwrap();
        assert_matches!(s, Schema::Record{ name: Name { ref name, ..}, ..} if name == "Address");

        let s = deps.pop().unwrap();
        assert_matches!(s, Schema::Record{ name: Name { ref name, ..}, ..} if name == "User");

        let s = deps.pop();
        assert_matches!(s, None);
    }
}
