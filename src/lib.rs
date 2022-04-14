//! # rsgen-avro
//!
//! **[Apache Avro](https://avro.apache.org/)** is a data serialization system which
//! provides rich data structures and a compact, fast, binary data format.
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
//! The **[avro-rs](https://github.com/flavray/avro-rs)** crate, which is re-exported,
//! provides a way to read and write Avro data with both Avro-specialized and Rust
//! serde-compatible types.
//!
//! **[rsgen-avro](https://github.com/lerouxrgd/rsgen-avro)** provides a way to generate
//! Rust serde-compatible types based on Avro schemas. Both a command line tool and
//! library crate are available.
//!
//! # Using the library
//!
//! Basic usage is:
//!
//! ```rust
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
//! let source = Source::SchemaStr(&raw_schema);
//!
//! let mut out = std::io::stdout();
//!
//! let g = Generator::new().unwrap();
//! g.gen(&source, &mut out).unwrap();
//! ```
//!
//! This will generate the following output:
//!
//! ```text
//! #[serde(default)]
//! #[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
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
//! pub enum Source<'a> {
//!     Schema(&'a avro_rs::Schema), // from re-exported `avro-rs` crate
//!     SchemaStr(&'a str),          // schema as a json string
//!     GlobPattern(&'a str),        // pattern to schema files
//! }
//! ```
//!
//! Note also that the `Generator` can be customized with a builder:
//!
//! ```rust
//! # use rsgen_avro::Generator;
//! let g = Generator::builder().precision(2).build().unwrap();
//! ```

mod error;
mod gen;
mod templates;

pub use crate::error::{Error, Result};
pub use crate::gen::{Generator, GeneratorBuilder, Source};

pub use apache_avro;
