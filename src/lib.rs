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
//! The **[apache-avro][]** crate, which is re-exported, provides a way to read and
//! write Avro data with both Avro-specialized and Rust serde-compatible types.
//!
//! **[rsgen-avro][]** provides a way to generate Rust serde-compatible types based on
//! Avro schemas. Both a command line tool and library crate are available.
//!
//! [apache-avro]: https://github.com/apache/avro/tree/master/lang/rust
//! [rsgen-avro]: https://github.com/lerouxrgd/rsgen-avro
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
//! #[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
//! pub struct Test {
//!     #[serde(default = "default_test_a")]
//!     pub a: i64,
//!     pub b: String,
//! }
//!
//! #[inline(always)]
//! fn default_test_a() -> i64 { 42 }
//! ```
//!
//! Various `Schema` sources can be used with `Generator`'s `.gen(..)` method:
//!
//! ```rust
//! pub enum Source<'a> {
//!     Schema(&'a rsgen_avro::Schema),    // Avro schema enum re-exported from `apache-avro`
//!     Schemas(&'a [rsgen_avro::Schema]), // A slice of Avro schema enums
//!     SchemaStr(&'a str),                // Schema as a json string
//!     GlobPattern(&'a str),              // Glob pattern to select schema files
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
use apache_avro::schema::UnionSchema;
pub use apache_avro::Schema;

// A temporary helper which is used as a workaround for
// https://issues.apache.org/jira/browse/AVRO-3625
// This method should be replaced with `union.is_nullable()`
// once apache-avro is updated to a version which includes the fix.
pub(crate) fn is_union_schema_nullable(union: &UnionSchema) -> bool {
    union.variants().iter().any(|s| s == &Schema::Null)
}
