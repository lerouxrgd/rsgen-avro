#![doc = include_str!("../README.md")]

mod error;
mod gen;
mod templates;

pub use crate::error::{Error, Result};
pub use crate::gen::{Generator, GeneratorBuilder, Source};

pub use apache_avro;
pub use apache_avro::Schema;
