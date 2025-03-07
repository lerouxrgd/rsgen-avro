#![doc = include_str!("../README.md")]

mod error;
mod generator;
mod templates;

pub use crate::error::{Error, Result};
pub use crate::generator::{Generator, GeneratorBuilder, Source};

pub use apache_avro;
pub use apache_avro::Schema;
