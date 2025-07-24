# rsgen-avro &emsp; [![latest]][crates.io] [![doc]][docs.rs]

[latest]: https://img.shields.io/crates/v/rsgen-avro.svg
[crates.io]: https://crates.io/crates/rsgen-avro
[doc]: https://docs.rs/rsgen-avro/badge.svg
[docs.rs]: https://docs.rs/rsgen-avro

A command line tool and library for generating [serde][]-compatible Rust types from
[Avro schemas][schemas]. The [apache-avro][] crate, which is re-exported, provides a way to
read and write Avro data with such types.

## Command line usage

Download the latest [release](https://github.com/lerouxrgd/rsgen-avro/releases).

Available options `rsgen-avro --help`:

```text
Generate Rust types from Avro schemas

Usage: rsgen-avro [OPTIONS] <GLOB_PATTERN> <OUTPUT_FILE>

Arguments:
  <GLOB_PATTERN>  Glob pattern to select Avro schema files
  <OUTPUT_FILE>   The file where Rust types will be written, '-' for stdout

Options:
      --fmt                      Run rustfmt on the resulting <output-file>
      --nullable                 Replace null fields with their default value when deserializing
      --precision <P>            Precision for f32/f64 default values that aren't round numbers [default: 3]
      --union-deser              Custom deserialization for apache-avro multi-valued union types
      --chrono-dates             Use chrono::NaiveDateTime for date/timestamps logical types
      --derive-builders          Derive builders for generated record structs
      --impl-schemas <METHOD>    Implement AvroSchema for generated record structs [default: none] [possible values: derive, copy-build-schema, none]
      --extra-derives <DERIVES>  Extract Derives for generated record structs, comma separated, e.g. `std::fmt::Display,std::string::ToString`
  -h, --help                     Print help (see more with '--help')
  -V, --version                  Print version
```

## Library usage

As a library, the basic usage is:

```rust
use rsgen_avro::{Source, Generator};

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

let source = Source::SchemaStr(&raw_schema);
let mut out = std::io::stdout();

let g = Generator::new().unwrap();
g.generate(&source, &mut out).unwrap();
```

This will generate the following output:

```text
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Test {
    #[serde(default = "default_test_a")]
    pub a: i64,
    pub b: String,
}

#[inline(always)]
fn default_test_a() -> i64 { 42 }
```

Various `Schema` sources can be used with `Generator::generate(source, output)` method:

```rust
pub enum Source<'a> {
    Schema(&'a rsgen_avro::Schema),    // Avro schema enum re-exported from `apache-avro`
    Schemas(&'a [rsgen_avro::Schema]), // A slice of Avro schema enums
    SchemaStr(&'a str),                // Schema as a json string
    GlobPattern(&'a str),              // Glob pattern to select schema files
}
```

Note also that the `Generator` can be customized with a builder:

```rust
let generator = rsgen_avro::Generator::builder()
    .precision(2)
    .build()
    .unwrap();
```

See [GeneratorBuilder][gen-builder-doc] documentation for all available options.

[gen-builder-doc]: https://docs.rs/rsgen-avro/latest/rsgen_avro/struct.GeneratorBuilder.html

## Limitations

- Avro schema `namespace` fields are ignored, therefore record names within a schema
  (and across schemas) must not conflict (i.e. must be unique).
- Rust `Option<T>` are supported through Avro unions having `"null"` in their first
  position only (See [#39](https://github.com/lerouxrgd/rsgen-avro/issues/39))

[schemas]: https://avro.apache.org/docs/current/spec.html
[apache-avro]: https://github.com/apache/avro/tree/master/lang/rust
[serde]: https://serde.rs
