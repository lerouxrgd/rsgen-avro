# rsgen-avro &emsp; [![latest]][crates.io] [![doc]][docs.rs]

[latest]: https://img.shields.io/crates/v/rsgen-avro.svg
[crates.io]: https://crates.io/crates/rsgen-avro
[doc]: https://docs.rs/rsgen-avro/badge.svg
[docs.rs]: https://docs.rs/rsgen-avro

A command line tool and library for generating [serde][]-compatible Rust types from
[Avro schemas][schemas]. The [avro-rs][] crate, which is re-exported, provides a way to
read and write Avro data with such types.

## Command line usage

Install with:

```sh
cargo install rsgen-avro
```

Available options:

```
Usage:
  rsgen-avro [options] <schema-file-pattern> <output-file>
  rsgen-avro (-h | --help)
  rsgen-avro (-V | --version)

Options:
  --fmt             Run rustfmt on the resulting <output-file>
  --nullable        Replace null fields with their default value when deserializing.
  --precision=P     Precision for f32/f64 default values that aren't round numbers [default: 3].
  --variant-access  Derive the traits in the variant_access_traits crate on union types.
  --union-deser     Custom deserialization for avro-rs multi-valued union types.
  -V, --version     Show version.
  -h, --help        Show this screen.
```

## Library usage

As a libray, the basic usage is:

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
g.gen(&source, &mut out).unwrap();
```

This will generate the following output:

```text
#[serde(default)]
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
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
```

Various `Schema` sources can be used with `Generator`'s `.gen(..)` method:

```rust
pub enum Source<'a> {
    Schema(&'a avro_rs::Schema), // from re-exported `avro-rs` crate
    SchemaStr(&'a str),          // schema as a json string
    GlobPattern(&'a str),        // pattern to schema files
}
```

Note also that the `Generator` can be customized with a builder:

```rust
let g = Generator::builder().precision(2).build().unwrap();
```

## Limitations

* Avro schema `namespace` fields are ignored, therefore names from a single schema must
  not conflict.

[schemas]: https://avro.apache.org/docs/current/spec.html
[avro-rs]: https://github.com/flavray/avro-rs
[serde]: https://serde.rs
