# rsgen-avro

[![Crates.io](https://img.shields.io/crates/v/rsgen-avro.svg)](https://crates.io/crates/rsgen-avro)
[![Docs](https://docs.rs/rsgen-avro/badge.svg)](https://docs.rs/rsgen-avro)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/lerouxrgd/rsgen-avro/blob/master/LICENSE)

A command line tool and library for generating [serde][]-compatible Rust types from [Avro schemas][schemas]. The [avro-rs][] crate provides a way to read and write Avro data with such types.

## Command line usage

Install with:

```sh
cargo install rsgen-avro
```

Available options:

```
Usage:
  rsgen-avro [options] <schema-file-or-dir> <output-file>
  rsgen-avro (-h | --help)
  rsgen-avro (-V | --version)

Options:
  --fmt          Run rustfmt on the resulting <output-file>
  --nullable     Replace null fields with their default value
                 when deserializing.
  --precision=P  Precision for f32/f64 default values
                 that aren't round numbers [default: 3].
  -V, --version  Show version.
  -h, --help     Show this screen.
```

## Library usage

As a libray, the basic usage is:

```rust
use avro_rs::Schema;
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

let schema = Schema::parse_str(&raw_schema).unwrap();
let source = Source::Schema(&schema);

let mut out = std::io::stdout();

let g = Generator::new().unwrap();
g.gen(&source, &mut out).unwrap();
```

This will generate the following output:

```text
use serde::{Deserialize, Serialize};

#[serde(default)]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
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
    Schema(&'a Schema),
    SchemaStr(&'a str),
    FilePath(&'a Path),
    DirPath(&'a Path),
}
```

Note also that the `Generator` can be customized with a builder:

```rust
let g = Generator::builder().precision(2).build().unwrap();
```

## Limitations

* Avro schema `namespace` fields are ignored, therefore names from a single schema must no clash.
* Only `union` of the form `["null", "some-type"]` are supported and treated as `Option<_>`.

[schemas]: https://avro.apache.org/docs/current/spec.html
[avro-rs]: https://github.com/flavray/avro-rs
[serde]: https://serde.rs
