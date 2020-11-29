use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::prelude::*;
use std::path::Path;

use avro_rs::{schema::RecordField, Schema};

use crate::error::{Error, Result};
use crate::templates::*;

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
/// It is stateless and can be reused many times.
pub struct Generator {
    templater: Templater,
}

impl Generator {
    /// Create a new `Generator` through a builder with default config.
    pub fn new() -> Result<Generator> {
        GeneratorBuilder::new().build()
    }

    /// Returns a fluid builder for custom `Generator` instantiation.
    pub fn builder() -> GeneratorBuilder {
        GeneratorBuilder::new()
    }

    /// Generates Rust code from an Avro schema `Source`.
    /// Writes all generated types to the ouput.
    pub fn gen(&self, source: &Source, output: &mut impl Write) -> Result<()> {
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
                let raw_schema = fs::read_to_string(schema_file)?;
                let schema = Schema::parse_str(&raw_schema)?;
                self.gen_in_order(&schema, output)?
            }

            Source::DirPath(schemas_dir) => {
                for entry in std::fs::read_dir(schemas_dir)? {
                    let path = entry?.path();
                    if !path.is_dir() {
                        let raw_schema = fs::read_to_string(path)?;
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
    fn gen_in_order(&self, schema: &Schema, output: &mut impl Write) -> Result<()> {
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

                _ => Err(Error::Schema(format!("Not a valid root schema: {:?}", s)))?,
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
    fn push_unique<'a>(deps: &mut Vec<&'a Schema>, s: &'a Schema) {
        if !deps.contains(&s) {
            deps.push(s)
        };
    }

    let mut deps = Vec::new();
    let mut q = VecDeque::new();

    q.push_back(schema);
    while !q.is_empty() {
        let s = q.pop_front().unwrap();

        match s {
            // No nested schemas, add them to the result stack
            Schema::Fixed { .. } => push_unique(&mut deps, s),
            Schema::Enum { .. } => push_unique(&mut deps, s),
            Schema::Decimal { inner, .. } if matches!(**inner, Schema::Fixed{ .. }) => {
                push_unique(&mut deps, s)
            } // TODO: needed ?

            // Explore the record fields for potentially nested schemas
            Schema::Record { fields, .. } => {
                push_unique(&mut deps, s);

                let by_pos = fields
                    .iter()
                    .map(|f| (f.position, f))
                    .collect::<HashMap<_, _>>();
                let mut i = 0;
                while let Some(RecordField { schema: sr, .. }) = by_pos.get(&i) {
                    match sr {
                        // No nested schemas, add them to the result stack
                        Schema::Fixed { .. } => push_unique(&mut deps, sr),
                        Schema::Enum { .. } => push_unique(&mut deps, sr),

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
                                    | Schema::Union(..) => {
                                        q.push_back(sc);
                                        push_unique(&mut deps, sc);
                                    }
                                    _ => (),
                                }
                            }
                        }
                        _ => (),
                    };
                    i += 1;
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
                _ => push_unique(&mut deps, s),
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
                        _ => push_unique(&mut deps, s),
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
    pub fn build(self) -> Result<Generator> {
        let mut templater = Templater::new()?;
        templater.precision = self.precision;
        templater.nullable = self.nullable;
        Ok(Generator { templater })
    }
}

#[cfg(test)]
mod tests {
    use avro_rs::schema::Name;

    use super::*;

    macro_rules! assert_schema_gen (
        ($generator:expr, $expected:expr, $raw_schema:expr) => (
            let schema = Schema::parse_str($raw_schema).unwrap();
            let source = Source::Schema(&schema);

            let mut buf = vec![];
            $generator.gen(&source, &mut buf).unwrap();
            let res = String::from_utf8(buf).unwrap();

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

        let expected = "
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

        let expected = r#"
/// Hi there.
#[serde(default)]
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct User {
    pub name: String,
    pub favorite_number: i32,
    pub likes_pizza: bool,
    pub oye: f32,
    #[serde(rename = "aa-i32")]
    pub aa_i32: Vec<Vec<i32>>,
}

impl Default for User {
    fn default() -> User {
        User {
            name: "".to_owned(),
            favorite_number: 7,
            likes_pizza: false,
            oye: 1.100,
            aa_i32: vec![vec![0], vec![12, -1]],
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

        let expected = r#"
macro_rules! deser(
    ($name:ident, $rtype:ty, $val:expr) => (
        fn $name<'de, D>(deserializer: D) -> Result<$rtype, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let opt = Option::deserialize(deserializer)?;
            Ok(opt.unwrap_or_else(|| $val))
        }
    );
);

#[serde(default)]
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
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
    fn nullable_array() {
        let raw_schema = r#"
{
  "name": "Snmp",
  "type": "record",
  "fields": [ {
    "name": "v1",
    "type": [ "null", {
      "name": "V1",
      "type": "record",
      "fields": [ {
        "name": "pdu",
        "type": [ "null", {
          "name": "TrapV1",
          "type": "record",
          "fields": [ {
            "name": "var",
            "type": ["null", {
              "type": "array",
              "items": {
                "name": "Variable",
                "type": "record",
                "fields": [ {
                  "name": "oid",
                  "type": ["null", {
                    "type":"array",
                    "items": "long"
                  } ],
                  "default": null
                }, {
                  "name": "val",
                  "type": ["null", "string"],
                  "default": null
                }],
                "default": {}
              }
            } ],
            "default": null
          } ]
        } ],
        "default": null
      } ]
    } ],
    "default": null
  } ],
  "default": {}
}
"#;

        let expected = r#"
#[serde(default)]
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Variable {
    pub oid: Option<Vec<i64>>,
    pub val: Option<String>,
}

impl Default for Variable {
    fn default() -> Variable {
        Variable {
            oid: None,
            val: None,
        }
    }
}

#[serde(default)]
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct TrapV1 {
    pub var: Option<Vec<Variable>>,
}

impl Default for TrapV1 {
    fn default() -> TrapV1 {
        TrapV1 {
            var: None,
        }
    }
}

#[serde(default)]
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct V1 {
    pub pdu: Option<TrapV1>,
}

impl Default for V1 {
    fn default() -> V1 {
        V1 {
            pdu: None,
        }
    }
}

#[serde(default)]
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Snmp {
    pub v1: Option<V1>,
}

impl Default for Snmp {
    fn default() -> Snmp {
        Snmp {
            v1: None,
        }
    }
}
"#;

        let g = Generator::new().unwrap();
        assert_schema_gen!(g, expected, raw_schema);
    }

    #[test]
    fn nullable_code() {
        use serde::{Deserialize, Deserializer};

        macro_rules! deser(
            ($name:ident, $rtype:ty, $val:expr) => (
                fn $name<'de, D>(deserializer: D) -> std::result::Result<$rtype, D::Error>
                where
                    D: Deserializer<'de>,
                {
                    let opt = Option::deserialize(deserializer)?;
                    Ok(opt.unwrap_or_else(|| $val))
                }
            );
        );

        #[serde(default)]
        #[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
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
        assert!(matches!(s, Schema::Enum{ name: Name { ref name, ..}, ..} if name == "Country"));

        let s = deps.pop().unwrap();
        assert!(matches!(s, Schema::Record{ name: Name { ref name, ..}, ..} if name == "Address"));

        let s = deps.pop().unwrap();
        assert!(matches!(s, Schema::Record{ name: Name { ref name, ..}, ..} if name == "User"));

        let s = deps.pop();
        assert!(matches!(s, None));
    }
}
