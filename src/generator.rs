use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::prelude::*;

use apache_avro::schema::{ArraySchema, DecimalSchema, MapSchema, RecordField, RecordSchema};

use crate::Schema;
use crate::error::{Error, Result};
use crate::templates::*;

/// An input source for generating Rust types.
pub enum Source<'a> {
    /// An Avro schema enum from the `apache-avro` crate.
    Schema(&'a Schema),
    /// A slice of Avro schema enums from the `apache-avro` crate.
    Schemas(&'a [Schema]),
    /// An Avro schema string in json format.
    SchemaStr(&'a str),
    /// Pattern for selecting files containing Avro schemas in json format.
    GlobPattern(&'a str),
}

/// The main component for generating Rust types from a [`Source`](Source).
///
/// It is stateless and can be reused many times.
#[derive(Debug)]
pub struct Generator {
    templater: Templater,
}

impl Generator {
    /// Creates a new [`Generator`](Generator) with default configuration.
    pub fn new() -> Result<Generator> {
        GeneratorBuilder::new().build()
    }

    /// Returns a fluid builder for custom [`Generator`](Generator) instantiation.
    pub fn builder() -> GeneratorBuilder {
        GeneratorBuilder::new()
    }

    /// Generates Rust code from an Avro schema [`Source`](Source).
    /// Writes all generated types to the output.
    pub fn generate(&self, source: &Source, output: &mut impl Write) -> Result<()> {
        match source {
            Source::Schema(schema) => {
                let mut deps = deps_stack(schema, vec![]);
                self.gen_in_order(&mut deps, output)?;
            }

            Source::Schemas(schemas) => {
                let mut deps = schemas
                    .iter()
                    .fold(vec![], |deps, schema| deps_stack(schema, deps));

                self.gen_in_order(&mut deps, output)?;
            }

            Source::SchemaStr(raw_schema) => {
                let schema = Schema::parse_str(raw_schema)?;
                let mut deps = deps_stack(&schema, vec![]);
                self.gen_in_order(&mut deps, output)?;
            }

            Source::GlobPattern(pattern) => {
                let mut raw_schemas = vec![];
                for entry in glob::glob(pattern)? {
                    let path = entry.map_err(|e| e.into_error())?;
                    if !path.is_dir() {
                        raw_schemas.push(fs::read_to_string(path)?);
                    }
                }

                let schemas = &raw_schemas.iter().map(|s| s.as_str()).collect::<Vec<_>>();
                let schemas = Schema::parse_list(schemas)?;
                self.generate(&Source::Schemas(&schemas), output)?;
            }
        }

        Ok(())
    }

    /// Given an Avro `schema`:
    /// * Find its ordered, nested dependencies with `deps_stack(schema)`
    /// * Pops sub-schemas and generate appropriate Rust types
    /// * Keeps tracks of nested schema->name with `GenState` mapping
    /// * Appends generated Rust types to the output
    fn gen_in_order(&self, deps: &mut Vec<Schema>, output: &mut impl Write) -> Result<()> {
        let mut gs = GenState::new(deps)?.with_chrono_dates(self.templater.use_chrono_dates);

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
                Schema::Array(ArraySchema {
                    items: ref inner, ..
                }) => {
                    let type_str = array_type(inner, &gs)?;
                    gs.put_type(&s, type_str)
                }
                Schema::Map(MapSchema {
                    types: ref inner, ..
                }) => {
                    let type_str = map_type(inner, &gs)?;
                    gs.put_type(&s, type_str)
                }

                Schema::Union(ref union) => {
                    // Generate custom enum with potentially nested types
                    if (union.is_nullable() && union.variants().len() > 2)
                        || (!union.is_nullable() && !union.variants().is_empty())
                    {
                        let code = &self.templater.str_union_enum(&s, &gs)?;
                        output.write_all(code.as_bytes())?
                    }

                    // Register inner union for it to be used as a nested type later
                    let type_str = union_type(union, &gs, true)?;
                    gs.put_type(&s, type_str)
                }

                _ => return Err(Error::Schema(format!("Not a valid root schema: {s:?}"))),
            }
        }

        Ok(())
    }
}

/// Utility function to find the ordered, nested dependencies of an Avro `schema`.
/// Explores nested `schema`s in a breadth-first fashion, pushing them on a stack at the
/// same time in order to have them ordered.  It is similar to traversing the `schema`
/// tree in a post-order fashion.
fn deps_stack(schema: &Schema, mut deps: Vec<Schema>) -> Vec<Schema> {
    fn push_unique(deps: &mut Vec<Schema>, s: Schema) {
        if let Some(i) = deps.iter().position(|d| d == &s) {
            deps.remove(i);
        }
        deps.push(s);
    }

    let mut q = VecDeque::new();

    q.push_back(schema);
    while !q.is_empty() {
        let s = q.pop_front().unwrap();

        match s {
            // No nested schemas, add them to the result stack
            Schema::Enum { .. } => push_unique(&mut deps, s.clone()),
            Schema::Fixed { .. } => push_unique(&mut deps, s.clone()),
            Schema::Decimal(DecimalSchema { inner, .. })
                if matches!(inner.as_ref(), Schema::Fixed { .. }) =>
            {
                push_unique(&mut deps, s.clone())
            }

            // Explore the record fields for potentially nested schemas
            Schema::Record(RecordSchema { fields, .. }) => {
                push_unique(&mut deps, s.clone());

                let by_pos = fields
                    .iter()
                    .map(|f| (f.position, f))
                    .collect::<HashMap<_, _>>();
                let mut i = 0;
                while let Some(RecordField { schema: sr, .. }) = by_pos.get(&i) {
                    match sr {
                        // No nested schemas, add them to the result stack
                        Schema::Fixed { .. } => push_unique(&mut deps, sr.clone()),
                        Schema::Enum { .. } => push_unique(&mut deps, sr.clone()),

                        // Push to the exploration queue for further checks
                        Schema::Record { .. } => q.push_back(sr),

                        // Push to the exploration queue, depending on the inner schema format
                        Schema::Map(MapSchema { types: sc, .. })
                        | Schema::Array(ArraySchema { items: sc, .. }) => match sc.as_ref() {
                            Schema::Fixed { .. }
                            | Schema::Enum { .. }
                            | Schema::Record { .. }
                            | Schema::Map(..)
                            | Schema::Array(..)
                            | Schema::Union(..) => {
                                q.push_back(sc);
                                push_unique(&mut deps, s.clone());
                            }
                            _ => (),
                        },
                        Schema::Union(union) => {
                            if (union.is_nullable() && union.variants().len() > 2)
                                || (!union.is_nullable() && !union.variants().is_empty())
                            {
                                push_unique(&mut deps, sr.clone());
                            }

                            union.variants().iter().for_each(|sc| match sc {
                                Schema::Fixed { .. }
                                | Schema::Enum { .. }
                                | Schema::Record { .. }
                                | Schema::Map(..)
                                | Schema::Array(..)
                                | Schema::Union(..) => {
                                    q.push_back(sc);
                                    push_unique(&mut deps, sc.clone());
                                }

                                _ => (),
                            });
                        }
                        _ => (),
                    };
                    i += 1;
                }
            }

            // Depending on the inner schema type ...
            Schema::Map(MapSchema { types: sc, .. })
            | Schema::Array(ArraySchema { items: sc, .. }) => match sc.as_ref() {
                // ... Needs further checks, push to the exploration queue
                Schema::Fixed { .. }
                | Schema::Enum { .. }
                | Schema::Record { .. }
                | Schema::Map(..)
                | Schema::Array(..)
                | Schema::Union(..) => {
                    q.push_back(sc.as_ref());
                    push_unique(&mut deps, s.clone());
                }
                // ... Not nested, can be pushed to the result stack
                _ => push_unique(&mut deps, s.clone()),
            },

            Schema::Union(union) => {
                if (union.is_nullable() && union.variants().len() > 2)
                    || (!union.is_nullable() && union.variants().len() > 1)
                {
                    push_unique(&mut deps, s.clone());
                }

                union.variants().iter().for_each(|sc| match sc {
                    // ... Needs further checks, push to the exploration queue
                    Schema::Fixed { .. }
                    | Schema::Enum { .. }
                    | Schema::Record { .. }
                    | Schema::Map(..)
                    | Schema::Array(..)
                    | Schema::Union(..) => {
                        q.push_back(sc);
                        push_unique(&mut deps, s.clone());
                    }
                    // ... Not nested, can be pushed to the result stack
                    _ => push_unique(&mut deps, s.clone()),
                });
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
    use_avro_rs_unions: bool,
    use_chrono_dates: bool,
    derive_builders: bool,
    derive_schemas: bool,
    extra_derives: Vec<String>,
}

impl Default for GeneratorBuilder {
    fn default() -> Self {
        Self {
            precision: 3,
            nullable: false,
            use_avro_rs_unions: false,
            use_chrono_dates: false,
            derive_builders: false,
            derive_schemas: false,
            extra_derives: vec![],
        }
    }
}

impl GeneratorBuilder {
    /// Creates a new [`GeneratorBuilder`](GeneratorBuilder).
    pub fn new() -> GeneratorBuilder {
        GeneratorBuilder::default()
    }

    /// Sets the precision for default values of f32/f64 fields.
    pub fn precision(mut self, precision: usize) -> GeneratorBuilder {
        self.precision = precision;
        self
    }

    /// Puts default value when deserializing `null` field.
    ///
    /// Doesn't apply to union fields ["null", "Foo"], which are `Option<Foo>`.
    pub fn nullable(mut self, nullable: bool) -> GeneratorBuilder {
        self.nullable = nullable;
        self
    }

    /// Adds support for deserializing union types from the `apache-avro` crate.
    ///
    /// Only necessary for unions of 3 or more types or 2-type unions without "null".
    /// Note that only int, long, float, double, boolean and bytes values are currently supported.
    pub fn use_avro_rs_unions(mut self, use_avro_rs_unions: bool) -> GeneratorBuilder {
        self.use_avro_rs_unions = use_avro_rs_unions;
        self
    }

    /// Use chrono::NaiveDateTime for date/timestamps logical types
    pub fn use_chrono_dates(mut self, use_chrono_dates: bool) -> GeneratorBuilder {
        self.use_chrono_dates = use_chrono_dates;
        self
    }

    /// Adds support to derive builders using the `rust-derive-builder` crate.
    ///
    /// Applies to record structs.
    pub fn derive_builders(mut self, derive_builders: bool) -> GeneratorBuilder {
        self.derive_builders = derive_builders;
        self
    }

    /// Adds support to derive [`avro_schema::AvroSchema`](avro_schema::AvroSchema).
    ///
    /// Applies to record structs.
    pub fn derive_schemas(mut self, derive_schemas: bool) -> GeneratorBuilder {
        self.derive_schemas = derive_schemas;
        self
    }

    /// Adds support to derive custom macros.
    ///
    /// Applies to record structs.
    pub fn extra_derives(mut self, extra_derives: Vec<String>) -> GeneratorBuilder {
        self.extra_derives = extra_derives;
        self
    }

    /// Create a [`Generator`](Generator) with the builder parameters.
    pub fn build(self) -> Result<Generator> {
        let mut templater = Templater::new()?;
        templater.precision = self.precision;
        templater.nullable = self.nullable;
        templater.use_avro_rs_unions = self.use_avro_rs_unions;
        templater.use_chrono_dates = self.use_chrono_dates;
        templater.derive_builders = self.derive_builders;
        templater.derive_schemas = self.derive_schemas;
        templater.extra_derives = self.extra_derives;
        Ok(Generator { templater })
    }
}

#[cfg(test)]
mod tests {
    use apache_avro::schema::{EnumSchema, Name};
    use pretty_assertions::assert_eq;

    use super::*;

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

        let schema = Schema::parse_str(raw_schema).unwrap();
        let mut deps = deps_stack(&schema, vec![]);

        let s = deps.pop().unwrap();
        assert!(
            matches!(s, Schema::Enum(EnumSchema{ name: Name { ref name, ..}, ..}) if name == "Country")
        );

        let s = deps.pop().unwrap();
        assert!(
            matches!(s, Schema::Record(RecordSchema{ name: Name { ref name, ..}, ..}) if name == "Address")
        );

        let s = deps.pop().unwrap();
        assert!(
            matches!(s, Schema::Record(RecordSchema{ name: Name { ref name, ..}, ..}) if name == "User")
        );

        let s = deps.pop();
        assert!(s.is_none());
    }

    #[test]
    fn cross_deps() -> std::result::Result<(), Box<dyn std::error::Error>> {
        use std::fs::File;
        use std::io::Write;
        use tempfile::tempdir;

        let dir = tempdir()?;

        let mut schema_a_file = File::create(dir.path().join("schema_a.avsc"))?;
        let schema_a_str = r#"
{
  "name": "A",
  "type": "record",
  "fields": [ {"name": "field_one", "type": "float"} ]
}
"#;
        schema_a_file.write_all(schema_a_str.as_bytes())?;

        let mut schema_b_file = File::create(dir.path().join("schema_b.avsc"))?;
        let schema_b_str = r#"
{
  "name": "B",
  "type": "record",
  "fields": [ {"name": "field_one", "type": "A"} ]
}
"#;
        schema_b_file.write_all(schema_b_str.as_bytes())?;

        let expected = r#"
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct B {
    pub field_one: A,
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct A {
    pub field_one: f32,
}
"#;

        let pattern = format!("{}/*.avsc", dir.path().display());
        let source = Source::GlobPattern(pattern.as_str());
        let g = Generator::new()?;
        let mut buf = vec![];
        g.generate(&source, &mut buf)?;
        let res = String::from_utf8(buf)?;

        assert_eq!(expected, res);

        drop(schema_a_file);
        drop(schema_b_file);
        dir.close()?;
        Ok(())
    }
}
