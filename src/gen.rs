use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::prelude::*;

use apache_avro::{schema::RecordField, Schema};

use crate::error::{Error, Result};
use crate::templates::*;

/// Represents a schema input source.
pub enum Source<'a> {
    /// An Avro schema enum from `apache-avro` crate.
    Schema(&'a Schema),
    /// An Avro schema string in json format.
    SchemaStr(&'a str),
    /// Pattern for selecting files containing Avro schemas in json format.
    GlobPattern(&'a str),
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
        match source {
            Source::Schema(schema) => {
                let mut deps = deps_stack(schema, vec![]);
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
                let mut deps = schemas
                    .iter()
                    .fold(vec![], |deps, schema| deps_stack(schema, deps));

                self.gen_in_order(&mut deps, output)?;
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
        let mut gs = GenState::with_deps(deps);

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
                Schema::Array(ref inner) => {
                    let type_str = array_type(inner, &gs)?;
                    gs.put_type(&s, type_str)
                }
                Schema::Map(ref inner) => {
                    let type_str = map_type(inner, &gs)?;
                    gs.put_type(&s, type_str)
                }

                Schema::Union(ref union) => {
                    // Generate custom enum with potentially nested types
                    if (union.is_nullable() && union.variants().len() > 2)
                        || (!union.is_nullable() && union.variants().len() > 1)
                    {
                        let code = &self.templater.str_union_enum(&s, &gs)?;
                        output.write_all(code.as_bytes())?
                    }

                    // Register inner union for it to be used as a nested type later
                    let type_str = union_type(union, &gs, true)?;
                    gs.put_type(&s, type_str)
                }

                _ => return Err(Error::Schema(format!("Not a valid root schema: {:?}", s))),
            }
        }

        Ok(())
    }
}

/// Utility function to find the ordered, nested dependencies of an Avro `schema`.
/// Explores nested `schema`s in a breadth-first fashion, pushing them on a stack
/// at the same time in order to have them ordered.
/// It is similar to traversing the `schema` tree in a post-order fashion.
fn deps_stack(schema: &Schema, mut deps: Vec<Schema>) -> Vec<Schema> {
    fn push_unique(deps: &mut Vec<Schema>, s: Schema) {
        if !deps.contains(&s) {
            deps.push(s);
        }
    }

    let mut q = VecDeque::new();

    q.push_back(schema);
    while !q.is_empty() {
        let s = q.pop_front().unwrap();

        match s {
            // No nested schemas, add them to the result stack
            Schema::Enum { .. } => push_unique(&mut deps, s.clone()),
            Schema::Fixed { .. } => push_unique(&mut deps, s.clone()),
            Schema::Decimal { inner, .. } if matches!(**inner, Schema::Fixed { .. }) => {
                push_unique(&mut deps, s.clone())
            }

            // Explore the record fields for potentially nested schemas
            Schema::Record { fields, .. } => {
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
                            if (union.is_nullable() && union.variants().len() > 2)
                                || (!union.is_nullable() && union.variants().len() > 1)
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
            Schema::Map(sc) | Schema::Array(sc) => match &**sc {
                // ... Needs further checks, push to the exploration queue
                Schema::Fixed { .. }
                | Schema::Enum { .. }
                | Schema::Record { .. }
                | Schema::Map(..)
                | Schema::Array(..)
                | Schema::Union(..) => q.push_back(&**sc),
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
                    | Schema::Union(..) => q.push_back(sc),
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
    derive_builders: bool,
}

impl Default for GeneratorBuilder {
    fn default() -> Self {
        Self {
            precision: 3,
            nullable: false,
            use_avro_rs_unions: false,
            derive_builders: false,
        }
    }
}

impl GeneratorBuilder {
    /// Creates a new `GeneratorBuilder`.
    pub fn new() -> GeneratorBuilder {
        GeneratorBuilder::default()
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

    /// Adds support for deserializing union types from the `apache-avro` crate.
    /// Only necessary for unions of 3 or more types or 2-type unions without "null".
    /// Note that only int, long, float, double, and boolean values are currently supported.
    pub fn use_avro_rs_unions(mut self, use_avro_rs_unions: bool) -> GeneratorBuilder {
        self.use_avro_rs_unions = use_avro_rs_unions;
        self
    }

    /// Adds support to derive builders using the `rust-derive-builder` crate.
    /// Will derive builders for record structs.
    pub fn derive_builders(mut self, derive_builders: bool) -> GeneratorBuilder {
        self.derive_builders = derive_builders;
        self
    }

    /// Create a `Generator` with the builder parameters.
    pub fn build(self) -> Result<Generator> {
        let mut templater = Templater::new()?;
        templater.precision = self.precision;
        templater.nullable = self.nullable;
        templater.use_avro_rs_unions = self.use_avro_rs_unions;
        templater.derive_builders = self.derive_builders;
        Ok(Generator { templater })
    }
}

#[cfg(test)]
mod tests {
    use apache_avro::schema::Name;

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
        assert!(matches!(s, Schema::Enum{ name: Name { ref name, ..}, ..} if name == "Country"));

        let s = deps.pop().unwrap();
        assert!(matches!(s, Schema::Record{ name: Name { ref name, ..}, ..} if name == "Address"));

        let s = deps.pop().unwrap();
        assert!(matches!(s, Schema::Record{ name: Name { ref name, ..}, ..} if name == "User"));

        let s = deps.pop();
        assert!(matches!(s, None));
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
        g.gen(&source, &mut buf)?;
        let res = String::from_utf8(buf)?;
        println!("{}", res);

        assert_eq!(expected, res);

        drop(schema_a_file);
        drop(schema_b_file);
        dir.close()?;
        Ok(())
    }
}
