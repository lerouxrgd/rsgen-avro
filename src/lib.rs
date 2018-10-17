extern crate avro_rs;
extern crate failure;
extern crate heck;
#[macro_use]
extern crate failure_derive;
#[macro_use]
extern crate lazy_static;
extern crate serde_json;
extern crate tera;

mod templates;

use std::collections::VecDeque;
use std::fs::File;
use std::io::prelude::*;

use avro_rs::{schema::RecordField, Schema};
use failure::Error;

use templates::*;

/// Describes errors happened while generating Rust code.
#[derive(Fail, Debug)]
#[fail(display = "Rsgen failure: {}", _0)]
pub struct RsgenError(String);

impl RsgenError {
    pub fn new<S>(msg: S) -> RsgenError
    where
        S: Into<String>,
    {
        RsgenError(msg.into())
    }
}

pub enum Source<'a> {
    Schema(&'a Schema),
    SchemaStr(&'a str),
    FilePath(&'a str),
    DirPath(&'a str),
}

pub struct Generator {
    templater: Templater,
}

impl Generator {
    pub fn new() -> Result<Generator, Error> {
        GeneratorBuilder::new().build()
    }

    pub fn builder() -> GeneratorBuilder {
        GeneratorBuilder::new()
    }

    pub fn gen(&self, source: &Source, output: &mut Box<Write>) -> Result<(), Error> {
        match source {
            Source::Schema(schema) => self.gen_in_order(schema, output)?,

            Source::SchemaStr(raw_schema) => {
                let schema = Schema::parse_str(&raw_schema)?;
                self.gen_in_order(&schema, output)?
            }

            Source::FilePath(schema_file) => {
                let mut raw_schema = String::new();
                File::open(&schema_file)?.read_to_string(&mut raw_schema)?;
                let schema = Schema::parse_str(&raw_schema)?;
                self.gen_in_order(&schema, output)?
            }

            Source::DirPath(schemas_dir) => {
                for entry in std::fs::read_dir(schemas_dir)? {
                    let path = entry?.path();
                    if !path.is_dir() {
                        let mut raw_schema = String::new();
                        File::open(&path)?.read_to_string(&mut raw_schema)?;
                        let schema = Schema::parse_str(&raw_schema)?;
                        self.gen_in_order(&schema, output)?
                    }
                }
            }
        }

        Ok(())
    }

    fn gen_in_order(&self, schema: &Schema, output: &mut Box<Write>) -> Result<(), Error> {
        /// TODO consider using some stateful Namimg/Defaults struct while templating
        let mut deps = deps_stack(schema);
        while let Some(s) = deps.pop() {
            match s {
                Schema::Fixed { .. } => {
                    let code = &self.templater.str_record(&s)?;
                    output.write_all(code.as_bytes())?
                }

                Schema::Enum { .. } => {
                    let code = &self.templater.str_enum(&s)?;
                    output.write_all(code.as_bytes())?
                }

                Schema::Record { .. } => {
                    let code = &self.templater.str_record(&s)?;
                    output.write_all(code.as_bytes())?
                }

                _ => Err(RsgenError::new(format!("Not a valid root schema: {:?}", s)))?,
            }
        }
        Ok(())
    }
}

pub struct GeneratorBuilder {
    // derive_deser: Option<bool>,
    templater: Option<Templater>,
}

impl GeneratorBuilder {
    pub fn new() -> GeneratorBuilder {
        GeneratorBuilder {
            // derive_deser: Some(true),
            templater: None,
        }
    }

    pub fn templater(mut self, templater: Templater) -> GeneratorBuilder {
        self.templater = Some(templater);
        self
    }

    // pub fn derive_deser(mut self, derive_deser: bool) -> GeneratorBuilder<'a> {
    //     self.derive_deser = Some(derive_deser);
    //     self
    // }

    pub fn build(self) -> Result<Generator, Error> {
        // let derive_deser = self
        //     .derive_deser
        //     .ok_or_else(|| RsgenError::new("`derive_deser` is not set"));

        let templater = self
            .templater
            .ok_or_else(|| Templater::new())
            .or_else(|e| e)?;

        Ok(Generator { templater })
    }
}

fn deps_stack(schema: &Schema) -> Vec<&Schema> {
    let mut deps = Vec::new();
    let mut q = VecDeque::new();

    q.push_back(schema);
    while !q.is_empty() {
        let s = q.pop_front().unwrap();
        deps.push(s);

        match s {
            Schema::Fixed { .. } => deps.push(s),
            Schema::Enum { .. } => deps.push(s),
            Schema::Record { fields, .. } => {
                for RecordField { schema: sr, .. } in fields {
                    match sr {
                        Schema::Fixed { .. } => deps.push(sr),
                        Schema::Enum { .. } => deps.push(sr),
                        Schema::Record { .. } => q.push_back(sr),
                        Schema::Map(sc) | Schema::Array(sc) => match &**sc {
                            Schema::Fixed { .. }
                            | Schema::Enum { .. }
                            | Schema::Record { .. }
                            | Schema::Map(..)
                            | Schema::Array(..) => q.push_back(&**sc),
                            _ => (),
                        },
                        _ => (),
                    };
                }
            }
            Schema::Map(sc) | Schema::Array(sc) => match &**sc {
                Schema::Fixed { .. }
                | Schema::Enum { .. }
                | Schema::Record { .. }
                | Schema::Map(..)
                | Schema::Array(..) => q.push_back(&**sc),
                _ => (),
            },
            _ => (),
        }
    }

    deps
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::stdout;

    #[test]
    fn record() {
        let raw_schema = r#"
        {"namespace": "example.avro",
         "type": "record",
         "name": "User",
         "fields": [
             {"name": "name", "type": "string", "default": ""},
             {"name": "favorite_number",  "type": "int", "default": 7},
             {"name": "likes_pizza", "type": "boolean", "default": false}
         ]
        }
        "#;

        /// TODO put that example in some doc
        // let mut out = Box::new(
        //     OpenOptions::new()
        //         .write(true)
        //         .create(true)
        //         .open("coucou.rs")
        //         .unwrap(),
        // ) as Box<Write>;
        let mut out: Box<Write> = Box::new(stdout());

        let schema = Schema::parse_str(&raw_schema).unwrap();
        let source = Source::Schema(&schema);

        let g = Generator::new().unwrap();
        g.gen(&source, &mut out).unwrap();
    }

    #[test]
    fn deps() {
        let raw_schema = r#"
{"type": "record",
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
   },
   {"name": "likes_pizza", "type": "boolean", "default": false}
 ]
}
"#;

        let schema = Schema::parse_str(&raw_schema).unwrap();
        let deps = deps_stack(&schema);
        let depss = deps
            .iter()
            .map(|s| format!("{:?}", s))
            .collect::<Vec<String>>()
            .as_slice()
            .join("\n\n");
        println!("{}", depss);
    }
}
