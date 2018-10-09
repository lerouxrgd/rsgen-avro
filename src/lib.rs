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

use avro_rs::Schema;
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
    Str(&'a str),
    File(&'a str),
    Dir(&'a str),
}

pub struct Generator<'a> {
    src: Source<'a>,
    templater: Templater,
}

impl<'a> Generator<'a> {
    pub fn new(src: Source) -> Result<Generator, Error> {
        GeneratorBuilder::new(src).build()
    }

    pub fn builder(src: Source) -> GeneratorBuilder {
        GeneratorBuilder::new(src)
    }

    pub fn gen(&self, out: &mut Box<Write>) -> Result<(), Error> {
        match &self.src {
            Source::Schema(schema) => {
                gen_record(schema, out, &self.templater)?;
                Ok(())
            }

            Source::Str(raw_schema) => {
                let schema = Schema::parse_str(&raw_schema)?;
                gen_record(&schema, out, &self.templater)?;
                Ok(())
            }

            Source::File(schema_file) => {
                let mut raw_schema = String::new();
                File::open(&schema_file)?.read_to_string(&mut raw_schema)?;
                let schema = Schema::parse_str(&raw_schema)?;
                gen_record(&schema, out, &self.templater)?;
                Ok(())
            }

            Source::Dir(schemas_dir) => {
                for entry in std::fs::read_dir(schemas_dir)? {
                    let path = entry?.path();
                    if !path.is_dir() {
                        let mut raw_schema = String::new();
                        File::open(&path)?.read_to_string(&mut raw_schema)?;
                        let schema = Schema::parse_str(&raw_schema)?;
                        gen_record(&schema, out, &self.templater)?;
                    }
                }
                Ok(())
            }
        }
    }
}

pub struct GeneratorBuilder<'a> {
    src: Source<'a>,
    // derive_deser: Option<bool>,
    templater: Option<Templater>,
}

impl<'a> GeneratorBuilder<'a> {
    pub fn new(src: Source) -> GeneratorBuilder {
        GeneratorBuilder {
            src,
            // derive_deser: Some(true),
            templater: None,
        }
    }

    pub fn templater(mut self, templater: Templater) -> GeneratorBuilder<'a> {
        self.templater = Some(templater);
        self
    }

    // pub fn derive_deser(mut self, derive_deser: bool) -> GeneratorBuilder<'a> {
    //     self.derive_deser = Some(derive_deser);
    //     self
    // }

    pub fn build(self) -> Result<Generator<'a>, Error> {
        let src = self.src;

        // let derive_deser = self
        //     .derive_deser
        //     .ok_or_else(|| RsgenError::new("`derive_deser` is not set"));

        let templater = self
            .templater
            .ok_or_else(|| Templater::new())
            .or_else(|e| e)?;

        Ok(Generator { src, templater })
    }
}

fn gen_record(schema: &Schema, out: &mut Box<Write>, templater: &Templater) -> Result<(), Error> {
    match schema {
        Schema::Record { .. } => (),
        _ => Err(RsgenError::new(format!(
            "Requires Schema::Record, found {:?}",
            schema
        )))?,
    }

    let mut q = VecDeque::new();
    q.push_back(schema);

    while !q.is_empty() {
        let s = q.pop_front().unwrap();
        let code = templater
            .str_record(&s)?
            .done
            .ok_or_else(|| RsgenError::new("Invalid GenState, empty result"))?;
        out.write_all(code.as_bytes())?;

        match s {
            Schema::Record { fields, .. } => {
                for field in fields {
                    match field.schema {
                        Schema::Enum { .. } => {
                            let code = templater.str_enum(&s)?;
                            out.write_all(code.as_bytes())?;
                        }
                        Schema::Record { .. } => {
                            q.push_back(&field.schema);
                        }
                        _ => (),
                    };
                }
            }
            _ => unreachable!(),
        }
    }
    Ok(())
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
        let g = Generator::new(Source::Schema(&schema)).unwrap();
        g.gen(&mut out).unwrap();
    }
}
