use std::collections::HashMap;
use std::collections::HashSet;

use avro_rs::schema::{Name, RecordField};
use avro_rs::Schema;
use failure::{Error, SyncFailure};
use heck::{CamelCase, SnakeCase};
use serde_json::Value;
use tera::{Context, Tera};

pub const RECORD_TERA: &str = "record.tera";
pub const RECORD_TEMPLATE: &str = "
#[serde(default)]
#[derive(Debug, Deserialize, Serialize)]
pub struct {{ name }} {
    {%- for f, type in fields %}
    {%- if f != originals[f] %}
    #[serde(rename = \"{{ originals[f] }}\")]
    {%- endif %}
    pub {{ f }}: {{ type }},
    {%- endfor %}
}

impl Default for {{ name }} {
    fn default() -> {{ name }} {
        {{ name }} {
            {%- for f, value in defaults %}
            {{ f }}: {{ value }},
            {%- endfor %}
        }
    }
}
";

pub const ENUM_TERA: &str = "enum.tera";
pub const ENUM_TEMPLATE: &str = "
#[derive(Debug, Deserialize, Serialize)]
pub enum {{ name }} {
    {%- for s, o in symbols %}
    {%- if s != o %}
    #[serde(rename = \"{{ o }}\")]
    {%- endif %}
    {{ s }},
    {%- endfor %}
}
";

pub const FIXED_TERA: &str = "fixed.tera";
pub const FIXED_TEMPLATE: &str = "
pub type {{ name }} = [u8; {{ size }}];
";

lazy_static! {
    static ref RESERVED: HashSet<String> = {
        let s: HashSet<_> = vec![
            "as", "break", "const", "continue", "crate", "else", "enum", "extern", "false", "fn",
            "for", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref",
            "return", "Self", "self", "static", "struct", "super", "trait", "true", "type",
            "unsage", "use", "where", "while", "abstract", "alignof", "become", "box", "do",
            "final", "macro", "offsetof", "override", "priv", "proc", "pure", "sizeof", "typeof",
            "unsized", "virtual", "yields",
        ].iter()
        .map(|s| s.to_string())
        .collect();
        s
    };
}

fn sanitize(mut s: String) -> String {
    if RESERVED.contains(&s) {
        s.push_str("_");
        s
    } else {
        s
    }
}

/// Describes errors happened while templating Rust code.
#[derive(Fail, Debug)]
#[fail(display = "Template failure: {}", _0)]
pub struct TemplateError(String);

impl TemplateError {
    pub fn new<S>(msg: S) -> TemplateError
    where
        S: Into<String>,
    {
        TemplateError(msg.into())
    }
}

macro_rules! err(
    ($($arg:tt)*) => (Err(TemplateError::new(format!($($arg)*))))
);

// https://github.com/rust-lang-nursery/failure/issues/109
trait ResultExt<T, E> {
    fn sync(self) -> Result<T, SyncFailure<E>>
    where
        Self: Sized,
        E: ::std::error::Error + Send + 'static;
}

// https://github.com/rust-lang-nursery/failure/issues/109
impl<T, E> ResultExt<T, E> for Result<T, E> {
    fn sync(self) -> Result<T, SyncFailure<E>>
    where
        Self: Sized,
        E: ::std::error::Error + Send + 'static,
    {
        self.map_err(SyncFailure::new)
    }
}

pub struct Templater {
    tera: Tera,
}

/// TODO handle arrays
/// TODO handle ["null", ...]
impl Templater {
    pub fn new() -> Result<Templater, Error> {
        let mut tera = Tera::new("/dev/null/*").sync()?;
        tera.add_raw_template(RECORD_TERA, RECORD_TEMPLATE).sync()?;
        tera.add_raw_template(ENUM_TERA, ENUM_TEMPLATE).sync()?;
        tera.add_raw_template(FIXED_TERA, FIXED_TEMPLATE).sync()?;
        Ok(Templater { tera })
    }

    pub fn str_fixed(&self, schema: &Schema) -> Result<String, Error> {
        if let Schema::Fixed {
            name: Name { name, .. },
            size,
        } = schema
        {
            let mut ctx = Context::new();
            ctx.add("name", &name.to_camel_case());
            ctx.add("size", size);

            Ok(self.tera.render(FIXED_TERA, &ctx).sync()?)
        } else {
            err!("Requires Schema::Fixed, found {:?}", schema)?
        }
    }

    pub fn str_enum(&self, schema: &Schema) -> Result<String, Error> {
        if let Schema::Enum {
            name: Name { name, .. },
            symbols,
            ..
        } = schema
        {
            if symbols.len() == 0 {
                err!("No symbol for emum: {:?}", name)?
            }
            let mut ctx = Context::new();
            ctx.add("name", &name.to_camel_case());
            let s: HashMap<_, _> = symbols
                .iter()
                .map(|s| (sanitize(s.to_camel_case()), s))
                .collect();
            ctx.add("symbols", &s);
            Ok(self.tera.render(ENUM_TERA, &ctx).sync()?)
        } else {
            err!("Requires Schema::Enum, found {:?}", schema)?
        }
    }

    pub fn str_record(&self, schema: &Schema) -> Result<String, Error> {
        if let Schema::Record {
            name: Name { name, .. },
            fields,
            ..
        } = schema
        {
            let mut ctx = Context::new();
            ctx.add("name", &name.to_camel_case());

            let mut f = HashMap::new(); // field name -> field type
            let mut o = HashMap::new(); // field name -> original name
            let mut d = HashMap::new(); // field name -> default value
            for RecordField {
                schema,
                name,
                default,
                ..
            } in fields
            {
                let name_std = sanitize(name.to_snake_case());
                o.insert(name_std.clone(), name);

                match schema {
                    Schema::Boolean => {
                        f.insert(name_std.clone(), "bool".to_string());
                        if let Some(Value::Bool(b)) = default {
                            d.insert(name_std.clone(), b.to_string());
                        } else {
                            d.insert(name_std.clone(), bool::default().to_string());
                        }
                    }

                    Schema::Int => {
                        f.insert(name_std.clone(), "i32".to_string());
                        match default {
                            Some(Value::Number(n)) if n.is_i64() => {
                                d.insert(name_std.clone(), n.to_string())
                            }
                            _ => d.insert(name_std.clone(), i32::default().to_string()),
                        };
                    }

                    Schema::Long => {
                        f.insert(name_std.clone(), "i64".to_string());
                        match default {
                            Some(Value::Number(n)) if n.is_i64() => {
                                d.insert(name_std.clone(), n.to_string())
                            }
                            _ => d.insert(name_std.clone(), i64::default().to_string()),
                        };
                    }

                    Schema::Float => {
                        f.insert(name_std.clone(), "f32".to_string());
                        match default {
                            Some(Value::Number(n)) if n.is_f64() => {
                                d.insert(name_std.clone(), n.to_string())
                            }
                            _ => d.insert(name_std.clone(), f32::default().to_string()),
                        };
                    }

                    Schema::Double => {
                        f.insert(name_std.clone(), "f64".to_string());
                        match default {
                            Some(Value::Number(n)) if n.is_f64() => {
                                d.insert(name_std.clone(), n.to_string())
                            }
                            _ => d.insert(name_std.clone(), f64::default().to_string()),
                        };
                    }

                    Schema::Bytes => {
                        f.insert(name_std.clone(), "Vec<u8>".to_string());
                        if let Some(Value::String(s)) = default {
                            let bytes = s.clone().into_bytes();
                            d.insert(name_std.clone(), format!("vec!{:?}", bytes))
                        } else {
                            d.insert(name_std.clone(), "vec![]".to_string())
                        };
                    }

                    Schema::String => {
                        f.insert(name_std.clone(), "String".to_string());
                        if let Some(Value::String(s)) = default {
                            d.insert(name_std.clone(), format!("\"{}\".to_owned()", s));
                        } else {
                            d.insert(name_std.clone(), "String::default()".to_string());
                        }
                    }

                    Schema::Fixed {
                        name: Name { name: f_name, .. },
                        size,
                    } => {
                        let f_name = sanitize(f_name.to_camel_case());
                        f.insert(name_std.clone(), f_name.clone());

                        if let Some(Value::String(s)) = default {
                            let bytes: Vec<u8> = s.clone().into_bytes();
                            if bytes.len() != *size {
                                err!("Invalid defaults: {:?}", bytes)?
                            }
                            d.insert(name_std.clone(), format!("{:?}", bytes))
                        } else {
                            d.insert(name_std.clone(), format!("{}::default()", f_name))
                        };
                    }

                    /// TODO save name_std-> type_str in some GenState
                    ///      for potentially recursive array/map ?
                    Schema::Array(schema) => match &**schema {
                        Schema::Boolean
                        | Schema::Int
                        | Schema::Long
                        | Schema::Float
                        | Schema::Double
                        | Schema::Bytes
                        | Schema::String
                        | Schema::Fixed { .. } => {
                            let (type_str, default_str) = default_array(&**schema, default)?;
                            f.insert(name_std.clone(), type_str);
                            d.insert(name_std.clone(), default_str);
                        }

                        Schema::Array(..) => (),
                        Schema::Map(..) => (),
                        Schema::Record { .. } => (),
                        Schema::Enum { .. } => (),
                        Schema::Union(..) => (),

                        Schema::Null => err!("Invalid use of Schema::Null")?,
                    },

                    /// TODO add support
                    Schema::Map(schema) => err!("Unhandled type: {:?}", schema)?,

                    Schema::Record {
                        name: Name { name: r_name, .. },
                        ..
                    } => {
                        let r_name = sanitize(r_name.to_camel_case());
                        f.insert(name_std.clone(), r_name.clone());
                        d.insert(name_std.clone(), format!("{}::default()", r_name));
                    }

                    Schema::Enum {
                        name: Name { name: e_name, .. },
                        symbols,
                        ..
                    } => {
                        let e_name = sanitize(e_name.to_camel_case());
                        f.insert(name_std.clone(), e_name);

                        if let Some(Value::String(s)) = default {
                            d.insert(name_std.clone(), s.clone());
                        } else if !symbols.is_empty() {
                            d.insert(name_std.clone(), symbols[0].to_string());
                        } else {
                            err!("No symbol for emum: {:?}", name)?
                        }
                    }

                    Schema::Union(union) => {
                        if let [Schema::Null, schema] = union.variants() {
                            let type_opt = match schema {
                                Schema::Boolean => "bool".to_string(),
                                Schema::Int => "i32".to_string(),
                                Schema::Long => "i64".to_string(),
                                Schema::Float => "f32".to_string(),
                                Schema::Double => "f64".to_string(),
                                Schema::Bytes => "Vec<u8>".to_string(),
                                Schema::String => "String".to_string(),
                                Schema::Fixed {
                                    name: Name { name: f_name, .. },
                                    ..
                                } => sanitize(f_name.to_camel_case()),

                                /// TODO add support
                                Schema::Array(..) => unreachable!(),
                                Schema::Map(..) => unreachable!(),
                                Schema::Record { .. } => unreachable!(),
                                Schema::Enum { .. } => unreachable!(),

                                Schema::Union(_) => err!("Unsupported nested Schema::Union")?,
                                Schema::Null => err!("Invalid use of Schema::Null")?,
                            };
                            f.insert(name_std.clone(), format!("Option<{}>", type_opt));

                            let default_opt = match default {
                                None => "None".to_string(),
                                Some(Value::String(s)) if s == "null" => "None".to_string(),
                                Some(Value::String(s)) if s != "null" => {
                                    err!("Invalid defaults: {:?}", s)?
                                }
                                _ => err!("Invalid defaults: {:?}", default)?,
                            };
                            d.insert(name_std.clone(), default_opt);
                        } else {
                            err!("Unsupported Schema:::Union {:?}", union.variants())?
                        }
                    }

                    Schema::Null => err!("Invalid use of Schema::Null")?,
                };
            }
            ctx.add("fields", &f);
            ctx.add("originals", &o);
            ctx.add("defaults", &d);

            Ok(self.tera.render(RECORD_TERA, &ctx).sync()?)
        } else {
            err!("Requires Schema::Record, found {:?}", schema)?
        }
    }
}

fn default_array(schema: &Schema, default: &Option<Value>) -> Result<(String, String), Error> {
    match schema {
        Schema::Boolean => {
            let type_str = "Vec<bool>".to_string();
            let default_str = if let Some(Value::Array(vals)) = default {
                let vals = vals
                    .iter()
                    .map(|v| match v {
                        Value::Bool(b) => Ok(b.to_string()),
                        _ => err!("Invalid defaults: {:?}", v),
                    }).collect::<Result<Vec<String>, TemplateError>>()?
                    .as_slice()
                    .join(", ");
                format!("vec![{}]", vals)
            } else {
                "vec![]".to_string()
            };
            Ok((type_str, default_str))
        }

        Schema::Int => {
            let type_str = "Vec<i32>".to_string();
            let default_str = if let Some(Value::Array(vals)) = default {
                let vals = vals
                    .iter()
                    .map(|v| match v {
                        Value::Number(n) if n.is_i64() => Ok(n.to_string()),
                        _ => err!("Invalid defaults: {:?}", v),
                    }).collect::<Result<Vec<String>, TemplateError>>()?
                    .as_slice()
                    .join(", ");
                format!("vec![{}]", vals)
            } else {
                "vec![]".to_string()
            };
            Ok((type_str, default_str))
        }

        Schema::Long => {
            let type_str = "Vec<i64>".to_string();
            let default_str = if let Some(Value::Array(vals)) = default {
                let vals = vals
                    .iter()
                    .map(|v| match v {
                        Value::Number(n) if n.is_i64() => Ok(n.to_string()),
                        _ => err!("Invalid defaults: {:?}", v),
                    }).collect::<Result<Vec<String>, TemplateError>>()?
                    .as_slice()
                    .join(", ");
                format!("vec![{}]", vals)
            } else {
                "vec![]".to_string()
            };
            Ok((type_str, default_str))
        }

        Schema::Float => {
            let type_str = "Vec<f32>".to_string();
            let default_str = if let Some(Value::Array(vals)) = default {
                let vals = vals
                    .iter()
                    .map(|v| match v {
                        Value::Number(n) if n.is_f64() => Ok(n.to_string()),
                        _ => err!("Invalid defaults: {:?}", v),
                    }).collect::<Result<Vec<String>, TemplateError>>()?
                    .as_slice()
                    .join(", ");
                format!("vec![{}]", vals)
            } else {
                "vec![]".to_string()
            };
            Ok((type_str, default_str))
        }

        Schema::Double => {
            let type_str = "Vec<f64>".to_string();
            let default_str = if let Some(Value::Array(vals)) = default {
                let vals = vals
                    .iter()
                    .map(|v| match v {
                        Value::Number(n) if n.is_f64() => Ok(n.to_string()),
                        _ => err!("Invalid defaults: {:?}", v),
                    }).collect::<Result<Vec<String>, TemplateError>>()?
                    .as_slice()
                    .join(", ");
                format!("vec![{}]", vals)
            } else {
                "vec![]".to_string()
            };
            Ok((type_str, default_str))
        }

        Schema::Bytes => {
            let type_str = "Vec<Vec<u8>>".to_string();
            let default_str = if let Some(Value::Array(vals)) = default {
                let vals = vals
                    .iter()
                    .map(|v| match v {
                        Value::String(s) => {
                            let bytes = s.clone().into_bytes();
                            Ok(format!("vec!{:?}", bytes))
                        }
                        _ => err!("Invalid defaults: {:?}", v),
                    }).collect::<Result<Vec<String>, TemplateError>>()?
                    .as_slice()
                    .join(", ");
                format!("vec![{}]", vals)
            } else {
                "vec![]".to_string()
            };
            Ok((type_str, default_str))
        }

        Schema::String => {
            let type_str = "Vec<String>".to_string();
            let default_str = if let Some(Value::Array(vals)) = default {
                let vals = vals
                    .iter()
                    .map(|v| match v {
                        Value::String(s) => Ok(s.clone()),
                        _ => err!("Invalid defaults: {:?}", v),
                    }).collect::<Result<Vec<String>, TemplateError>>()?
                    .as_slice()
                    .join(", ");
                format!("vec![{}]", vals)
            } else {
                "vec![]".to_string()
            };
            Ok((type_str, default_str))
        }

        Schema::Fixed {
            name: Name { name: f_name, .. },
            size,
        } => {
            let f_name = sanitize(f_name.to_camel_case());
            let type_str = format!("Vec<{}>", f_name);
            let default_str = if let Some(Value::Array(vals)) = default {
                let vals = vals
                    .iter()
                    .map(|v| match v {
                        Value::String(s) => {
                            let bytes: Vec<u8> = s.clone().into_bytes();
                            if bytes.len() != *size {
                                err!("Invalid defaults: {:?}", bytes)
                            } else {
                                Ok(format!("{:?}", bytes))
                            }
                        }
                        _ => err!("Invalid defaults: {:?}", v),
                    }).collect::<Result<Vec<String>, TemplateError>>()?
                    .as_slice()
                    .join(", ");
                format!("vec![{}]", vals)
            } else {
                "vec![]".to_string()
            };
            Ok((type_str, default_str))
        }

        /// TODO add support
        Schema::Array(..) => unreachable!(),
        Schema::Map(..) => unreachable!(),
        Schema::Record { .. } => unreachable!(),
        Schema::Enum { .. } => unreachable!(),
        Schema::Union(..) => unreachable!(),

        Schema::Null => err!("Invalid use of Schema::Null")?,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tera() {
        let raw_schema = r#"
        {"namespace": "example.avro",
         "type": "record",
         "name": "User",
         "fields": [
             {"name": "as", "type": "string"},
             {"name": "favoriteNumber",  "type": "int", "default": 7},
             {"name": "likes_pizza", "type": "boolean", "default": false},
             {"name": "b", "type": "bytes", "default": "\u00FF"},
             {"name": "t-bool", "type": {"type": "array", "items": "boolean"}, "default": [true, false]},
             {"name": "t-i32", "type": {"type": "array", "items": "int"}, "default": [12, -1]}
         ]
        }"#;

        let templater = Templater::new().unwrap();
        let schema = Schema::parse_str(&raw_schema).unwrap();
        let res = templater.str_record(&schema).unwrap();
        println!("{}", res);
    }

    #[test]
    fn tero() {
        let raw_schema = r#"
        {"type": "enum",
         "name": "Colors",
         "symbols": ["GREEN", "BLUE"]
        }"#;

        let templater = Templater::new().unwrap();
        let schema = Schema::parse_str(&raw_schema).unwrap();
        let res = templater.str_enum(&schema).unwrap();
        println!("{}", res);
    }

    #[test]
    fn teri() {
        let raw_schema = r#"
        {"type": "fixed",
         "name": "Md5",
         "size": 2
        }"#;

        let templater = Templater::new().unwrap();
        let schema = Schema::parse_str(&raw_schema).unwrap();
        let res = templater.str_fixed(&schema).unwrap();
        println!("{}", res);
    }
}
