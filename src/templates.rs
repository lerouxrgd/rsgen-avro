//! Logic for templating Rust types and default values from Avro schema.
use std::collections::{HashMap, HashSet};

use avro_rs::schema::{Name, RecordField};
use avro_rs::Schema;
use by_address::ByAddress;
use failure::{Error, Fail, SyncFailure};
use heck::{CamelCase, SnakeCase};
use lazy_static::lazy_static;
use serde_json::Value;
use tera::{Context, Tera};

pub const SERDE_TERA: &str = "serde.tera";
pub const SERDE_TEMPLATE: &str =
    "use serde::{Deserialize{% if nullable %}, Deserializer{% endif %}, Serialize};
";

pub const DESER_NULLABLE: &str = r#"
macro_rules! deser(
    ($name:ident, $rtype:ty, $val:expr) => (
        fn $name<'de, D>(deserializer: D) -> Result<$rtype, D::Error>
        where
            D: Deserializer<'de>,
        {
            let opt = Option::deserialize(deserializer)?;
            Ok(opt.unwrap_or_else(|| $val))
        }
    );
);
"#;

pub const RECORD_TERA: &str = "record.tera";
pub const RECORD_TEMPLATE: &str = r#"
{%- if doc %}
/// {{ doc }}
{%- endif %}
#[serde(default)]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct {{ name }} {
    {%- for f, type in fields %}
    {%- if f != originals[f] and not nullable %}
    #[serde(rename = "{{ originals[f] }}")]
    {%- elif f != originals[f] and nullable and not type is starting_with("Option") %}
    #[serde(rename = "{{ originals[f] }}", deserialize_with = "nullable_{{ name|lower }}_{{ f }}")]
    {%- elif nullable and not type is starting_with("Option") %}
    #[serde(deserialize_with = "nullable_{{ name|lower }}_{{ f }}")]
    {%- endif %}
    pub {{ f }}: {{ type }},
    {%- endfor %}
}

{%- for f, type in fields %}
{%- if nullable and not type is starting_with("Option") %}
deser!(nullable_{{ name|lower }}_{{ f }}, {{ type }}, {{ defaults[f] }});
{%- endif %}
{%- endfor %}

impl Default for {{ name }} {
    fn default() -> {{ name }} {
        {{ name }} {
            {%- for f, value in defaults %}
            {{ f }}: {{ value }},
            {%- endfor %}
        }
    }
}
"#;

pub const ENUM_TERA: &str = "enum.tera";
pub const ENUM_TEMPLATE: &str = r#"
{%- if doc %}
/// {{ doc }}
{%- endif %}
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Deserialize, Serialize)]
pub enum {{ name }} {
    {%- for s in symbols %}
    {%- if s != originals[s] %}
    #[serde(rename = "{{ originals[s] }}")]
    {%- endif %}
    {{ s }},
    {%- endfor %}
}
"#;

pub const FIXED_TERA: &str = "fixed.tera";
pub const FIXED_TEMPLATE: &str = "
pub type {{ name }} = [u8; {{ size }}];
";

lazy_static! {
    static ref RESERVED: HashSet<String> = {
        let s: HashSet<_> = vec![
            "as", "async", "await", "break", "const", "continue", "crate", "dyn", "else", "enum",
            "extern", "false", "fn", "for", "if", "impl", "in", "let", "loop", "match", "mod",
            "move", "mut", "pub", "ref", "return", "Self", "self", "static", "struct", "super",
            "trait", "true", "try", "type", "unsafe", "use", "where", "while", "abstract",
            "alignof", "become", "box", "do", "final", "macro", "offsetof", "override", "priv",
            "proc", "pure", "sizeof", "typeof", "unsized", "virtual", "yields",
        ]
        .iter()
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

/// Fix for converting error-chain to failure.
/// see  https://github.com/rust-lang-nursery/failure/issues/109
trait ResultExt<T, E> {
    fn sync(self) -> Result<T, SyncFailure<E>>
    where
        Self: Sized,
        E: ::std::error::Error + Send + 'static;
}

/// Fix for converting error-chain to failure.
/// see  https://github.com/rust-lang-nursery/failure/issues/109
impl<T, E> ResultExt<T, E> for Result<T, E> {
    fn sync(self) -> Result<T, SyncFailure<E>>
    where
        Self: Sized,
        E: ::std::error::Error + Send + 'static,
    {
        self.map_err(SyncFailure::new)
    }
}

/// A helper struct for nested schema generation.
/// Used to store inner schema String type so that outter schema String type can be created.
#[derive(Debug)]
pub struct GenState<'a>(HashMap<ByAddress<&'a Schema>, String>);

impl<'a> GenState<'a> {
    pub fn new() -> GenState<'a> {
        GenState(HashMap::new())
    }

    /// Stores the String type of a given schema.
    pub fn put_type<'b: 'a>(&mut self, schema: &'b Schema, t: String) {
        self.0.insert(ByAddress(schema), t);
    }

    /// Retrieves the String type of a given schema.
    pub fn get_type(&self, schema: &'a Schema) -> Option<&String> {
        self.0.get(&ByAddress(schema))
    }
}

/// The main, stateless, component for templating. Current implementation uses Tera.
/// Its responsability is to generate String representing Rust code/types for a given Avro schema.
pub struct Templater {
    tera: Tera,
    pub precision: usize,
    pub nullable: bool,
}

impl Templater {
    /// Creates a new `Templater.`
    pub fn new() -> Result<Templater, Error> {
        let mut tera = Tera::new("/dev/null/*").sync()?;
        tera.add_raw_template(SERDE_TERA, SERDE_TEMPLATE).sync()?;
        tera.add_raw_template(RECORD_TERA, RECORD_TEMPLATE).sync()?;
        tera.add_raw_template(ENUM_TERA, ENUM_TEMPLATE).sync()?;
        tera.add_raw_template(FIXED_TERA, FIXED_TEMPLATE).sync()?;
        Ok(Templater {
            tera,
            precision: 3,
            nullable: false,
        })
    }

    /// Generates `use serde` statement
    pub fn str_serde(&self) -> Result<String, Error> {
        let mut ctx = Context::new();
        if self.nullable {
            ctx.insert("nullable", &true);
        }
        Ok(self.tera.render(SERDE_TERA, &ctx).sync()?)
    }

    /// Generates a Rust type based on a Schema::Fixed schema.
    pub fn str_fixed(&self, schema: &Schema) -> Result<String, Error> {
        if let Schema::Fixed {
            name: Name { name, .. },
            size,
        } = schema
        {
            let mut ctx = Context::new();
            ctx.insert("name", &sanitize(name.to_camel_case()));
            ctx.insert("size", size);
            Ok(self.tera.render(FIXED_TERA, &ctx).sync()?)
        } else {
            err!("Requires Schema::Fixed, found {:?}", schema)?
        }
    }

    /// Generates a Rust enum based on a Schema::Enum schema
    pub fn str_enum(&self, schema: &Schema) -> Result<String, Error> {
        if let Schema::Enum {
            name: Name { name, .. },
            symbols,
            doc,
            ..
        } = schema
        {
            if symbols.len() == 0 {
                err!("No symbol for emum: {:?}", name)?
            }
            let mut ctx = Context::new();
            ctx.insert("name", &sanitize(name.to_camel_case()));
            let doc = if let Some(d) = doc { d } else { "" };
            ctx.insert("doc", doc);
            let o: HashMap<_, _> = symbols
                .iter()
                .map(|s| (sanitize(s.to_camel_case()), s))
                .collect();
            let s: Vec<_> = symbols
                .iter()
                .map(|s| sanitize(s.to_camel_case()))
                .collect();
            ctx.insert("originals", &o);
            ctx.insert("symbols", &s);
            Ok(self.tera.render(ENUM_TERA, &ctx).sync()?)
        } else {
            err!("Requires Schema::Enum, found {:?}", schema)?
        }
    }

    /// Generates a Rust struct based on a Schema::Record schema.
    /// Makes use of a `GenState` for nested schemas (i.e. Array/Map/Union).
    pub fn str_record(&self, schema: &Schema, gen_state: &GenState) -> Result<String, Error> {
        if let Schema::Record {
            name: Name { name, .. },
            fields,
            doc,
            ..
        } = schema
        {
            let mut ctx = Context::new();
            ctx.insert("name", &name.to_camel_case());
            let doc = if let Some(d) = doc { d } else { "" };
            ctx.insert("doc", doc);

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
                        let default = match default {
                            Some(Value::Bool(b)) => b.to_string(),
                            None => bool::default().to_string(),
                            _ => err!("Invalid default: {:?}", default)?,
                        };
                        f.insert(name_std.clone(), "bool".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    Schema::Int => {
                        let default = match default {
                            Some(Value::Number(n)) if n.is_i64() => {
                                (n.as_i64().unwrap() as i32).to_string()
                            }
                            None => i32::default().to_string(),
                            _ => err!("Invalid default: {:?}", default)?,
                        };
                        f.insert(name_std.clone(), "i32".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    Schema::Long => {
                        let default = match default {
                            Some(Value::Number(n)) if n.is_i64() => n.to_string(),
                            None => i64::default().to_string(),
                            _ => err!("Invalid default: {:?}", default)?,
                        };
                        f.insert(name_std.clone(), "i64".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    Schema::Float => {
                        let default = match default {
                            Some(Value::Number(n)) if n.is_f64() => {
                                let n = n.as_f64().unwrap() as f32;
                                if n == n.ceil() {
                                    format!("{:.1}", n)
                                } else {
                                    format!("{:.*}", self.precision, n)
                                }
                            }
                            None => format!("{:.1}", f32::default()),
                            _ => err!("Invalid default: {:?}", default)?,
                        };
                        f.insert(name_std.clone(), "f32".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    Schema::Double => {
                        let default = match default {
                            Some(Value::Number(n)) if n.is_f64() => {
                                let n = n.as_f64().unwrap();
                                if n == n.ceil() {
                                    format!("{:.1}", n)
                                } else {
                                    format!("{:.*}", self.precision, n)
                                }
                            }
                            None => format!("{:.1}", f64::default()),
                            _ => err!("Invalid default: {:?}", default)?,
                        };
                        f.insert(name_std.clone(), "f64".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    Schema::Bytes => {
                        let default = match default {
                            Some(Value::String(s)) => {
                                let bytes = s.clone().into_bytes();
                                format!("vec!{:?}", bytes)
                            }
                            None => "vec![]".to_string(),
                            _ => err!("Invalid default: {:?}", default)?,
                        };
                        f.insert(name_std.clone(), "Vec<u8>".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    Schema::String => {
                        let default = match default {
                            Some(Value::String(s)) => format!("\"{}\".to_owned()", s),
                            None => "String::default()".to_string(),
                            _ => err!("Invalid default: {:?}", default)?,
                        };
                        f.insert(name_std.clone(), "String".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    Schema::Fixed {
                        name: Name { name: f_name, .. },
                        size,
                    } => {
                        let f_name = sanitize(f_name.to_camel_case());
                        let default = match default {
                            Some(Value::String(s)) => {
                                let bytes: Vec<u8> = s.clone().into_bytes();
                                if bytes.len() != *size {
                                    err!("Invalid default: {:?}", bytes)?
                                }
                                format!("{:?}", bytes)
                            }
                            None => format!("{}::default()", f_name),
                            _ => err!("Invalid default: {:?}", default)?,
                        };
                        f.insert(name_std.clone(), f_name.clone());
                        d.insert(name_std.clone(), default);
                    }

                    Schema::Array(inner) => match &**inner {
                        Schema::Null => err!("Invalid use of Schema::Null")?,
                        _ => {
                            let type_str = array_type(&**inner, &*gen_state)?;
                            let default_str = self.array_default(&**inner, default)?;
                            f.insert(name_std.clone(), type_str);
                            d.insert(name_std.clone(), default_str);
                        }
                    },

                    Schema::Map(inner) => match &**inner {
                        Schema::Null => err!("Invalid use of Schema::Null")?,
                        _ => {
                            let type_str = map_type(&**inner, &*gen_state)?;
                            let default_str = self.map_default(&**inner, default)?;
                            f.insert(name_std.clone(), type_str);
                            d.insert(name_std.clone(), default_str);
                        }
                    },

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
                        let valids: HashSet<_> = symbols
                            .iter()
                            .map(|s| sanitize(s.to_camel_case()))
                            .collect();
                        let default = match default {
                            Some(Value::String(s)) => {
                                let s = sanitize(s.to_camel_case());
                                if valids.contains(&s) {
                                    format!("{}::{}", e_name, sanitize(s.to_camel_case()))
                                } else {
                                    err!("Invalid default: {:?}", default)?
                                }
                            }
                            None if !symbols.is_empty() => {
                                format!("{}::{}", e_name, sanitize(symbols[0].to_camel_case()))
                            }
                            _ => err!("Invalid default: {:?}", default)?,
                        };
                        f.insert(name_std.clone(), e_name);
                        d.insert(name_std.clone(), default);
                    }

                    Schema::Union(union) => {
                        if let [Schema::Null, inner] = union.variants() {
                            let type_str = option_type(inner, &*gen_state)?;
                            let default_str = self.option_default(inner, default)?;
                            f.insert(name_std.clone(), type_str);
                            d.insert(name_std.clone(), default_str);
                        } else {
                            err!("Unsupported Schema:::Union {:?}", union.variants())?
                        }
                    }

                    Schema::Null => err!("Invalid use of Schema::Null")?,
                };
            }

            ctx.insert("fields", &f);
            ctx.insert("originals", &o);
            ctx.insert("defaults", &d);
            if self.nullable {
                ctx.insert("nullable", &true);
            }
            Ok(self.tera.render(RECORD_TERA, &ctx).sync()?)
        } else {
            err!("Requires Schema::Record, found {:?}", schema)?
        }
    }

    /// Helper to coerce defaults from an Avro schema to Rust types.
    fn coerce_default_fn<'a>(
        &'a self,
        schema: &'a Schema,
    ) -> Box<Fn(&'a Value) -> Result<String, TemplateError> + 'a> {
        match schema {
            Schema::Null => Box::new(|_| err!("Invalid use of Schema::Null")?),

            Schema::Boolean => Box::new(|v: &Value| match v {
                Value::Bool(b) => Ok(b.to_string()),
                _ => err!("Invalid defaults: {:?}", v),
            }),

            Schema::Int => Box::new(|v: &Value| match v {
                Value::Number(n) if n.is_i64() => Ok((n.as_i64().unwrap() as i32).to_string()),
                _ => err!("Invalid defaults: {:?}", v),
            }),

            Schema::Long => Box::new(|v: &Value| match v {
                Value::Number(n) if n.is_i64() => Ok(n.to_string()),
                _ => err!("Invalid defaults: {:?}", v),
            }),

            Schema::Float => Box::new(move |v: &Value| match v {
                Value::Number(n) if n.is_f64() => {
                    let n = n.as_f64().unwrap() as f32;
                    if n == n.ceil() {
                        Ok(format!("{:.1}", n))
                    } else {
                        Ok(format!("{:.*}", self.precision, n))
                    }
                }
                _ => err!("Invalid defaults: {:?}", v),
            }),

            Schema::Double => Box::new(move |v: &Value| match v {
                Value::Number(n) if n.is_f64() => {
                    let n = n.as_f64().unwrap();
                    if n == n.ceil() {
                        Ok(format!("{:.1}", n))
                    } else {
                        Ok(format!("{:.*}", self.precision, n))
                    }
                }
                _ => err!("Invalid defaults: {:?}", v),
            }),

            Schema::Bytes => Box::new(|v: &Value| match v {
                Value::String(s) => {
                    let bytes = s.clone().into_bytes();
                    Ok(format!("vec!{:?}", bytes))
                }
                _ => err!("Invalid defaults: {:?}", v),
            }),

            Schema::String => Box::new(|v: &Value| match v {
                Value::String(s) => Ok(s.clone()),
                _ => err!("Invalid defaults: {:?}", v),
            }),

            Schema::Fixed { size, .. } => Box::new(move |v: &Value| match v {
                Value::String(s) => {
                    let bytes: Vec<u8> = s.clone().into_bytes();
                    if bytes.len() == *size {
                        Ok(format!("{:?}", bytes))
                    } else {
                        err!("Invalid defaults: {:?}", bytes)
                    }
                }
                _ => err!("Invalid defaults: {:?}", v),
            }),

            Schema::Array(s) => {
                Box::new(move |v: &Value| Ok(self.array_default(s, &Some(v.clone()))?))
            }

            Schema::Map(s) => Box::new(move |v: &Value| Ok(self.map_default(s, &Some(v.clone()))?)),

            Schema::Enum {
                name: Name { name: e_name, .. },
                symbols,
                ..
            } => {
                let e_name = sanitize(e_name.to_camel_case());
                let valids: HashSet<_> = symbols
                    .iter()
                    .map(|s| sanitize(s.to_camel_case()))
                    .collect();
                Box::new(move |v: &Value| match v {
                    Value::String(s) => {
                        let s = sanitize(s.to_camel_case());
                        if valids.contains(&s) {
                            Ok(format!("{}::{}", e_name, s))
                        } else {
                            err!("Invalid defaults: {:?}", s)
                        }
                    }
                    _ => err!("Invalid defaults: {:?}", v),
                })
            }

            Schema::Union(..) => Box::new(|_| Ok("None".to_string())),

            Schema::Record {
                name: Name { name, .. },
                ..
            } => Box::new(move |_| Ok(format!("{}::default()", sanitize(name.to_camel_case())))),
        }
    }

    /// Generates Rust default values for the inner schema of an Avro array.
    fn array_default(
        &self,
        inner: &Schema,
        default: &Option<Value>,
    ) -> Result<String, TemplateError> {
        let default_str = if let Some(Value::Array(vals)) = default {
            let vals = vals
                .iter()
                .map(&*self.coerce_default_fn(inner))
                .collect::<Result<Vec<String>, TemplateError>>()?
                .as_slice()
                .join(", ");
            format!("vec![{}]", vals)
        } else {
            "vec![]".to_string()
        };
        Ok(default_str)
    }

    /// Generates Rust default values for the inner schema of an Avro map.
    fn map_default(
        &self,
        inner: &Schema,
        default: &Option<Value>,
    ) -> Result<String, TemplateError> {
        let default_str = if let Some(Value::Object(o)) = default {
            let vals = o
                .iter()
                .map(|(k, v)| {
                    Ok(format!(
                        "m.insert({}, {});",
                        format!("\"{}\".to_owned()", k),
                        self.coerce_default_fn(inner)(v)?
                    ))
                })
                .collect::<Result<Vec<String>, TemplateError>>()?
                .as_slice()
                .join(" ");
            format!(
                "{{ let mut m = ::std::collections::HashMap::new(); {} m }}",
                vals
            )
        } else {
            "::std::collections::HashMap::new()".to_string()
        };
        Ok(default_str)
    }

    /// Generates Rust default values for the inner schema of an Avro union.
    fn option_default(&self, _: &Schema, default: &Option<Value>) -> Result<String, Error> {
        let default_str = match default {
            None => "None".to_string(),
            Some(Value::Null) => "None".to_string(),
            _ => err!("Invalid default: {:?}", default)?,
        };
        Ok(default_str)
    }
}

/// Generates the Rust type of the inner schema of an Avro array.
pub fn array_type(inner: &Schema, gen_state: &GenState) -> Result<String, Error> {
    let type_str = match inner {
        Schema::Boolean => "Vec<bool>".to_string(),
        Schema::Int => "Vec<i32>".to_string(),
        Schema::Long => "Vec<i64>".to_string(),
        Schema::Float => "Vec<f32>".to_string(),
        Schema::Double => "Vec<f64>".to_string(),
        Schema::Bytes => "Vec<Vec<u8>>".to_string(),
        Schema::String => "Vec<String>".to_string(),

        Schema::Fixed {
            name: Name { name: f_name, .. },
            ..
        } => {
            let f_name = sanitize(f_name.to_camel_case());
            format!("Vec<{}>", f_name)
        }

        Schema::Array(..) | Schema::Map(..) | Schema::Union(..) => {
            let nested_type = gen_state.get_type(inner).ok_or_else(|| {
                TemplateError(format!(
                    "Didn't find schema {:?} in state {:?}",
                    inner, &gen_state
                ))
            })?;
            format!("Vec<{}>", nested_type)
        }

        Schema::Record {
            name: Name { name, .. },
            ..
        }
        | Schema::Enum {
            name: Name { name, .. },
            ..
        } => format!("Vec<{}>", &sanitize(name.to_camel_case())),

        Schema::Null => err!("Invalid use of Schema::Null")?,
    };
    Ok(type_str)
}

fn map_of(t: &str) -> String {
    format!("::std::collections::HashMap<String, {}>", t)
}

/// Generates the Rust type of the inner schema of an Avro map.
pub fn map_type(inner: &Schema, gen_state: &GenState) -> Result<String, Error> {
    let type_str = match inner {
        Schema::Boolean => map_of("bool"),
        Schema::Int => map_of("i32"),
        Schema::Long => map_of("i64"),
        Schema::Float => map_of("f32"),
        Schema::Double => map_of("f64"),
        Schema::Bytes => map_of("Vec<u8>"),
        Schema::String => map_of("String"),

        Schema::Fixed {
            name: Name { name: f_name, .. },
            ..
        } => {
            let f_name = sanitize(f_name.to_camel_case());
            map_of(&f_name)
        }

        Schema::Array(..) | Schema::Map(..) | Schema::Union(..) => {
            let nested_type = gen_state.get_type(inner).ok_or_else(|| {
                TemplateError(format!(
                    "Didn't find schema {:?} in state {:?}",
                    inner, &gen_state
                ))
            })?;
            map_of(nested_type)
        }

        Schema::Record {
            name: Name { name, .. },
            ..
        }
        | Schema::Enum {
            name: Name { name, .. },
            ..
        } => map_of(&sanitize(name.to_camel_case())),

        Schema::Null => err!("Invalid use of Schema::Null")?,
    };
    Ok(type_str)
}

/// Generates the Rust type of the inner schema of an Avro union.
pub fn option_type(inner: &Schema, gen_state: &GenState) -> Result<String, Error> {
    let type_str = match inner {
        Schema::Boolean => "Option<bool>".to_string(),
        Schema::Int => "Option<i32>".to_string(),
        Schema::Long => "Option<i64>".to_string(),
        Schema::Float => "Option<f32>".to_string(),
        Schema::Double => "Option<f64>".to_string(),
        Schema::Bytes => "Option<Vec<u8>>".to_string(),
        Schema::String => "Option<String>".to_string(),

        Schema::Fixed {
            name: Name { name: f_name, .. },
            ..
        } => {
            let f_name = sanitize(f_name.to_camel_case());
            format!("Option<{}>", f_name)
        }

        Schema::Array(..) | Schema::Map(..) | Schema::Union(..) => {
            let nested_type = gen_state.get_type(inner).ok_or_else(|| {
                TemplateError(format!(
                    "Didn't find schema {:?} in state {:?}",
                    inner, &gen_state
                ))
            })?;
            format!("Option<{}>", nested_type)
        }

        Schema::Record {
            name: Name { name, .. },
            ..
        }
        | Schema::Enum {
            name: Name { name, .. },
            ..
        } => format!("Option<{}>", &sanitize(name.to_camel_case())),

        Schema::Null => err!("Invalid use of Schema::Null")?,
    };
    Ok(type_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gen_record() {
        let raw_schema = r#"
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "as", "type": "string"},
    {"name": "favoriteNumber",  "type": "int", "default": 7},
    {"name": "likes_pizza", "type": "boolean", "default": false},
    {"name": "b", "type": "bytes", "default": "\u00FF"},
    {"name": "a-bool", "type": {"type": "array", "items": "boolean"}, "default": [true, false]},
    {"name": "a-i32", "type": {"type": "array", "items": "int"}, "default": [12, -1]},
    {"name": "m-f64", "type": {"type": "map", "values": "double"}}
  ]
}
"#;

        let expected = r#"
#[serde(default)]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct User {
    #[serde(rename = "a-bool")]
    pub a_bool: Vec<bool>,
    #[serde(rename = "a-i32")]
    pub a_i32: Vec<i32>,
    #[serde(rename = "as")]
    pub as_: String,
    pub b: Vec<u8>,
    #[serde(rename = "favoriteNumber")]
    pub favorite_number: i32,
    pub likes_pizza: bool,
    #[serde(rename = "m-f64")]
    pub m_f64: ::std::collections::HashMap<String, f64>,
}

impl Default for User {
    fn default() -> User {
        User {
            a_bool: vec![true, false],
            a_i32: vec![12, -1],
            as_: String::default(),
            b: vec![195, 191],
            favorite_number: 7,
            likes_pizza: false,
            m_f64: ::std::collections::HashMap::new(),
        }
    }
}
"#;

        let templater = Templater::new().unwrap();
        let schema = Schema::parse_str(&raw_schema).unwrap();
        let gs = GenState::new();
        let res = templater.str_record(&schema, &gs).unwrap();

        assert_eq!(expected, res);
    }

    #[test]
    fn gen_default_map() {
        let raw_schema = r#"
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "m-f64",
     "type": {"type": "map", "values": "double"},
     "default": {"a": 12.0, "b": 42.1}}
  ]
}
"#;

        let expected = r#"
#[serde(default)]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct User {
    #[serde(rename = "m-f64")]
    pub m_f64: ::std::collections::HashMap<String, f64>,
}

impl Default for User {
    fn default() -> User {
        User {
            m_f64: { let mut m = ::std::collections::HashMap::new(); m.insert("a".to_owned(), 12.0); m.insert("b".to_owned(), 42.100); m },
        }
    }
}
"#;

        let templater = Templater::new().unwrap();
        let schema = Schema::parse_str(&raw_schema).unwrap();
        let gs = GenState::new();
        let res = templater.str_record(&schema, &gs).unwrap();

        assert_eq!(expected, res);
    }

    #[test]
    fn gen_enum() {
        let raw_schema = r#"
{
  "type": "enum",
  "name": "Colors",
  "doc": "Roses are red violets are blue.",
  "symbols": ["RED", "GREEN", "BLUE"]
}
"#;

        let templater = Templater::new().unwrap();
        let schema = Schema::parse_str(&raw_schema).unwrap();
        let res = templater.str_enum(&schema).unwrap();

        let expected = r#"
/// Roses are red violets are blue.
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Deserialize, Serialize)]
pub enum Colors {
    #[serde(rename = "RED")]
    Red,
    #[serde(rename = "GREEN")]
    Green,
    #[serde(rename = "BLUE")]
    Blue,
}
"#;

        assert_eq!(expected, res);
    }

    #[test]
    fn gen_fixed() {
        let raw_schema = r#"
{
  "type": "fixed",
  "name": "Md5",
  "size": 16
}
"#;

        let templater = Templater::new().unwrap();
        let schema = Schema::parse_str(&raw_schema).unwrap();
        let res = templater.str_fixed(&schema).unwrap();

        let expected = "
pub type Md5 = [u8; 16];
";

        assert_eq!(expected, res);
    }
}
