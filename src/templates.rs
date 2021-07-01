//! Logic for templating Rust types and default values from Avro schema.

use std::collections::{HashMap, HashSet};

use avro_rs::schema::{Name, RecordField, UnionSchema};
use avro_rs::Schema;
use heck::{CamelCase, SnakeCase};
use lazy_static::lazy_static;
use serde_json::Value;
use tera::{Context, Tera};
use uuid::Uuid;

use crate::error::{Error, Result};

pub const DESER_NULLABLE: &str = r#"
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
"#;

pub const RECORD_TERA: &str = "record.tera";
pub const RECORD_TEMPLATE: &str = r#"
{%- if doc %}
/// {{ doc }}
{%- endif %}
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct {{ name }} {
    {%- for f in fields %}
    {%- set type = types[f] %}
    {%- if f != originals[f] and not nullable and not f is starting_with("r#") %}
    #[serde(rename = "{{ originals[f] }}")]
    {%- elif f != originals[f] and nullable and not type is starting_with("Option") %}
    #[serde(rename = "{{ originals[f] }}", deserialize_with = "nullable_{{ name|lower }}_{{ f }}")]
    {%- elif nullable and not type is starting_with("Option") %}
    #[serde(deserialize_with = "nullable_{{ name|lower }}_{{ f }}")]
    {%- endif %}
    pub {{ f }}: {{ type }},
    {%- endfor %}
}

{%- for f in fields %}
{%- set type = types[f] %}
{%- if nullable and not type is starting_with("Option") %}
deser!(nullable_{{ name|lower }}_{{ f }}, {{ type }}, {{ defaults[f] }});
{%- endif %}
{%- endfor %}

impl Default for {{ name }} {
    fn default() -> {{ name }} {
        {{ name }} {
            {%- for f in fields %}
            {{ f }}: {{ defaults[f] }},
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
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, serde::Deserialize, serde::Serialize)]
pub enum {{ name }} {
    {%- for s in symbols %}
    {%- if s != originals[s] %}
    #[serde(rename = "{{ originals[s] }}")]
    {%- endif %}
    {{ s }},
    {%- endfor %}
}
"#;

pub const UNION_TERA: &str = "union.tera";
pub const UNION_TEMPLATE: &str = r#"
/// Auto-generated type for unnamed Avro union variants.
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize {%- if use_variant_access %}, variant_access_derive::VariantAccess {%- endif %})]
pub enum {{ name }} {
    {%- for s in symbols %}
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
            "as", "break", "const", "continue", "crate", "dyn", "else", "enum", "extern", "false",
            "fn", "for", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub",
            "ref", "return", "Self", "self", "static", "struct", "super", "trait", "true", "type",
            "unsafe", "use", "where", "while", "abstract", "async", "await", "become", "box", "do",
            "final", "macro", "override", "priv", "try", "typeof", "unsized", "virtual", "yield",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        s
    };
}

fn sanitize(mut s: String) -> String {
    if RESERVED.contains(&s) {
        s.insert_str(0, "r#");
        s
    } else {
        s
    }
}

macro_rules! err(
    ($($arg:tt)*) => (Err(Error::Template(format!($($arg)*))))
);

/// A helper struct for nested schema generation.
///
/// Used to store inner schema String type so that outter schema String type can be created.
#[derive(Debug)]
pub struct GenState(HashMap<String, String>);

impl GenState {
    pub fn new() -> GenState {
        GenState(HashMap::new())
    }

    /// Stores the String type of a given schema.
    pub fn put_type(&mut self, schema: &Schema, t: String) {
        let k = serde_json::to_string(schema).expect("Unexpected invalid schema");
        self.0.insert(k, t);
    }

    /// Retrieves the String type of a given schema.
    pub fn get_type(&self, schema: &Schema) -> Option<&String> {
        let k = serde_json::to_string(schema).expect("Unexpected invalid schema");
        self.0.get(&k)
    }
}

/// The main, stateless, component for templating.
///
/// Current implementation uses Tera.  Its responsability is to generate String
/// representing Rust code/types for a given Avro schema.
pub struct Templater {
    tera: Tera,
    pub precision: usize,
    pub nullable: bool,
    pub use_variant_access: bool,
}

impl Templater {
    /// Creates a new `Templater.`
    pub fn new() -> Result<Templater> {
        let mut tera = Tera::new("/dev/null/*")?;

        tera.add_raw_template(RECORD_TERA, RECORD_TEMPLATE)?;
        tera.add_raw_template(ENUM_TERA, ENUM_TEMPLATE)?;
        tera.add_raw_template(FIXED_TERA, FIXED_TEMPLATE)?;
        tera.add_raw_template(UNION_TERA, UNION_TEMPLATE)?;

        Ok(Templater {
            tera,
            precision: 3,
            nullable: false,
            use_variant_access: false,
        })
    }

    /// Generates a Rust type based on a Schema::Fixed schema.
    pub fn str_fixed(&self, schema: &Schema) -> Result<String> {
        if let Schema::Fixed {
            name: Name { name, .. },
            size,
        } = schema
        {
            let mut ctx = Context::new();
            ctx.insert("name", &sanitize(name.to_camel_case()));
            ctx.insert("size", size);
            Ok(self.tera.render(FIXED_TERA, &ctx)?)
        } else {
            err!("Requires Schema::Fixed, found {:?}", schema)?
        }
    }

    /// Generates a Rust enum based on a Schema::Enum schema
    pub fn str_enum(&self, schema: &Schema) -> Result<String> {
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
            Ok(self.tera.render(ENUM_TERA, &ctx)?)
        } else {
            err!("Requires Schema::Enum, found {:?}", schema)?
        }
    }

    /// Generates a Rust struct based on a Schema::Record schema.
    /// Makes use of a `GenState` for nested schemas (i.e. Array/Map/Union).
    pub fn str_record(&self, schema: &Schema, gen_state: &GenState) -> Result<String> {
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

            let mut f = Vec::new(); // field names;
            let mut t = HashMap::new(); // field name -> field type
            let mut o = HashMap::new(); // field name -> original name
            let mut d = HashMap::new(); // field name -> default value

            let by_pos = fields
                .iter()
                .map(|f| (f.position, f))
                .collect::<HashMap<_, _>>();
            let mut i = 0;
            while let Some(RecordField {
                schema,
                name,
                default,
                ..
            }) = by_pos.get(&i)
            {
                let name_std = sanitize(name.to_snake_case());
                o.insert(name_std.clone(), name);

                match schema {
                    Schema::Boolean => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "bool".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    Schema::Int | Schema::Date | Schema::TimeMillis => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "i32".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    Schema::Long
                    | Schema::TimeMicros
                    | Schema::TimestampMillis
                    | Schema::TimestampMicros => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "i64".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    Schema::Float => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "f32".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    Schema::Double => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "f64".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    Schema::Bytes => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "Vec<u8>".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    Schema::String => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "String".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    Schema::Uuid => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "uuid::Uuid".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    Schema::Duration => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "avro_rs::Duration".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    Schema::Decimal { .. } => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "avro_rs::Decimal".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    Schema::Fixed {
                        name: Name { name: f_name, .. },
                        ..
                    } => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        let f_name = sanitize(f_name.to_camel_case());
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), f_name.clone());
                        d.insert(name_std.clone(), default);
                    }

                    Schema::Array(inner) => match inner.as_ref() {
                        Schema::Null => err!("Invalid use of Schema::Null")?,
                        _ => {
                            let default = self.parse_default(schema, gen_state, default)?;
                            let type_str = array_type(inner, gen_state)?;
                            f.push(name_std.clone());
                            t.insert(name_std.clone(), type_str);
                            d.insert(name_std.clone(), default);
                        }
                    },

                    Schema::Map(inner) => match inner.as_ref() {
                        Schema::Null => err!("Invalid use of Schema::Null")?,
                        _ => {
                            let default = self.parse_default(schema, gen_state, default)?;
                            let type_str = map_type(inner, gen_state)?;
                            f.push(name_std.clone());
                            t.insert(name_std.clone(), type_str);
                            d.insert(name_std.clone(), default);
                        }
                    },

                    Schema::Record {
                        name: Name { name: r_name, .. },
                        ..
                    } => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        let r_name = sanitize(r_name.to_camel_case());
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), r_name.clone());
                        d.insert(name_std.clone(), default);
                    }

                    Schema::Enum {
                        name: Name { name: e_name, .. },
                        ..
                    } => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        let e_name = sanitize(e_name.to_camel_case());
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), e_name);
                        d.insert(name_std.clone(), default);
                    }

                    Schema::Union(union) => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        let type_str = union_type(union, gen_state, true)?;
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), type_str);
                        d.insert(name_std.clone(), default);
                    }

                    Schema::Null => err!("Invalid use of Schema::Null")?,
                };

                i += 1;
            }

            ctx.insert("fields", &f);
            ctx.insert("types", &t);
            ctx.insert("originals", &o);
            ctx.insert("defaults", &d);
            if self.nullable {
                ctx.insert("nullable", &true);
            }

            Ok(self.tera.render(RECORD_TERA, &ctx)?)
        } else {
            err!("Requires Schema::Record, found {:?}", schema)?
        }
    }

    pub fn str_union_enum(&self, schema: &Schema, gen_state: &GenState) -> Result<String> {
        if let Schema::Union(union) = schema {
            let variants = union.variants();

            if variants.is_empty() {
                err!("Invalid empty Schema::Union")?
            } else if variants.len() == 1 {
                err!("Invalid Schema::Union of a single element")?
            } else if union.is_nullable() && variants.len() == 2 {
                err!("Attempt to generate a union enum for an optional")?
            }

            let schemas = if variants[0] == Schema::Null {
                &variants[1..]
            } else {
                variants
            };

            let e_name = union_type(union, gen_state, false)?;

            let mut symbols = vec![];
            for sc in schemas {
                let symbol_str = match sc {
                    Schema::Boolean => "Boolean(bool)".into(),
                    Schema::Int => "Int(i32)".into(),
                    Schema::Long => "Long(i64)".into(),
                    Schema::Float => "Float(f32)".into(),
                    Schema::Double => "Double(f64)".into(),
                    Schema::Bytes => "Bytes(Vec<u8>)".into(),
                    Schema::String => "String(String)".into(),
                    Schema::Array(inner) => {
                        format!(
                            "Array{}({})",
                            union_enum_variant(inner.as_ref(), gen_state)?,
                            array_type(inner.as_ref(), gen_state)?
                        )
                    }
                    Schema::Map(inner) => format!(
                        "Map{}({})",
                        union_enum_variant(inner.as_ref(), gen_state)?,
                        map_type(sc, gen_state)?
                    ),
                    Schema::Union(union) => {
                        format!("{u}({u})", u = union_type(union, gen_state, false)?)
                    }
                    Schema::Record {
                        name: Name { name, .. },
                        ..
                    } => {
                        format!("{rec}({rec})", rec = name.to_camel_case())
                    }
                    Schema::Enum {
                        name: Name { name, .. },
                        ..
                    } => {
                        format!("{e}({e})", e = sanitize(name.to_camel_case()))
                    }
                    Schema::Fixed {
                        name: Name { name, .. },
                        ..
                    } => {
                        format!("{f}({f})", f = sanitize(name.to_camel_case()))
                    }
                    Schema::Decimal { .. } => "Decimal(avro_rs::Decimal)".into(),
                    Schema::Uuid => "Uuid(uuid::Uuid)".into(),
                    Schema::Date => "Date(i32)".into(),
                    Schema::TimeMillis => "TimeMillis(i32)".into(),
                    Schema::TimeMicros => "TimeMicros(i64)".into(),
                    Schema::TimestampMillis => "TimestampMillis(i64)".into(),
                    Schema::TimestampMicros => "TimestampMicros(i64)".into(),
                    Schema::Duration => "Duration(avro_rs::Duration)".into(),
                    Schema::Null => err!(
                        "Invalid Schema::Null not in first position on an UnionSchema variants"
                    )?,
                };
                symbols.push(symbol_str);
            }

            let mut ctx = Context::new();
            ctx.insert("name", &e_name);
            ctx.insert("symbols", &symbols);
            ctx.insert("use_variant_access", &self.use_variant_access);

            Ok(self.tera.render(UNION_TERA, &ctx)?)
        } else {
            err!("Requires Schema::Union, found {:?}", schema)?
        }
    }

    fn parse_default(
        &self,
        schema: &Schema,
        gen_state: &GenState,
        default: &Option<serde_json::Value>,
    ) -> Result<String> {
        let default_str = match schema {
            Schema::Boolean => match default {
                Some(Value::Bool(b)) => b.to_string(),
                None => bool::default().to_string(),
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::Int | Schema::Date | Schema::TimeMillis => match default {
                Some(Value::Number(n)) if n.is_i64() => (n.as_i64().unwrap() as i32).to_string(),
                None => i32::default().to_string(),
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::Long
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros => match default {
                Some(Value::Number(n)) if n.is_i64() => n.to_string(),
                None => i64::default().to_string(),
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::Float => match default {
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
            },

            Schema::Double => match default {
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
            },

            Schema::Bytes => match default {
                Some(Value::String(s)) => {
                    let bytes = s.clone().into_bytes();
                    format!("vec!{:?}", bytes)
                }
                None => "vec![]".to_string(),
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::String => match default {
                Some(Value::String(s)) => format!("\"{}\".to_owned()", s),
                None => "String::default()".to_string(),
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::Uuid => match default {
                Some(Value::String(s)) => Uuid::parse_str(&s)?.to_string(),
                None => Uuid::nil().to_string(),
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::Duration => match default {
                Some(Value::String(s)) => {
                    let bytes = s.clone().into_bytes();
                    if bytes.len() != 12 {
                        err!("Invalid default: {:?}", bytes)?
                    }
                    format!("{:?}", bytes)
                }
                None => String::from_utf8_lossy(&vec![0; 12]).to_string(),
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::Decimal { inner, .. } => match inner.as_ref() {
                Schema::Bytes => match default {
                    Some(Value::String(s)) => {
                        let bytes = s.clone().into_bytes();
                        format!("vec!{:?}", bytes)
                    }
                    None => "vec![]".to_string(),
                    _ => err!("Invalid default: {:?}", default)?,
                },
                Schema::Fixed {
                    name: Name { name: f_name, .. },
                    size,
                } => match default {
                    Some(Value::String(s)) => {
                        let bytes = s.clone().into_bytes();
                        if bytes.len() != *size {
                            err!("Invalid default: {:?}", bytes)?
                        }
                        format!("{:?}", bytes)
                    }
                    None => format!("{}::default()", f_name),
                    _ => err!("Invalid default: {:?}", default)?,
                },
                _ => err!("Invalid Decimal inner Schema: {:?}", inner)?,
            },

            Schema::Fixed {
                name: Name { name: f_name, .. },
                size,
            } => match default {
                Some(Value::String(s)) => {
                    let bytes = s.clone().into_bytes();
                    if bytes.len() != *size {
                        err!("Invalid default: {:?}", bytes)?
                    }
                    format!("{:?}", bytes)
                }
                None => format!("{}::default()", f_name),
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::Array(inner) => match inner.as_ref() {
                Schema::Null => err!("Invalid use of Schema::Null")?,
                _ => self.array_default(inner, gen_state, default)?,
            },

            Schema::Map(inner) => match inner.as_ref() {
                Schema::Null => err!("Invalid use of Schema::Null")?,
                _ => self.map_default(inner, gen_state, default)?,
            },

            Schema::Record { .. } => self.record_default(schema, gen_state, default)?,

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
                match default {
                    Some(Value::String(ref s)) => {
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
                }
            }

            Schema::Union(union) => self.union_default(union, gen_state, default)?,

            Schema::Null => err!("Invalid use of Schema::Null")?,
        };

        Ok(default_str)
    }

    /// Helper to coerce defaults from an Avro schema to Rust types.
    fn coerce_default_fn<'a>(
        &'a self,
        schema: &'a Schema,
        gen_state: &'a GenState,
    ) -> Box<dyn Fn(&'a Value) -> Result<String> + 'a> {
        match schema {
            Schema::Null => Box::new(|_| err!("Invalid use of Schema::Null")?),

            Schema::Boolean => Box::new(|v: &Value| match v {
                Value::Bool(b) => Ok(b.to_string()),
                _ => err!("Invalid defaults: {:?}", v),
            }),

            Schema::Int | Schema::Date | Schema::TimeMillis => Box::new(|v: &Value| match v {
                Value::Number(n) if n.is_i64() => Ok((n.as_i64().unwrap() as i32).to_string()),
                _ => err!("Invalid defaults: {:?}", v),
            }),

            Schema::Long
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros => Box::new(|v: &Value| match v {
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
                Value::String(s) => Ok(format!("\"{}\".to_owned()", s)),
                _ => err!("Invalid defaults: {:?}", v),
            }),

            Schema::Uuid => Box::new(|v: &Value| match v {
                Value::String(s) => Ok(Uuid::parse_str(s)?.to_string()),
                _ => err!("Invalid defaults: {:?}", v),
            }),

            Schema::Duration => Box::new(|v: &Value| match v {
                Value::String(s) => {
                    let bytes = s.clone().into_bytes();
                    if bytes.len() == 12 {
                        Ok(format!("{:?}", bytes))
                    } else {
                        err!("Invalid defaults: {:?}", bytes)
                    }
                }
                _ => err!("Invalid defaults: {:?}", v),
            }),

            Schema::Decimal { inner, .. } => Box::new(move |v: &Value| match inner.as_ref() {
                Schema::Bytes => match v {
                    Value::String(s) => {
                        let bytes = s.clone().into_bytes();
                        Ok(format!("vec!{:?}", bytes))
                    }
                    _ => err!("Invalid default: {:?}", v),
                },
                Schema::Fixed { size, .. } => match v {
                    Value::String(s) => {
                        let bytes = s.clone().into_bytes();
                        if bytes.len() != *size {
                            return err!("Invalid default: {:?}", bytes);
                        }
                        Ok(format!("{:?}", bytes))
                    }
                    _ => err!("Invalid default: {:?}", v),
                },
                _ => err!("Invalid Decimal inner Schema: {:?}", inner),
            }),

            Schema::Fixed { size, .. } => Box::new(move |v: &Value| match v {
                Value::String(s) => {
                    let bytes = s.clone().into_bytes();
                    if bytes.len() == *size {
                        Ok(format!("{:?}", bytes))
                    } else {
                        err!("Invalid defaults: {:?}", bytes)
                    }
                }
                _ => err!("Invalid defaults: {:?}", v),
            }),

            Schema::Array(s) => {
                Box::new(move |v: &Value| self.array_default(s, gen_state, &Some(v.clone())))
            }

            Schema::Map(s) => {
                Box::new(move |v: &Value| self.map_default(s, gen_state, &Some(v.clone())))
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

            Schema::Union(union) => {
                Box::new(move |v: &Value| self.union_default(union, gen_state, &Some(v.clone())))
            }

            Schema::Record { .. } => {
                Box::new(move |v: &Value| self.record_default(schema, gen_state, &Some(v.clone())))
            }
        }
    }

    /// Generates Rust default values for the inner schema of an Avro array.
    fn array_default(
        &self,
        inner: &Schema,
        gen_state: &GenState,
        default: &Option<Value>,
    ) -> Result<String> {
        let default_str = if let Some(Value::Array(vals)) = default {
            let vals = vals
                .iter()
                .map(self.coerce_default_fn(inner, gen_state))
                .collect::<Result<Vec<String>>>()?
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
        gen_state: &GenState,
        default: &Option<Value>,
    ) -> Result<String> {
        let default_str = if let Some(Value::Object(o)) = default {
            if o.is_empty() {
                "::std::collections::HashMap::new()".to_string()
            } else {
                let vals = o
                    .iter()
                    .map(|(k, v)| {
                        Ok(format!(
                            "m.insert({}, {});",
                            format!("\"{}\".to_owned()", k),
                            self.coerce_default_fn(inner, gen_state)(v)?
                        ))
                    })
                    .collect::<Result<Vec<String>>>()?
                    .as_slice()
                    .join(" ");
                format!(
                    "{{ let mut m = ::std::collections::HashMap::new(); {} m }}",
                    vals
                )
            }
        } else {
            "::std::collections::HashMap::new()".to_string()
        };
        Ok(default_str)
    }

    /// Generates Rust default values for an Avro record
    fn record_default(
        &self,
        inner: &Schema,
        gen_state: &GenState,
        default: &Option<Value>,
    ) -> Result<String> {
        match inner {
            Schema::Record {
                name: Name { name, .. },
                fields,
                lookup,
                ..
            } => {
                let default_str = if let Some(Value::Object(o)) = default {
                    if o.len() > 0 {
                        let vals = o
                            .iter()
                            .map(|(k, v)| {
                                let f = sanitize(k.to_snake_case());
                                let rf = fields
                                    .get(*lookup.get(k).expect("Missing lookup"))
                                    .expect("Missing record field");
                                let d = self.coerce_default_fn(&rf.schema, gen_state)(v)?;
                                Ok(format!("r.{} = {};", f, d))
                            })
                            .collect::<Result<Vec<String>>>()?
                            .as_slice()
                            .join(" ");
                        format!(
                            "{{ let mut r = {}::default(); {} r }}",
                            sanitize(name.to_camel_case()),
                            vals
                        )
                    } else {
                        format!("{}::default()", sanitize(name.to_camel_case()))
                    }
                } else {
                    format!("{}::default()", sanitize(name.to_camel_case()))
                };
                Ok(default_str)
            }
            _ => err!("Invalid record: {:?}", inner)?,
        }
    }

    /// Generates Rust default values for the inner schemas of an Avro union.
    fn union_default(
        &self,
        union: &UnionSchema,
        gen_state: &GenState,
        default: &Option<Value>,
    ) -> Result<String> {
        if union.is_nullable() {
            let default_str = match default {
                None => "None".into(),
                Some(Value::Null) => "None".into(),
                _ => err!("Invalid optional union default: {:?}", default)?,
            };
            Ok(default_str)
        } else {
            let e_name = union_type(union, gen_state, false)?;
            let e_variant = union_enum_variant(&union.variants()[0], gen_state)?;
            let default_str = self.parse_default(&union.variants()[0], gen_state, default)?;
            Ok(format!("{}::{}({})", e_name, e_variant, default_str))
        }
    }
}

/// Generates the Rust type of the inner schema of an Avro array.
pub(crate) fn array_type(inner: &Schema, gen_state: &GenState) -> Result<String> {
    let type_str = match inner {
        Schema::Boolean => "Vec<bool>".into(),
        Schema::Int => "Vec<i32>".into(),
        Schema::Long => "Vec<i64>".into(),
        Schema::Float => "Vec<f32>".into(),
        Schema::Double => "Vec<f64>".into(),
        Schema::Bytes => "Vec<Vec<u8>>".into(),
        Schema::String => "Vec<String>".into(),

        Schema::Date => "Vec<i32>".into(),
        Schema::TimeMillis => "Vec<i32>".into(),
        Schema::TimeMicros => "Vec<i64>".into(),
        Schema::TimestampMillis => "Vec<i64>".into(),
        Schema::TimestampMicros => "Vec<i64>".into(),

        Schema::Uuid => "Vec<uuid::Uuid>".into(),
        Schema::Decimal { .. } => "Vec<avro_rs::Decimal>".into(),
        Schema::Duration { .. } => "Vec<avro_rs::Duration>".into(),

        Schema::Fixed {
            name: Name { name: f_name, .. },
            ..
        } => {
            let f_name = sanitize(f_name.to_camel_case());
            format!("Vec<{}>", f_name)
        }

        Schema::Array(..) | Schema::Map(..) | Schema::Union(..) => {
            let nested_type = gen_state.get_type(inner).ok_or_else(|| {
                Error::Template(format!(
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

/// Generates the Rust type of the inner schema of an Avro map.
pub(crate) fn map_type(inner: &Schema, gen_state: &GenState) -> Result<String> {
    fn map_of(t: &str) -> String {
        format!("::std::collections::HashMap<String, {}>", t)
    }

    let type_str = match inner {
        Schema::Boolean => map_of("bool"),
        Schema::Int => map_of("i32"),
        Schema::Long => map_of("i64"),
        Schema::Float => map_of("f32"),
        Schema::Double => map_of("f64"),
        Schema::Bytes => map_of("Vec<u8>"),
        Schema::String => map_of("String"),

        Schema::Date => map_of("i32"),
        Schema::TimeMillis => map_of("i32"),
        Schema::TimeMicros => map_of("i64"),
        Schema::TimestampMillis => map_of("i64"),
        Schema::TimestampMicros => map_of("i64"),

        Schema::Uuid => map_of("uuid::Uuid"),
        Schema::Decimal { .. } => map_of("avro_rs::Decimal"),
        Schema::Duration { .. } => map_of("avro_rs::Duration"),

        Schema::Fixed {
            name: Name { name: f_name, .. },
            ..
        } => {
            let f_name = sanitize(f_name.to_camel_case());
            map_of(&f_name)
        }

        Schema::Array(..) | Schema::Map(..) | Schema::Union(..) => {
            let nested_type = gen_state.get_type(inner).ok_or_else(|| {
                Error::Template(format!(
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

fn union_enum_variant(schema: &Schema, gen_state: &GenState) -> Result<String> {
    let variant_str = match schema {
        Schema::Boolean => "Boolean".into(),
        Schema::Int => "Int".into(),
        Schema::Long => "Long".into(),
        Schema::Float => "Float".into(),
        Schema::Double => "Double".into(),
        Schema::Bytes => "Bytes".into(),
        Schema::String => "String".into(),
        Schema::Array(inner) => format!("Array{:?}", union_enum_variant(inner.as_ref(), gen_state)),
        Schema::Map(inner) => format!("Map{:?}", union_enum_variant(inner.as_ref(), gen_state)),
        Schema::Union(union) => union_type(union, gen_state, false)?,
        Schema::Record {
            name: Name { name, .. },
            ..
        } => name.to_camel_case(),
        Schema::Enum {
            name: Name { name, .. },
            ..
        } => sanitize(name.to_camel_case()),
        Schema::Fixed {
            name: Name { name, .. },
            ..
        } => sanitize(name.to_camel_case()),

        Schema::Decimal { .. } => "Decimal".into(),
        Schema::Uuid => "Uuid".into(),
        Schema::Date => "Date".into(),
        Schema::TimeMillis => "TimeMillis".into(),
        Schema::TimeMicros => "TimeMicros".into(),
        Schema::TimestampMillis => "TimestampMillis".into(),
        Schema::TimestampMicros => "TimestampMicros".into(),
        Schema::Duration => "Duration".into(),
        Schema::Null => {
            err!("Invalid Schema::Null not in first position on an UnionSchema variants")?
        }
    };

    Ok(variant_str)
}

pub(crate) fn union_type(
    union: &UnionSchema,
    gen_state: &GenState,
    wrap_if_optional: bool,
) -> Result<String> {
    let variants = union.variants();

    if variants.len() == 0 {
        err!("Invalid empty Schema::Union")?
    } else if variants.len() == 1 && variants[0] == Schema::Null {
        err!("Invalid Schema::Union of only Schema::Null")?
    }

    if union.is_nullable() && variants.len() == 2 {
        return Ok(option_type(&variants[1], gen_state)?);
    }

    let schemas = if variants[0] == Schema::Null {
        &variants[1..]
    } else {
        variants
    };

    let mut type_str = String::from("Union");
    for sc in schemas {
        type_str.push_str(&union_enum_variant(sc, gen_state)?);
    }

    if variants[0] == Schema::Null && wrap_if_optional {
        Ok(format!("Option<{}>", type_str))
    } else {
        Ok(type_str.into())
    }
}

/// Generates the Rust type of the inner schema of an Avro optional union.
pub(crate) fn option_type(inner: &Schema, gen_state: &GenState) -> Result<String> {
    let type_str = match inner {
        Schema::Boolean => "Option<bool>".into(),
        Schema::Int => "Option<i32>".into(),
        Schema::Long => "Option<i64>".into(),
        Schema::Float => "Option<f32>".into(),
        Schema::Double => "Option<f64>".into(),
        Schema::Bytes => "Option<Vec<u8>>".into(),
        Schema::String => "Option<String>".into(),

        Schema::Date => "Option<i32>".into(),
        Schema::TimeMillis => "Option<i32>".into(),
        Schema::TimeMicros => "Option<i64>".into(),
        Schema::TimestampMillis => "Option<i64>".into(),
        Schema::TimestampMicros => "Option<i64>".into(),

        Schema::Uuid => "Option<uuid::Uuid>".into(),
        Schema::Decimal { .. } => "Option<avro_rs::Decimal>".into(),
        Schema::Duration { .. } => "Option<avro_rs::Duration>".into(),

        Schema::Fixed {
            name: Name { name: f_name, .. },
            ..
        } => {
            let f_name = sanitize(f_name.to_camel_case());
            format!("Option<{}>", f_name)
        }

        Schema::Array(..) | Schema::Map(..) | Schema::Union(..) => {
            let nested_type = gen_state.get_type(inner).ok_or_else(|| {
                Error::Template(format!(
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
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct User {
    pub r#as: String,
    #[serde(rename = "favoriteNumber")]
    pub favorite_number: i32,
    pub likes_pizza: bool,
    pub b: Vec<u8>,
    #[serde(rename = "a-bool")]
    pub a_bool: Vec<bool>,
    #[serde(rename = "a-i32")]
    pub a_i32: Vec<i32>,
    #[serde(rename = "m-f64")]
    pub m_f64: ::std::collections::HashMap<String, f64>,
}

impl Default for User {
    fn default() -> User {
        User {
            r#as: String::default(),
            favorite_number: 7,
            likes_pizza: false,
            b: vec![195, 191],
            a_bool: vec![true, false],
            a_i32: vec![12, -1],
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
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
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
    fn gen_default_record() {
        let raw_schema = r#"
{
  "type": "record",
  "name": "User",
  "fields": [ {
    "name": "info",
    "type": {
      "type": "record",
      "name": "Info",
      "fields": [ {
        "name": "name",
        "type": "string"
      } ]
    },
    "default": {"name": "bob"}
  } ]
}
"#;

        let expected = r#"
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct User {
    pub info: Info,
}

impl Default for User {
    fn default() -> User {
        User {
            info: { let mut r = Info::default(); r.name = "bob".to_owned(); r },
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
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, serde::Deserialize, serde::Serialize)]
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

    #[test]
    fn gen_union() {
        let raw_schema = r#"
{
  "type": "record",
  "name": "Contact",
  "fields": [ {
    "name": "extra",
    "type" : ["null", "string", "long", "double", "boolean" ]
  } ]
}
"#;

        let templater = Templater::new().unwrap();
        let schema = Schema::parse_str(&raw_schema).unwrap();
        let gs = GenState::new();
        let res = templater.str_record(&schema, &gs).unwrap();

        let expected = "
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct Contact {
    pub extra: Option<UnionStringLongDoubleBoolean>,
}

impl Default for Contact {
    fn default() -> Contact {
        Contact {
            extra: None,
        }
    }
}
";

        assert_eq!(expected, res);
    }
}
