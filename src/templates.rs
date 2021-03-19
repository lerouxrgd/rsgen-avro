//! Logic for templating Rust types and default values from Avro schema.

use std::collections::{HashMap, HashSet};

use avro_rs::schema::*;
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
#[serde(default)]
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
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
    pub fn put_type(&mut self, schema: SchemaType, t: String) {
        let k = serde_json::to_string(&schema).expect("Unexpected invalid schema");
        self.0.insert(k, t);
    }

    /// Retrieves the String type of a given schema.
    pub fn get_type(&self, schema: SchemaType) -> Option<&String> {
        let k = serde_json::to_string(&schema).expect("Unexpected invalid schema");
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

    /// Generates a Rust type based on a SchemaType::Fixed schema.
    pub fn str_fixed(&self, schema: SchemaType) -> Result<String> {
        if let SchemaType::Fixed(fixed) = schema {
            let mut ctx = Context::new();
            ctx.insert("name", &sanitize(fixed.name().name().to_camel_case()));
            ctx.insert("size", &fixed.size());
            Ok(self.tera.render(FIXED_TERA, &ctx)?)
        } else {
            err!("Requires SchemaType::Fixed, found {:?}", schema)?
        }
    }

    /// Generates a Rust enum based on a SchemaType::Enum schema
    pub fn str_enum(&self, schema: SchemaType) -> Result<String> {
        if let SchemaType::Enum(inner) = schema {
            let symbols = inner.symbols();
            let name = inner.name().name();
            let doc = inner.doc();

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
            err!("Requires SchemaType::Enum, found {:?}", schema)?
        }
    }

    /// Generates a Rust struct based on a SchemaType::Record schema.
    /// Makes use of a `GenState` for nested schemas (i.e. Array/Map/Union).
    pub fn str_record(&self, schema: SchemaType, gen_state: &GenState) -> Result<String> {
        if let SchemaType::Record(record) = schema {
            let name = record.name();
            let fields = record.fields();
            let doc = record.doc();

            let mut ctx = Context::new();
            ctx.insert("name", &name.name().to_camel_case());
            let doc = if let Some(d) = doc { d } else { "" };
            ctx.insert("doc", doc);

            let mut f = Vec::new(); // field names;
            let mut t = HashMap::new(); // field name -> field type
            let mut o = HashMap::new(); // field name -> original name
            let mut d = HashMap::new(); // field name -> default value

            let by_pos = fields
                .iter()
                .map(|f| (f.position(), f))
                .collect::<HashMap<_, _>>();
            let mut i = 0;

            while let Some(field) = by_pos.get(&i) {
                let name_std = sanitize(field.name().to_snake_case());
                o.insert(name_std.clone(), name.name());

                let default = field.default();

                match field.schema() {
                    SchemaType::Boolean => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "bool".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    SchemaType::Int | SchemaType::Date | SchemaType::TimeMillis => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "i32".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    SchemaType::Long
                    | SchemaType::TimeMicros
                    | SchemaType::TimestampMillis
                    | SchemaType::TimestampMicros => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "i64".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    SchemaType::Float => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "f32".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    SchemaType::Double => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "f64".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    SchemaType::Bytes => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "Vec<u8>".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    SchemaType::String => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "String".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    SchemaType::Uuid => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "uuid::Uuid".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    SchemaType::Duration => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "avro_rs::Duration".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    SchemaType::Decimal(_) => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "avro_rs::Decimal".to_string());
                        d.insert(name_std.clone(), default);
                    }

                    SchemaType::Fixed(fixed) => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        let f_name = sanitize(fixed.name().name().to_camel_case());
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), f_name.clone());
                        d.insert(name_std.clone(), default);
                    }

                    SchemaType::Array(array) => match array.items() {
                        SchemaType::Null => err!("Invalid use of SchemaType::Null")?,
                        inner => {
                            let default = self.parse_default(schema, gen_state, default)?;
                            let type_str = array_type(inner, gen_state)?;
                            f.push(name_std.clone());
                            t.insert(name_std.clone(), type_str);
                            d.insert(name_std.clone(), default);
                        }
                    },

                    SchemaType::Map(map) => match map.items() {
                        SchemaType::Null => err!("Invalid use of SchemaType::Null")?,
                        inner => {
                            let default = self.parse_default(schema, gen_state, default)?;
                            let type_str = map_type(inner, gen_state)?;
                            f.push(name_std.clone());
                            t.insert(name_std.clone(), type_str);
                            d.insert(name_std.clone(), default);
                        }
                    },

                    SchemaType::Record(record) => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        let r_name = sanitize(record.name().name().to_camel_case());
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), r_name.clone());
                        d.insert(name_std.clone(), default);
                    }

                    SchemaType::Enum(enumeration) => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        let e_name = sanitize(enumeration.name().name().to_camel_case());
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), e_name);
                        d.insert(name_std.clone(), default);
                    }

                    SchemaType::Union(union) => {
                        let default = self.parse_default(schema, gen_state, default)?;
                        let type_str = union_type(union, gen_state, true)?;
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), type_str);
                        d.insert(name_std.clone(), default);
                    }

                    SchemaType::Null => err!("Invalid use of SchemaType::Null")?,
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
            err!("Requires SchemaType::Record, found {:?}", schema)?
        }
    }

    pub fn str_union_enum(&self, schema: SchemaType, gen_state: &GenState) -> Result<String> {
        if let SchemaType::Union(union) = schema {
            let mut variants = union.variants();

            if variants.is_empty() {
                err!("Invalid empty SchemaType::Union")?
            } else if variants.len() == 1 {
                err!("Invalid SchemaType::Union of a single element")?
            } else if union.is_nullable() && variants.len() == 2 {
                err!("Attempt to generate a union enum for an optional")?
            }

            if variants[0] == SchemaType::Null {
                variants.remove(0);
            }

            let schemas = variants;

            let e_name = union_type(union, gen_state, false)?;

            let mut symbols = vec![];
            for sc in schemas {
                let symbol_str = match sc {
                    SchemaType::Boolean => "Boolean(bool)".into(),
                    SchemaType::Int => "Int(i32)".into(),
                    SchemaType::Long => "Long(i64)".into(),
                    SchemaType::Float => "Float(f32)".into(),
                    SchemaType::Double => "Double(f64)".into(),
                    SchemaType::Bytes => "Bytes(Vec<u8>)".into(),
                    SchemaType::String => "String(String)".into(),
                    SchemaType::Array(inner) => {
                        format!(
                            "Array{}({})",
                            union_enum_variant(inner.items(), gen_state)?,
                            array_type(inner.items(), gen_state)?
                        )
                    }
                    SchemaType::Map(inner) => format!(
                        "Map{}({})",
                        union_enum_variant(inner.items(), gen_state)?,
                        map_type(sc, gen_state)?
                    ),
                    SchemaType::Union(union) => {
                        format!("{u}({u})", u = union_type(union, gen_state, false)?)
                    }
                    SchemaType::Record(record) => {
                        format!("{rec}({rec})", rec = record.name().name().to_camel_case())
                    }
                    SchemaType::Enum(inner) => {
                        format!("{e}({e})", e = sanitize(inner.name().name().to_camel_case()))
                    }
                    SchemaType::Fixed(fixed) => {
                        format!("{f}({f})", f = sanitize(fixed.name().name().to_camel_case()))
                    }
                    SchemaType::Decimal(_) => "Decimal(avro_rs::Decimal)".into(),
                    SchemaType::Uuid => "Uuid(uuid::Uuid)".into(),
                    SchemaType::Date => "Date(i32)".into(),
                    SchemaType::TimeMillis => "TimeMillis(i32)".into(),
                    SchemaType::TimeMicros => "TimeMicros(i64)".into(),
                    SchemaType::TimestampMillis => "TimestampMillis(i64)".into(),
                    SchemaType::TimestampMicros => "TimestampMicros(i64)".into(),
                    SchemaType::Duration => "Duration(avro_rs::Duration)".into(),
                    SchemaType::Null => err!(
                        "Invalid SchemaType::Null not in first position on an UnionSchema variants"
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
            err!("Requires SchemaType::Union, found {:?}", schema)?
        }
    }

    fn parse_default(
        &self,
        schema: SchemaType,
        gen_state: &GenState,
        default: Option<&serde_json::Value>,
    ) -> Result<String> {
        let default_str = match schema {
            SchemaType::Boolean => match default {
                Some(Value::Bool(b)) => b.to_string(),
                None => bool::default().to_string(),
                _ => err!("Invalid default: {:?}", default)?,
            },

            SchemaType::Int | SchemaType::Date | SchemaType::TimeMillis => match default {
                Some(Value::Number(n)) if n.is_i64() => (n.as_i64().unwrap() as i32).to_string(),
                None => i32::default().to_string(),
                _ => err!("Invalid default: {:?}", default)?,
            },

            SchemaType::Long
            | SchemaType::TimeMicros
            | SchemaType::TimestampMillis
            | SchemaType::TimestampMicros => match default {
                Some(Value::Number(n)) if n.is_i64() => n.to_string(),
                None => i64::default().to_string(),
                _ => err!("Invalid default: {:?}", default)?,
            },

            SchemaType::Float => match default {
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

            SchemaType::Double => match default {
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

            SchemaType::Bytes => match default {
                Some(Value::String(s)) => {
                    let bytes = s.clone().into_bytes();
                    format!("vec!{:?}", bytes)
                }
                None => "vec![]".to_string(),
                _ => err!("Invalid default: {:?}", default)?,
            },

            SchemaType::String => match default {
                Some(Value::String(s)) => format!("\"{}\".to_owned()", s),
                None => "String::default()".to_string(),
                _ => err!("Invalid default: {:?}", default)?,
            },

            SchemaType::Uuid => match default {
                Some(Value::String(s)) => Uuid::parse_str(&s)?.to_string(),
                None => Uuid::nil().to_string(),
                _ => err!("Invalid default: {:?}", default)?,
            },

            SchemaType::Duration => match default {
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

            SchemaType::Decimal(decimal) => match decimal.size() {
                None => match default {
                    Some(Value::String(s)) => {
                        let bytes = s.clone().into_bytes();
                        format!("vec!{:?}", bytes)
                    }
                    None => "vec![]".to_string(),
                    _ => err!("Invalid default: {:?}", default)?,
                },
                Some(size) => match default {
                    Some(Value::String(s)) => {
                        let bytes = s.clone().into_bytes();
                        if bytes.len() != size as usize {
                            err!("Invalid default: {:?}", bytes)?
                        }
                        format!("{:?}", bytes)
                    }
                    None => format!("{}::default()", decimal.name().name()),
                    _ => err!("Invalid default: {:?}", default)?,
                },
            },

            SchemaType::Fixed(fixed) => match default {
                Some(Value::String(s)) => {
                    let bytes = s.clone().into_bytes();
                    if bytes.len() != fixed.size() {
                        err!("Invalid default: {:?}", bytes)?
                    }
                    format!("{:?}", bytes)
                }
                None => format!("{}::default()", fixed.name().name()),
                _ => err!("Invalid default: {:?}", default)?,
            },

            SchemaType::Array(inner) => match inner.items() {
                SchemaType::Null => err!("Invalid use of SchemaType::Null")?,
                items => self.array_default(items, gen_state, default)?,
            },

            SchemaType::Map(inner) => match inner.items() {
                SchemaType::Null => err!("Invalid use of SchemaType::Null")?,
                items => self.map_default(items, gen_state, default)?,
            },

            SchemaType::Record(_) => self.record_default(schema, gen_state, default)?,

            SchemaType::Enum(inner) => {
                let e_name = sanitize(inner.name().name().to_camel_case());
                let valids: HashSet<_> = inner.iter_symbols()
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
                    None if !inner.symbols().is_empty() => {
                        format!("{}::{}", e_name, sanitize(inner.symbols()[0].to_camel_case()))
                    }
                    _ => err!("Invalid default: {:?}", default)?,
                }
            }

            SchemaType::Union(union) => self.union_default(union, gen_state, default)?,

            SchemaType::Null => err!("Invalid use of SchemaType::Null")?,
        };

        Ok(default_str)
    }

    /// Helper to coerce defaults from an Avro schema to Rust types.
    fn coerce_default_fn<'a>(
        &'a self,
        schema: SchemaType<'a>,
        gen_state: &'a GenState,
    ) -> Box<dyn Fn(&'a Value) -> Result<String> + 'a> {
        match schema {
            SchemaType::Null => Box::new(|_| err!("Invalid use of SchemaType::Null")?),

            SchemaType::Boolean => Box::new(|v: &Value| match v {
                Value::Bool(b) => Ok(b.to_string()),
                _ => err!("Invalid defaults: {:?}", v),
            }),

            SchemaType::Int | SchemaType::Date | SchemaType::TimeMillis => Box::new(|v: &Value| match v {
                Value::Number(n) if n.is_i64() => Ok((n.as_i64().unwrap() as i32).to_string()),
                _ => err!("Invalid defaults: {:?}", v),
            }),

            SchemaType::Long
            | SchemaType::TimeMicros
            | SchemaType::TimestampMillis
            | SchemaType::TimestampMicros => Box::new(|v: &Value| match v {
                Value::Number(n) if n.is_i64() => Ok(n.to_string()),
                _ => err!("Invalid defaults: {:?}", v),
            }),

            SchemaType::Float => Box::new(move |v: &Value| match v {
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

            SchemaType::Double => Box::new(move |v: &Value| match v {
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

            SchemaType::Bytes => Box::new(|v: &Value| match v {
                Value::String(s) => {
                    let bytes = s.clone().into_bytes();
                    Ok(format!("vec!{:?}", bytes))
                }
                _ => err!("Invalid defaults: {:?}", v),
            }),

            SchemaType::String => Box::new(|v: &Value| match v {
                Value::String(s) => Ok(format!("\"{}\".to_owned()", s)),
                _ => err!("Invalid defaults: {:?}", v),
            }),

            SchemaType::Uuid => Box::new(|v: &Value| match v {
                Value::String(s) => Ok(Uuid::parse_str(s)?.to_string()),
                _ => err!("Invalid defaults: {:?}", v),
            }),

            SchemaType::Duration => Box::new(|v: &Value| match v {
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

            SchemaType::Decimal(decimal) => Box::new(move |v: &Value| match decimal.size() {
                None => match v {
                    Value::String(s) => {
                        let bytes = s.clone().into_bytes();
                        Ok(format!("vec!{:?}", bytes))
                    }
                    _ => err!("Invalid default: {:?}", v),
                },
                Some(size) => match v {
                    Value::String(s) => {
                        let bytes = s.clone().into_bytes();
                        if bytes.len() != size as usize {
                            return err!("Invalid default: {:?}", bytes);
                        }
                        Ok(format!("{:?}", bytes))
                    }
                    _ => err!("Invalid default: {:?}", v),
                },
            }),

            SchemaType::Fixed(fixed) => Box::new(move |v: &Value| match v {
                Value::String(s) => {
                    let bytes = s.clone().into_bytes();
                    if bytes.len() == fixed.size() {
                        Ok(format!("{:?}", bytes))
                    } else {
                        err!("Invalid defaults: {:?}", bytes)
                    }
                }
                _ => err!("Invalid defaults: {:?}", v),
            }),

            SchemaType::Array(array) => {
                let items = array.items();
                Box::new(move |v: &Value| self.array_default(items, gen_state, Some(v)))
            }

            SchemaType::Map(map) => {
                let items = map.items();
                Box::new(move |v: &Value| self.map_default(items, gen_state, Some(v)))
            }

            SchemaType::Enum(enumeration) => {
                let e_name = sanitize(enumeration.name().name().to_camel_case());
                let valids: HashSet<_> = enumeration.symbols()
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

            SchemaType::Union(union) => {
                Box::new(move |v: &Value| self.union_default(union, gen_state, Some(v)))
            }

            SchemaType::Record { .. } => {
                Box::new(move |v: &Value| self.record_default(schema, gen_state, Some(v)))
            }
        }
    }

    /// Generates Rust default values for the inner schema of an Avro array.
    fn array_default(
        &self,
        inner: SchemaType,
        gen_state: &GenState,
        default: Option<&Value>,
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
        inner: SchemaType,
        gen_state: &GenState,
        default: Option<&Value>,
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
        inner: SchemaType,
        gen_state: &GenState,
        default: Option<&Value>,
    ) -> Result<String> {
        match inner {
            SchemaType::Record(record) => {
                let name = record.name().name();
                let default_str = if let Some(Value::Object(o)) = default {
                    if o.len() > 0 {
                        let vals = o
                            .iter()
                            .map(|(k, v)| {
                                let f = sanitize(k.to_snake_case());
                                let rf = record.field(k).expect("Missing record field");
                                let d = self.coerce_default_fn(rf.schema(), gen_state)(v)?;
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
        union: UnionSchema,
        gen_state: &GenState,
        default: Option<&Value>,
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
            let e_variant = union_enum_variant(union.variants()[0], gen_state)?;
            let default_str = self.parse_default(union.variants()[0], gen_state, default)?;
            Ok(format!("{}::{}({})", e_name, e_variant, default_str))
        }
    }
}

/// Generates the Rust type of the inner schema of an Avro array.
pub(crate) fn array_type(inner: SchemaType, gen_state: &GenState) -> Result<String> {
    let type_str = match inner {
        SchemaType::Boolean => "Vec<bool>".into(),
        SchemaType::Int => "Vec<i32>".into(),
        SchemaType::Long => "Vec<i64>".into(),
        SchemaType::Float => "Vec<f32>".into(),
        SchemaType::Double => "Vec<f64>".into(),
        SchemaType::Bytes => "Vec<Vec<u8>>".into(),
        SchemaType::String => "Vec<String>".into(),

        SchemaType::Date => "Vec<i32>".into(),
        SchemaType::TimeMillis => "Vec<i32>".into(),
        SchemaType::TimeMicros => "Vec<i64>".into(),
        SchemaType::TimestampMillis => "Vec<i64>".into(),
        SchemaType::TimestampMicros => "Vec<i64>".into(),

        SchemaType::Uuid => "Vec<uuid::Uuid>".into(),
        SchemaType::Decimal { .. } => "Vec<avro_rs::Decimal>".into(),
        SchemaType::Duration { .. } => "Vec<avro_rs::Duration>".into(),

        SchemaType::Fixed(fixed) => {
            let f_name = sanitize(fixed.name().name().to_camel_case());
            format!("Vec<{}>", f_name)
        }

        SchemaType::Array(inner) | SchemaType::Map(inner) => {
            let nested_type = gen_state.get_type(inner.items()).ok_or_else(|| {
                Error::Template(format!(
                    "Didn't find schema {:?} in state {:?}",
                    inner, &gen_state
                ))
            })?;
            format!("Vec<{}>", nested_type)
        }

        SchemaType::Union(_) => {
            let nested_type = gen_state.get_type(inner).ok_or_else(|| {
                Error::Template(format!(
                    "Didn't find schema {:?} in state {:?}",
                    inner, &gen_state
                ))
            })?;
            format!("Vec<{}>", nested_type)
        }

        SchemaType::Record(inner) => {
            format!("Vec<{}>", &sanitize(inner.name().name().to_camel_case()))
        }

        SchemaType::Enum(inner) => {
            format!("Vec<{}>", &sanitize(inner.name().name().to_camel_case()))
        }

        SchemaType::Null => err!("Invalid use of SchemaType::Null")?,
    };
    Ok(type_str)
}

/// Generates the Rust type of the inner schema of an Avro map.
pub(crate) fn map_type(inner: SchemaType, gen_state: &GenState) -> Result<String> {
    fn map_of(t: &str) -> String {
        format!("::std::collections::HashMap<String, {}>", t)
    }

    let type_str = match inner {
        SchemaType::Boolean => map_of("bool"),
        SchemaType::Int => map_of("i32"),
        SchemaType::Long => map_of("i64"),
        SchemaType::Float => map_of("f32"),
        SchemaType::Double => map_of("f64"),
        SchemaType::Bytes => map_of("Vec<u8>"),
        SchemaType::String => map_of("String"),

        SchemaType::Date => map_of("i32"),
        SchemaType::TimeMillis => map_of("i32"),
        SchemaType::TimeMicros => map_of("i64"),
        SchemaType::TimestampMillis => map_of("i64"),
        SchemaType::TimestampMicros => map_of("i64"),

        SchemaType::Uuid => map_of("uuid::Uuid"),
        SchemaType::Decimal(_) => map_of("avro_rs::Decimal"),
        SchemaType::Duration => map_of("avro_rs::Duration"),

        SchemaType::Fixed(fixed) => {
            let f_name = sanitize(fixed.name().name().to_camel_case());
            map_of(&f_name)
        }

        SchemaType::Array(inner) | SchemaType::Map(inner) => {
            let nested_type = gen_state.get_type(inner.items()).ok_or_else(|| {
                Error::Template(format!(
                    "Didn't find schema {:?} in state {:?}",
                    inner, &gen_state
                ))
            })?;
            map_of(nested_type)
        }

        SchemaType::Union(_) => {
            let nested_type = gen_state.get_type(inner).ok_or_else(|| {
                Error::Template(format!(
                    "Didn't find schema {:?} in state {:?}",
                    inner, &gen_state
                ))
            })?;
            map_of(nested_type)
        }

        SchemaType::Record(inner) => map_of(&sanitize(inner.name().name().to_camel_case())),
        SchemaType::Enum(inner) => map_of(&sanitize(inner.name().name().to_camel_case())),

        SchemaType::Null => err!("Invalid use of SchemaType::Null")?,
    };
    Ok(type_str)
}

fn union_enum_variant(schema: SchemaType, gen_state: &GenState) -> Result<String> {
    let variant_str = match schema {
        SchemaType::Boolean => "Boolean".into(),
        SchemaType::Int => "Int".into(),
        SchemaType::Long => "Long".into(),
        SchemaType::Float => "Float".into(),
        SchemaType::Double => "Double".into(),
        SchemaType::Bytes => "Bytes".into(),
        SchemaType::String => "String".into(),
        SchemaType::Array(inner) => format!("Array{:?}", union_enum_variant(inner.items(), gen_state)),
        SchemaType::Map(inner) => format!("Map{:?}", union_enum_variant(inner.items(), gen_state)),
        SchemaType::Union(union) => union_type(union, gen_state, false)?,
        SchemaType::Record(record) => record.name().name().to_camel_case(),
        SchemaType::Enum(enumeration) => sanitize(enumeration.name().name().to_camel_case()),
        SchemaType::Fixed(fixed) => sanitize(fixed.name().name().to_camel_case()),
        SchemaType::Decimal(_)=> "Decimal".into(),
        SchemaType::Uuid => "Uuid".into(),
        SchemaType::Date => "Date".into(),
        SchemaType::TimeMillis => "TimeMillis".into(),
        SchemaType::TimeMicros => "TimeMicros".into(),
        SchemaType::TimestampMillis => "TimestampMillis".into(),
        SchemaType::TimestampMicros => "TimestampMicros".into(),
        SchemaType::Duration => "Duration".into(),
        SchemaType::Null => {
            err!("Invalid SchemaType::Null not in first position on an UnionSchema variants")?
        }
    };

    Ok(variant_str)
}

pub(crate) fn union_type(
    union: UnionSchema,
    gen_state: &GenState,
    wrap_if_optional: bool,
) -> Result<String> {
    let mut variants = union.variants();

    if variants.len() == 0 {
        err!("Invalid empty SchemaType::Union")?
    } else if variants.len() == 1 && variants[0] == SchemaType::Null {
        err!("Invalid SchemaType::Union of only SchemaType::Null")?
    }

    if union.is_nullable() && variants.len() == 2 {
        return Ok(option_type(variants[1], gen_state)?);
    }

    if variants[0] == SchemaType::Null {
        variants.remove(0);
    }

    let schemas = variants;

    let mut type_str = String::from("Union");
    for sc in schemas {
        type_str.push_str(&union_enum_variant(sc, gen_state)?);
    }

    if variants[0] == SchemaType::Null && wrap_if_optional {
        Ok(format!("Option<{}>", type_str))
    } else {
        Ok(type_str.into())
    }
}

/// Generates the Rust type of the inner schema of an Avro optional union.
pub(crate) fn option_type(inner: SchemaType, gen_state: &GenState) -> Result<String> {
    let type_str = match inner {
        SchemaType::Boolean => "Option<bool>".into(),
        SchemaType::Int => "Option<i32>".into(),
        SchemaType::Long => "Option<i64>".into(),
        SchemaType::Float => "Option<f32>".into(),
        SchemaType::Double => "Option<f64>".into(),
        SchemaType::Bytes => "Option<Vec<u8>>".into(),
        SchemaType::String => "Option<String>".into(),

        SchemaType::Date => "Option<i32>".into(),
        SchemaType::TimeMillis => "Option<i32>".into(),
        SchemaType::TimeMicros => "Option<i64>".into(),
        SchemaType::TimestampMillis => "Option<i64>".into(),
        SchemaType::TimestampMicros => "Option<i64>".into(),

        SchemaType::Uuid => "Option<uuid::Uuid>".into(),
        SchemaType::Decimal(_) => "Option<avro_rs::Decimal>".into(),
        SchemaType::Duration => "Option<avro_rs::Duration>".into(),

        SchemaType::Fixed(fixed) => {
            let f_name = sanitize(fixed.name().name().to_camel_case());
            format!("Option<{}>", f_name)
        }

        // TODO(Bowyer) - This duplication is irritating
        SchemaType::Array(inner) | SchemaType::Map(inner) => {
            let nested_type = gen_state.get_type(inner.items()).ok_or_else(|| {
                Error::Template(format!(
                    "Didn't find schema {:?} in state {:?}",
                    inner, &gen_state
                ))
            })?;
            format!("Option<{}>", nested_type)
        },

        SchemaType::Union(_) => {
            let nested_type = gen_state.get_type(inner).ok_or_else(|| {
                Error::Template(format!(
                    "Didn't find schema {:?} in state {:?}",
                    inner, &gen_state
                ))
            })?;
            format!("Option<{}>", nested_type)
        }

        SchemaType::Record(inner) => {
            format!("Option<{}>", &sanitize(inner.name().name().to_camel_case()))
        }

        SchemaType::Enum(inner) => {
            format!("Option<{}>", &sanitize(inner.name().name().to_camel_case()))
        }

        SchemaType::Null => err!("Invalid use of SchemaType::Null")?,
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
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
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
        let res = templater.str_record(schema.root(), &gs).unwrap();

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
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
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
        let res = templater.str_record(schema.root(), &gs).unwrap();

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
#[serde(default)]
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
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
        let res = templater.str_record(schema.root(), &gs).unwrap();

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
        let res = templater.str_enum(schema.root()).unwrap();

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
        let res = templater.str_fixed(schema.root()).unwrap();

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
        let res = templater.str_record(schema.root(), &gs).unwrap();

        let expected = "
#[serde(default)]
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
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
