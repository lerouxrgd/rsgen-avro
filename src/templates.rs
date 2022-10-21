//! Logic for templating Rust types and default values from Avro schema.

#![allow(clippy::try_err)]

use std::collections::{HashMap, HashSet};

use apache_avro::schema::{Name, RecordField, UnionSchema};
use apache_avro::Schema;
use heck::{ToSnakeCase, ToUpperCamelCase};
use lazy_static::lazy_static;
use serde_json::Value;
use tera::{Context, Tera};
use uuid::Uuid;

use crate::error::{Error, Result};

pub const RECORD_TERA: &str = "record.tera";
pub const RECORD_TEMPLATE: &str = r#"
{%- if doc %}
{%- set doc_lines = doc | split(pat="\n") %}
{%- for doc_line in doc_lines %}
/// {{ doc_line }}
{%- endfor %}
{%- endif %}
#[derive(Debug, PartialEq{%- if is_eq_derivable %}, Eq{%- endif %}, Clone, serde::Deserialize, serde::Serialize{%- if derive_builders %}, derive_builder::Builder {%- endif %}{%- if derive_schemas %}, apache_avro::AvroSchema {%- endif %})]
{%- if derive_builders %}
#[builder(setter(into))]
{%- endif %}
{%- if fields | length == defaults | length %}
#[serde(default)]
{%- endif %}
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
    {%- if defaults is containing(f) and not fields | length == defaults | length %}
    #[serde(default = "default_{{ name | lower }}_{{ f | lower | trim_start_matches(pat="r#") }}")]
    {%- endif %}
    {%- if bytes is containing(f) %}
    #[serde(with = "serde_bytes")]
    {%- endif %}
    pub {{ f }}: {{ type }},
    {%- endfor %}
}

{%- for f in fields %}
{%- set type = types[f] %}
{%- if nullable and not type is starting_with("Option") %}
{# #}
#[inline(always)]
fn nullable_{{ name|lower }}_{{ f }}<'de, D>(deserializer: D) -> Result<{{ type }}, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_else(|| {{ defaults[f] }}))
}
{%- endif %}
{%- endfor %}

{%- for f in fields %}
{%- if defaults is containing(f) %}
{# #}
#[inline(always)]
fn default_{{ name | lower }}_{{ f | lower | trim_start_matches(pat="r#") }}() -> {{ types[f] }} { {{ defaults[f] }} }
{%- endif %}
{%- endfor %}
{%- if fields | length == defaults | length %}
{# #}
impl Default for {{ name }} {
    fn default() -> {{ name }} {
        {{ name }} {
            {%- for f in fields %}
            {{ f }}: default_{{ name | lower }}_{{ f | lower | trim_start_matches(pat="r#") }}(),
            {%- endfor %}
        }
    }
}
{%- endif %}
"#;

pub const ENUM_TERA: &str = "enum.tera";
pub const ENUM_TEMPLATE: &str = r#"
{%- if doc %}
{%- set doc_lines = doc | split(pat="\n") %}
{%- for doc_line in doc_lines %}
/// {{ doc_line }}
{%- endfor %}
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
#[derive(Debug, PartialEq{%- if is_eq_derivable %}, Eq{%- endif %}, Clone{%- if not use_avro_rs_unions %}, serde::Deserialize {%- endif %}, serde::Serialize)]
pub enum {{ name }} {
    {%- for s in symbols %}
    {{ s }},
    {%- endfor %}
}
{%- for v in visitors %}
{# #}
impl From<{{ v.rust_type }}> for {{ name }} {
    fn from(v: {{ v.rust_type }}) -> Self {
        Self::{{ v.variant }}(v)
    }
}

impl TryFrom<{{ name }}> for {{ v.rust_type }} {
    type Error = {{ name }};

    fn try_from(v: {{ name }}) -> Result<Self, Self::Error> {
        if let {{ name }}::{{ v.variant }}(v) = v {
            Ok(v)
        } else {
            Err(v)
        }
    }
}
{%- endfor %}

{%- if use_avro_rs_unions %}
{# #}
impl<'de> serde::Deserialize<'de> for {{ name }} {
    fn deserialize<D>(deserializer: D) -> Result<{{ name }}, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        /// Serde visitor for the auto-generated unnamed Avro union type.
        struct {{ name }}Visitor;

        impl<'de> serde::de::Visitor<'de> for {{ name }}Visitor {
            type Value = {{ name }};

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a {{ name}}")
            }
            {%- for v in visitors %}
            {%- if v.serde_visitor %}

            fn visit_{{ v.serde_visitor | trim_start_matches(pat="&") }}<E>(self, value: {{ v.serde_visitor }}) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok({{ name }}::{{ v.variant }}(value.into()))
            }
            {%- endif %}
            {%- endfor %}
        }

        deserializer.deserialize_any({{ name }}Visitor)
    }
}
{%- endif %}
"#;

pub const FIXED_TERA: &str = "fixed.tera";
pub const FIXED_TEMPLATE: &str = "
pub type {{ name }} = [u8; {{ size }}];
";

lazy_static! {
    static ref RESERVED: HashSet<String> = {
        let s: HashSet<_> = vec![
            "Self", "abstract", "as", "async", "await", "become", "box", "break", "const",
            "continue", "crate", "do", "dyn", "else", "enum", "extern", "false", "final", "fn",
            "for", "if", "impl", "in", "let", "loop", "macro", "match", "mod", "move", "mut",
            "override", "priv", "pub", "ref", "return", "self", "static", "struct", "super",
            "trait", "true", "try", "type", "typeof", "union", "unsafe", "unsized", "use",
            "virtual", "where", "while", "yield",
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

macro_rules! err (
    ($($arg:tt)*) => (Err(Error::Template(format!($($arg)*))))
);

/// A helper struct for apache-avro union deserialization visitors.
#[derive(Debug, serde::Serialize)]
struct GenUnionVisitor {
    variant: String,
    rust_type: String,
    serde_visitor: Option<String>,
}

/// A helper struct for nested schema generation.
///
/// Used to store inner schema String type so that outer schema String type can be created.
#[derive(Debug, Default)]
pub struct GenState {
    types_by_schema: HashMap<String, String>,
    schemata_by_name: HashMap<Name, Schema>,
    not_eq: HashSet<String>,
}

impl GenState {
    pub fn with_deps(deps: &[Schema]) -> GenState {
        let schemata_by_name: HashMap<Name, Schema> = deps
            .iter()
            .filter_map(|s| match s {
                Schema::Record { name, .. }
                | Schema::Fixed { name, .. }
                | Schema::Enum { name, .. } => Some((name.clone(), s.clone())),
                _ => None,
            })
            .collect::<HashMap<_, _>>();
        let not_eq = Self::get_not_eq_schemas(deps, &schemata_by_name);
        GenState {
            types_by_schema: HashMap::new(),
            schemata_by_name,
            not_eq,
        }
    }

    pub(crate) fn get_schema(&self, name: &Name) -> Option<&Schema> {
        self.schemata_by_name.get(name)
    }

    /// Stores the String type of a given schema.
    pub fn put_type(&mut self, schema: &Schema, t: String) {
        let k = serde_json::to_string(schema).expect("Unexpected invalid schema");
        self.types_by_schema.insert(k, t);
    }

    /// Retrieves the String type of a given schema.
    pub fn get_type(&self, schema: &Schema) -> Option<&String> {
        let k = serde_json::to_string(schema).expect("Unexpected invalid schema");
        self.types_by_schema.get(&k)
    }

    /// Checks that schema does not contains nested type which does not implement Eq trait.
    pub fn is_eq_derivable(&self, schema: &Schema) -> bool {
        match schema {
            Schema::Union(_) | Schema::Record { .. } => !self
                .not_eq
                .contains(&serde_json::to_string(schema).expect("Unexpected invalid schema")),
            _ => true,
        }
    }

    /// Utility function to find nested type that does not implement Eq in record and it's dependencies.
    fn deep_search_not_eq(schema: &Schema, schemata_by_name: &HashMap<Name, Schema>) -> bool {
        match schema {
            Schema::Array(inner) | Schema::Map(inner) => {
                Self::deep_search_not_eq(inner, schemata_by_name)
            }
            Schema::Record { fields, .. } => fields
                .iter()
                .any(|f| Self::deep_search_not_eq(&f.schema, schemata_by_name)),
            Schema::Union(union) => union
                .variants()
                .iter()
                .any(|s| Self::deep_search_not_eq(s, schemata_by_name)),
            Schema::Ref { name } => schemata_by_name
                .get(name)
                .map(|s| Self::deep_search_not_eq(s, schemata_by_name))
                .unwrap_or_else(|| {
                    panic!("Ref `{:?}` is not resolved. Schema: {:?}", name, schema)
                }),
            Schema::Float | Schema::Double => true,
            _ => false,
        }
    }

    /// Fill HashSet with schemas that contains type which does not implement Eq
    fn get_not_eq_schemas(
        deps: &[Schema],
        schemata_by_name: &HashMap<Name, Schema>,
    ) -> HashSet<String> {
        let mut float_schemas = HashSet::new();
        for dep in deps {
            if Self::deep_search_not_eq(dep, schemata_by_name) {
                let str_schema = serde_json::to_string(dep).expect("Unexpected invalid schema");
                float_schemas.insert(str_schema);
            }
        }
        float_schemas
    }
}

/// The main, stateless, component for templating Rust types.
///
/// Current implementation uses Tera. Its responsability is to generate String
/// representing Rust code/types for a given Avro schema.
#[derive(Debug)]
pub struct Templater {
    tera: Tera,
    pub precision: usize,
    pub nullable: bool,
    pub use_avro_rs_unions: bool,
    pub derive_builders: bool,
    pub derive_schemas: bool,
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
            use_avro_rs_unions: false,
            derive_builders: false,
            derive_schemas: false,
        })
    }

    /// Generates a Rust type based on a `Schema::Fixed` schema.
    pub fn str_fixed(&self, schema: &Schema) -> Result<String> {
        if let Schema::Fixed {
            name: Name { name, .. },
            size,
            ..
        } = schema
        {
            let mut ctx = Context::new();
            ctx.insert("name", &sanitize(name.to_upper_camel_case()));
            ctx.insert("size", size);
            Ok(self.tera.render(FIXED_TERA, &ctx)?)
        } else {
            err!("Requires Schema::Fixed, found {:?}", schema)?
        }
    }

    /// Generates a Rust enum based on a `Schema::Enum` schema
    pub fn str_enum(&self, schema: &Schema) -> Result<String> {
        if let Schema::Enum {
            name: Name { name, .. },
            symbols,
            doc,
            ..
        } = schema
        {
            if symbols.is_empty() {
                err!("No symbol for enum: {:?}", name)?
            }
            let mut ctx = Context::new();
            ctx.insert("name", &sanitize(name.to_upper_camel_case()));
            let doc = if let Some(d) = doc { d } else { "" };
            ctx.insert("doc", doc);
            let o: HashMap<_, _> = symbols
                .iter()
                .map(|s| (sanitize(s.to_upper_camel_case()), s))
                .collect();
            let s: Vec<_> = symbols
                .iter()
                .map(|s| sanitize(s.to_upper_camel_case()))
                .collect();
            ctx.insert("originals", &o);
            ctx.insert("symbols", &s);
            Ok(self.tera.render(ENUM_TERA, &ctx)?)
        } else {
            err!("Requires Schema::Enum, found {:?}", schema)?
        }
    }

    /// Generates a Rust struct based on a `Schema::Record` schema.
    ///
    /// Makes use of a [`GenState`](GenState) for nested schemas (i.e. Array/Map/Union).
    pub fn str_record(&self, schema: &Schema, gen_state: &GenState) -> Result<String> {
        if let Schema::Record {
            name: Name { name, .. },
            fields,
            doc,
            ..
        } = schema
        {
            let mut ctx = Context::new();
            ctx.insert("name", &name.to_upper_camel_case());
            let doc = if let Some(d) = doc { d } else { "" };
            ctx.insert("doc", doc);
            ctx.insert("derive_builders", &self.derive_builders);
            ctx.insert("derive_schemas", &self.derive_schemas);

            let mut f = Vec::new(); // field names;
            let mut t = HashMap::new(); // field name -> field type
            let mut o = HashMap::new(); // field name -> original name
            let mut d = HashMap::new(); // field name -> default value
            let mut b = HashSet::new(); // bytes fields;

            let mut fields_by_pos = fields.iter().clone().collect::<Vec<_>>();
            fields_by_pos.sort_by_key(|f| f.position);

            for RecordField {
                schema,
                name,
                default,
                ..
            } in fields_by_pos.iter()
            {
                let name_std = sanitize(name.to_snake_case());
                o.insert(name_std.clone(), name);

                let schema = if let Schema::Ref { ref name } = schema {
                    gen_state.get_schema(name).unwrap_or_else(|| {
                        panic!("Schema reference '{:?}' cannot be resolved", name)
                    })
                } else {
                    schema
                };

                match schema {
                    Schema::Ref { .. } => {} // already resolved above
                    Schema::Boolean => {
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "bool".to_string());
                        if let Some(default) = default {
                            let default = self.parse_default(schema, gen_state, default)?;
                            d.insert(name_std.clone(), default);
                        }
                    }

                    Schema::Int | Schema::Date | Schema::TimeMillis => {
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "i32".to_string());
                        if let Some(default) = default {
                            let default = self.parse_default(schema, gen_state, default)?;
                            d.insert(name_std.clone(), default);
                        }
                    }

                    Schema::Long
                    | Schema::TimeMicros
                    | Schema::TimestampMillis
                    | Schema::TimestampMicros => {
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "i64".to_string());
                        if let Some(default) = default {
                            let default = self.parse_default(schema, gen_state, default)?;
                            d.insert(name_std.clone(), default);
                        }
                    }

                    Schema::Float => {
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "f32".to_string());
                        if let Some(default) = default {
                            let default = self.parse_default(schema, gen_state, default)?;
                            d.insert(name_std.clone(), default);
                        }
                    }

                    Schema::Double => {
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "f64".to_string());
                        if let Some(default) = default {
                            let default = self.parse_default(schema, gen_state, default)?;
                            d.insert(name_std.clone(), default);
                        }
                    }

                    Schema::Bytes => {
                        b.insert(name_std.clone());
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "Vec<u8>".to_string());
                        if let Some(default) = default {
                            let default = self.parse_default(schema, gen_state, default)?;
                            d.insert(name_std.clone(), default);
                        }
                    }

                    Schema::String => {
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "String".to_string());
                        if let Some(default) = default {
                            let default = self.parse_default(schema, gen_state, default)?;
                            d.insert(name_std.clone(), default);
                        }
                    }

                    Schema::Uuid => {
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "uuid::Uuid".to_string());
                        if let Some(default) = default {
                            let default = self.parse_default(schema, gen_state, default)?;
                            d.insert(name_std.clone(), default);
                        }
                    }

                    Schema::Duration => {
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "apache_avro::Duration".to_string());
                        if let Some(default) = default {
                            let default = self.parse_default(schema, gen_state, default)?;
                            d.insert(name_std.clone(), default);
                        }
                    }

                    Schema::Decimal { .. } => {
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "apache_avro::Decimal".to_string());
                        if let Some(default) = default {
                            let default = self.parse_default(schema, gen_state, default)?;
                            d.insert(name_std.clone(), default);
                        }
                    }

                    Schema::Fixed {
                        name: Name { name: f_name, .. },
                        ..
                    } => {
                        let f_name = sanitize(f_name.to_upper_camel_case());
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), f_name.clone());
                        if let Some(default) = default {
                            let default = self.parse_default(schema, gen_state, default)?;
                            d.insert(name_std.clone(), default);
                        }
                    }

                    Schema::Array(inner) => match inner.as_ref() {
                        Schema::Null => err!("Invalid use of Schema::Null")?,
                        _ => {
                            let type_str = array_type(inner, gen_state)?;
                            f.push(name_std.clone());
                            t.insert(name_std.clone(), type_str);
                            if let Some(default) = default {
                                let default = self.parse_default(schema, gen_state, default)?;
                                d.insert(name_std.clone(), default);
                            }
                        }
                    },

                    Schema::Map(inner) => match inner.as_ref() {
                        Schema::Null => err!("Invalid use of Schema::Null")?,
                        _ => {
                            let type_str = map_type(inner, gen_state)?;
                            f.push(name_std.clone());
                            t.insert(name_std.clone(), type_str);
                            if let Some(default) = default {
                                let default = self.parse_default(schema, gen_state, default)?;
                                d.insert(name_std.clone(), default);
                            }
                        }
                    },

                    Schema::Record {
                        name: Name { name: r_name, .. },
                        ..
                    } => {
                        let r_name = sanitize(r_name.to_upper_camel_case());
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), r_name.clone());
                        if let Some(default) = default {
                            let default = self.parse_default(schema, gen_state, default)?;
                            d.insert(name_std.clone(), default);
                        }
                    }

                    Schema::Enum {
                        name: Name { name: e_name, .. },
                        ..
                    } => {
                        let e_name = sanitize(e_name.to_upper_camel_case());
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), e_name);
                        if let Some(default) = default {
                            let default = self.parse_default(schema, gen_state, default)?;
                            d.insert(name_std.clone(), default);
                        }
                    }

                    Schema::Union(union) => {
                        let type_str = union_type(union, gen_state, true)?;
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), type_str);
                        if let Some(default) = default {
                            let default = self.parse_default(schema, gen_state, default)?;
                            d.insert(name_std.clone(), default);
                        }
                        union.variants().iter().any(|variant| {
                            if let Schema::Bytes = variant {
                                b.insert(name_std.clone());
                                return true;
                            }
                            false
                        });
                    }

                    Schema::Null => err!("Invalid use of Schema::Null")?,
                };
            }

            ctx.insert("fields", &f);
            ctx.insert("types", &t);
            ctx.insert("originals", &o);
            ctx.insert("defaults", &d);
            ctx.insert("bytes", &b);
            ctx.insert("is_eq_derivable", &gen_state.is_eq_derivable(schema));
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
            let mut visitors = vec![];
            for mut sc in schemas {
                // Resolve potentially nested schema ref
                while let Schema::Ref { ref name } = sc {
                    match gen_state.get_schema(name) {
                        Some(s) => sc = s,
                        None => err!("Schema reference '{:?}' cannot be resolved", name)?,
                    }
                }
                let symbol_str = match sc {
                    Schema::Ref { .. } => unreachable!(),
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
                        format!("{rec}({rec})", rec = name.to_upper_camel_case())
                    }
                    Schema::Enum {
                        name: Name { name, .. },
                        ..
                    } => {
                        format!("{e}({e})", e = sanitize(name.to_upper_camel_case()))
                    }
                    Schema::Fixed {
                        name: Name { name, .. },
                        ..
                    } => {
                        format!("{f}({f})", f = sanitize(name.to_upper_camel_case()))
                    }
                    Schema::Decimal { .. } => "Decimal(apache_avro::Decimal)".into(),
                    Schema::Uuid => "Uuid(uuid::Uuid)".into(),
                    Schema::Date => "Date(i32)".into(),
                    Schema::TimeMillis => "TimeMillis(i32)".into(),
                    Schema::TimeMicros => "TimeMicros(i64)".into(),
                    Schema::TimestampMillis => "TimestampMillis(i64)".into(),
                    Schema::TimestampMicros => "TimestampMicros(i64)".into(),
                    Schema::Duration => "Duration(apache_avro::Duration)".into(),
                    Schema::Null => err!(
                        "Invalid Schema::Null not in first position on an UnionSchema variants"
                    )?,
                };
                symbols.push(symbol_str);

                match sc {
                    Schema::Record {
                        name: Name { name, .. },
                        ..
                    } => visitors.push(GenUnionVisitor {
                        variant: name.to_upper_camel_case(),
                        rust_type: name.to_upper_camel_case(),
                        serde_visitor: None,
                    }),
                    Schema::Boolean => visitors.push(GenUnionVisitor {
                        variant: String::from("Boolean"),
                        rust_type: String::from("bool"),
                        serde_visitor: String::from("bool").into(),
                    }),
                    Schema::Int => visitors.push(GenUnionVisitor {
                        variant: String::from("Int"),
                        rust_type: String::from("i32"),
                        serde_visitor: String::from("i32").into(),
                    }),
                    Schema::Long => visitors.push(GenUnionVisitor {
                        variant: String::from("Long"),
                        rust_type: String::from("i64"),
                        serde_visitor: String::from("i64").into(),
                    }),
                    Schema::Float => visitors.push(GenUnionVisitor {
                        variant: String::from("Float"),
                        rust_type: String::from("f32"),
                        serde_visitor: String::from("f32").into(),
                    }),
                    Schema::Double => visitors.push(GenUnionVisitor {
                        variant: String::from("Double"),
                        rust_type: String::from("f64"),
                        serde_visitor: String::from("f64").into(),
                    }),
                    Schema::String => visitors.push(GenUnionVisitor {
                        variant: String::from("String"),
                        rust_type: String::from("String"),
                        serde_visitor: String::from("&str").into(),
                    }),
                    _ => (),
                };
            }

            let mut ctx = Context::new();
            ctx.insert("name", &e_name);
            ctx.insert("symbols", &symbols);
            ctx.insert("visitors", &visitors);
            ctx.insert("use_avro_rs_unions", &self.use_avro_rs_unions);
            ctx.insert("is_eq_derivable", &gen_state.is_eq_derivable(schema));

            Ok(self.tera.render(UNION_TERA, &ctx)?)
        } else {
            err!("Requires Schema::Union, found {:?}", schema)?
        }
    }

    fn parse_default(
        &self,
        schema: &Schema,
        gen_state: &GenState,
        default: &serde_json::Value,
    ) -> Result<String> {
        let default_str = match schema {
            Schema::Ref { name } => match gen_state.get_schema(name) {
                Some(s) => self.parse_default(s, gen_state, default)?,
                None => err!("Schema reference '{:?}' cannot be resolved", name)?,
            },

            Schema::Boolean => match default {
                Value::Bool(b) => b.to_string(),
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::Int | Schema::Date | Schema::TimeMillis => match default {
                Value::Number(n) if n.is_i64() => (n.as_i64().unwrap() as i32).to_string(),
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::Long
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros => match default {
                Value::Number(n) if n.is_i64() => n.to_string(),
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::Float => match default {
                Value::Number(n) if n.is_f64() => {
                    let n = n.as_f64().unwrap() as f32;
                    if n == n.ceil() {
                        format!("{:.1}", n)
                    } else {
                        format!("{:.*}", self.precision, n)
                    }
                }
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::Double => match default {
                Value::Number(n) if n.is_f64() => {
                    let n = n.as_f64().unwrap();
                    if n == n.ceil() {
                        format!("{:.1}", n)
                    } else {
                        format!("{:.*}", self.precision, n)
                    }
                }
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::Bytes => match default {
                Value::String(s) => {
                    let bytes = s.clone().into_bytes();
                    format!("vec!{:?}", bytes)
                }
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::String => match default {
                Value::String(s) => format!("\"{}\".to_owned()", s),
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::Uuid => match default {
                Value::String(s) => {
                    format!(
                        r#"uuid::Uuid::parse_str("{}").unwrap()"#,
                        Uuid::parse_str(s)?
                    )
                }
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::Duration => match default {
                Value::String(s) => {
                    let bytes = s.clone().into_bytes();
                    if bytes.len() != 12 {
                        err!("Invalid default: {:?}", bytes)?
                    }
                    format!("{:?}", bytes)
                }
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::Decimal { inner, .. } => match inner.as_ref() {
                Schema::Bytes => match default {
                    Value::String(s) => {
                        let bytes = s.clone().into_bytes();
                        format!("vec!{:?}", bytes)
                    }
                    _ => err!("Invalid default: {:?}", default)?,
                },
                Schema::Fixed { size, .. } => match default {
                    Value::String(s) => {
                        let bytes = s.clone().into_bytes();
                        if bytes.len() != *size {
                            err!("Invalid default: {:?}", bytes)?
                        }
                        format!("{:?}", bytes)
                    }
                    _ => err!("Invalid default: {:?}", default)?,
                },
                _ => err!("Invalid Decimal inner Schema: {:?}", inner)?,
            },

            Schema::Fixed { size, .. } => match default {
                Value::String(s) => {
                    let bytes = s.clone().into_bytes();
                    if bytes.len() != *size {
                        err!("Invalid default: {:?}", bytes)?
                    }
                    format!("{:?}", bytes)
                }
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
                let e_name = sanitize(e_name.to_upper_camel_case());
                let valids: HashSet<_> = symbols
                    .iter()
                    .map(|s| sanitize(s.to_upper_camel_case()))
                    .collect();
                match default {
                    Value::String(ref s) => {
                        let s = sanitize(s.to_upper_camel_case());
                        if valids.contains(&s) {
                            format!("{}::{}", e_name, sanitize(s.to_upper_camel_case()))
                        } else {
                            err!("Invalid default: {:?}", default)?
                        }
                    }
                    _ => err!("Invalid default: {:?}", default)?,
                }
            }

            Schema::Union(union) => self.union_default(union, gen_state, default)?,

            Schema::Null => err!("Invalid use of Schema::Null")?,
        };

        Ok(default_str)
    }

    /// Generates Rust default values for the inner schema of an Avro array.
    fn array_default(
        &self,
        inner: &Schema,
        gen_state: &GenState,
        default: &Value,
    ) -> Result<String> {
        if let Value::Array(vals) = default {
            let vals = vals
                .iter()
                .map(|d| self.parse_default(inner, gen_state, d))
                .collect::<Result<Vec<String>>>()?
                .as_slice()
                .join(", ");
            Ok(format!("vec![{}]", vals))
        } else {
            err!("Invalid default: {:?}, expected: Array", default)
        }
    }

    /// Generates Rust default values for the inner schema of an Avro map.
    fn map_default(&self, inner: &Schema, gen_state: &GenState, default: &Value) -> Result<String> {
        if let Value::Object(o) = default {
            if o.is_empty() {
                Ok("::std::collections::HashMap::new()".to_string())
            } else {
                let vals = o
                    .iter()
                    .map(|(k, v)| {
                        Ok(format!(
                            r#"m.insert("{}".to_owned(), {});"#,
                            k,
                            self.parse_default(inner, gen_state, v)?
                        ))
                    })
                    .collect::<Result<Vec<String>>>()?
                    .as_slice()
                    .join(" ");
                Ok(format!(
                    "{{ let mut m = ::std::collections::HashMap::new(); {} m }}",
                    vals
                ))
            }
        } else {
            err!("Invalid default: {:?}, expected: Map", default)
        }
    }

    /// Generates Rust default values for an Avro record
    fn record_default(
        &self,
        inner: &Schema,
        gen_state: &GenState,
        default: &Value,
    ) -> Result<String> {
        match inner {
            Schema::Record {
                name: Name { name, .. },
                fields,
                ..
            } => {
                let default_str = if let Value::Object(o) = default {
                    if !o.is_empty() {
                        let vals = fields
                            .iter()
                            .map(|rf| {
                                let f = sanitize(rf.name.to_snake_case());
                                let d = if let Some(v) = o.get(&rf.name) {
                                    self.parse_default(&rf.schema, gen_state, v)?
                                } else {
                                    format!("default_{}_{}()", name.to_lowercase(), f)
                                };
                                Ok(format!("{}: {},", f, d))
                            })
                            .collect::<Result<Vec<String>>>()?
                            .as_slice()
                            .join(" ");
                        format!("{} {{ {} }}", sanitize(name.to_upper_camel_case()), vals)
                    } else {
                        format!("{}::default()", sanitize(name.to_upper_camel_case()))
                    }
                } else {
                    err!("Invalid default: {:?}, expected: Object", default)?
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
        default: &Value,
    ) -> Result<String> {
        if union.is_nullable() {
            let default_str = match default {
                Value::Null => "None".into(),
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
        Schema::Ref { name } => match gen_state.get_schema(name) {
            Some(s) => array_type(s, gen_state)?,
            None => err!("Schema reference '{:?}' cannot be resolved", name)?,
        },
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
        Schema::Decimal { .. } => "Vec<apache_avro::Decimal>".into(),
        Schema::Duration { .. } => "Vec<apache_avro::Duration>".into(),

        Schema::Fixed {
            name: Name { name: f_name, .. },
            ..
        } => {
            let f_name = sanitize(f_name.to_upper_camel_case());
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
        } => format!("Vec<{}>", &sanitize(name.to_upper_camel_case())),

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
        Schema::Ref { name } => match gen_state.get_schema(name) {
            Some(s) => map_type(s, gen_state)?,
            None => err!("Schema reference '{:?}' cannot be resolved", name)?,
        },
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
        Schema::Decimal { .. } => map_of("apache_avro::Decimal"),
        Schema::Duration { .. } => map_of("apache_avro::Duration"),

        Schema::Fixed {
            name: Name { name: f_name, .. },
            ..
        } => {
            let f_name = sanitize(f_name.to_upper_camel_case());
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
        } => map_of(&sanitize(name.to_upper_camel_case())),

        Schema::Null => err!("Invalid use of Schema::Null")?,
    };
    Ok(type_str)
}

fn union_enum_variant(schema: &Schema, gen_state: &GenState) -> Result<String> {
    let variant_str = match schema {
        Schema::Ref { name } => match gen_state.get_schema(name) {
            Some(s) => union_enum_variant(s, gen_state)?,
            None => err!("Schema reference '{:?}' cannot be resolved", name)?,
        },
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
        } => name.to_upper_camel_case(),
        Schema::Enum {
            name: Name { name, .. },
            ..
        } => sanitize(name.to_upper_camel_case()),
        Schema::Fixed {
            name: Name { name, .. },
            ..
        } => sanitize(name.to_upper_camel_case()),

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

    if variants.is_empty() {
        err!("Invalid empty Schema::Union")?
    } else if variants.len() == 1 && variants[0] == Schema::Null {
        err!("Invalid Schema::Union of only Schema::Null")?
    }

    if union.is_nullable() && variants.len() == 2 {
        return option_type(&variants[1], gen_state);
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
        Ok(type_str)
    }
}

/// Generates the Rust type of the inner schema of an Avro optional union.
pub(crate) fn option_type(inner: &Schema, gen_state: &GenState) -> Result<String> {
    let type_str = match inner {
        Schema::Ref { name } => match gen_state.get_schema(name) {
            Some(s) => option_type(s, gen_state)?,
            None => err!("Schema reference '{:?}' cannot be resolved", name)?,
        },
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
        Schema::Decimal { .. } => "Option<apache_avro::Decimal>".into(),
        Schema::Duration { .. } => "Option<apache_avro::Duration>".into(),

        Schema::Fixed {
            name: Name { name: f_name, .. },
            ..
        } => {
            let f_name = sanitize(f_name.to_upper_camel_case());
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
        } => format!("Option<{}>", &sanitize(name.to_upper_camel_case())),

        Schema::Null => err!("Invalid use of Schema::Null")?,
    };
    Ok(type_str)
}
