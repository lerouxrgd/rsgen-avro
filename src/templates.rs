//! Logic for templating Rust types and default values from Avro schema.

#![allow(clippy::try_err)]

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use apache_avro::Schema;
use apache_avro::schema::{
    ArraySchema, DecimalSchema, EnumSchema, FixedSchema, MapSchema, Name, RecordField,
    RecordSchema, UnionSchema,
};
use heck::{ToSnakeCase, ToUpperCamelCase};
use lazy_static::lazy_static;
use serde_json::Value;
use tera::{Context, Tera};

use crate::error::{Error, Result};

pub const RECORD_TERA: &str = "record.tera";
pub const RECORD_TEMPLATE: &str = r#"
{%- if doc %}
{%- set doc_lines = doc | split(pat="\n") %}
{%- for doc_line in doc_lines %}
/// {{ doc_line }}
{%- endfor %}
{%- endif %}
#[derive(Debug, PartialEq{%- if is_eq_derivable %}, Eq{%- endif %}, Clone, serde::Deserialize, serde::Serialize{%- if derive_builders %}, derive_builder::Builder {%- endif %}{%- if derive_schemas %}, apache_avro::AvroSchema {%- endif %}  {%- if extra_derives %}, {{ extra_derives}} {%- endif %})]
{%- if derive_builders %}
#[builder(setter(into))]
{%- endif %}
{%- if fields | length == defaults | length %}
#[serde(default)]
{%- endif %}
pub struct {{ name }} {
    {%- for f in fields %}
    {%- if docs[f] %}
    {%- set doc_lines = docs[f] | split(pat="\n") %}
    {%- for doc_line in doc_lines %}
    /// {{ doc_line }}
    {%- endfor %}
    {%- endif %}
    {%- set type = types[f] %}
    {%- if f != originals[f] and not f is starting_with("r#") %}
    #[serde(rename = "{{ originals[f] }}")]
    {%- endif %}
    {%- if nullable and not type is starting_with("Option") %}
    #[serde(deserialize_with = "nullable_{{ name|lower }}_{{ f }}")]
    {%- endif %}
    {%- if nullable and not type is starting_with("Option") and serde_with is containing(f) %}
    #[serde(serialize_with = "{{ serde_with[f] }}::serialize")]
    {%- endif %}
    {%- if not nullable and serde_with is containing(f) %}
    #[serde(with = "{{ serde_with[f] }}")]
    {%- endif %}
    {%- if defaults is containing(f) and not fields | length == defaults | length %}
    #[serde(default = "default_{{ name | lower }}_{{ f | lower | trim_start_matches(pat="r#") }}")]
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
    {%- if serde_with is containing(f) %}
    #[derive(serde::Deserialize)]
    struct Wrapper(#[serde(with = "{{ serde_with[f] }}")] {{ type }});
    let opt = Option::<Wrapper>::deserialize(deserializer)?.map(|w| w.0);
    {%- else %}
    let opt = Option::deserialize(deserializer)?;
    {%- endif %}
    Ok(opt.unwrap_or_else(|| default_{{ name | lower }}_{{ f | lower | trim_start_matches(pat="r#") }}() ))
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
#[derive(Debug, PartialEq{%- if is_eq_derivable %}, Eq{%- endif %}, Clone, serde::Deserialize, serde::Serialize)]
#[serde(remote = "Self")]
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
{% if visitors | length == 1 %}
impl From<{{ name }}> for {{ v.rust_type }} {
    fn from(v: {{ name }}) -> Self {
        let {{ name }}::{{ v.variant }}(v) = v;
        v
    }
}
{%- else %}
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
{%- endif %}
{%- endfor %}

impl serde::Serialize for {{ name }} {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        struct NewtypeVariantSerializer<S>(S);

        impl<S> serde::Serializer for NewtypeVariantSerializer<S>
        where
            S: serde::Serializer,
        {
            type Ok = S::Ok;
            type Error = S::Error;
            type SerializeSeq = serde::ser::Impossible<S::Ok, S::Error>;
            type SerializeTuple = serde::ser::Impossible<S::Ok, S::Error>;
            type SerializeTupleStruct = serde::ser::Impossible<S::Ok, S::Error>;
            type SerializeTupleVariant = serde::ser::Impossible<S::Ok, S::Error>;
            type SerializeMap = serde::ser::Impossible<S::Ok, S::Error>;
            type SerializeStruct = serde::ser::Impossible<S::Ok, S::Error>;
            type SerializeStructVariant = serde::ser::Impossible<S::Ok, S::Error>;
            fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_none(self) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_some<T: ?Sized + serde::Serialize>(self, _value: &T) -> Result<Self::Ok, Self::Error>{ unimplemented!() }
            fn serialize_unit(self) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_unit_variant(self ,_name: &'static str, _variant_index: u32, _variant: &'static str) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_newtype_struct<T: ?Sized + serde::Serialize>(self, _name: &'static str, _value: &T,) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_seq(self,_len: Option<usize>,) -> Result<Self::SerializeSeq, Self::Error> { unimplemented!() }
            fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> { unimplemented!() }
            fn serialize_tuple_struct(self,_name: &'static str,_len: usize) -> Result<Self::SerializeTupleStruct, Self::Error> { unimplemented!() }
            fn serialize_tuple_variant(self,_name: &'static str,_variant_index: u32,_variant: &'static str,_len: usize) -> Result<Self::SerializeTupleVariant, Self::Error> { unimplemented!() }
            fn serialize_map(self,_len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> { unimplemented!() }
            fn serialize_struct(self,_name: &'static str,_len: usize) -> Result<Self::SerializeStruct, Self::Error> { unimplemented!() }
            fn serialize_struct_variant(self,_name: &'static str,_variant_index: u32,_variant: &'static str,_len: usize) -> Result<Self::SerializeStructVariant, Self::Error> { unimplemented!() }
            fn serialize_newtype_variant<T: ?Sized + serde::Serialize>(
                self,
                _name: &'static str,
                _variant_index: u32,
                _variant: &'static str,
                value: &T,
            ) -> Result<Self::Ok, Self::Error> {
                value.serialize(self.0)
            }
        }

        Self::serialize(self, NewtypeVariantSerializer(serializer))
    }
}
{# #}
{%- if use_avro_rs_unions %}
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

            fn visit_{{ v.serde_visitor | replace(from="&[u8]", to="bytes") | trim_start_matches(pat="&") }}<E>(self, value: {{ v.serde_visitor }}) -> Result<Self::Value, E>
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
{%- else %}
impl<'de> serde::Deserialize<'de> for {{ name }} {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::deserialize(deserializer)
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
    static ref UNESCAPABLE: HashSet<String> = {
        let s: HashSet<_> = ["Self", "self", "super", "extern", "crate"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        s
    };
}

fn sanitize(mut s: String) -> String {
    if RESERVED.contains(&s) {
        if UNESCAPABLE.contains(&s) {
            s.push('_');
        }
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
    use_chrono_dates: bool,
}

impl GenState {
    pub fn new(deps: &[Schema]) -> Result<Self> {
        let schemata_by_name: HashMap<Name, Schema> = deps
            .iter()
            .filter_map(|s| match s {
                Schema::Record(RecordSchema { name, .. })
                | Schema::Fixed(FixedSchema { name, .. })
                | Schema::Enum(EnumSchema { name, .. }) => Some((name.clone(), s.clone())),
                _ => None,
            })
            .collect::<HashMap<_, _>>();
        let not_eq = Self::get_not_eq_schemata(deps, &schemata_by_name)?;
        Ok(GenState {
            types_by_schema: HashMap::new(),
            schemata_by_name,
            not_eq,
            use_chrono_dates: false,
        })
    }

    pub fn with_chrono_dates(mut self, use_chrono_dates: bool) -> Self {
        self.use_chrono_dates = use_chrono_dates;
        self
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

    /// Utility function to find nested type that does not implement Eq in record and
    /// its dependencies.
    fn deep_search_not_eq(
        schema: &Schema,
        schemata_by_name: &HashMap<Name, Schema>,
        inner_not_eq: &mut HashMap<Name, bool>,
        outer_not_eq: &mut HashMap<Name, bool>,
    ) -> Result<bool> {
        if let Some(name) = schema.name() {
            match inner_not_eq.entry(name.clone()) {
                Entry::Occupied(not_eq) => return Ok(*not_eq.get()),
                Entry::Vacant(not_eq) => {
                    not_eq.insert(false);
                }
            }
        }
        match schema {
            Schema::Array(ArraySchema { items: inner, .. })
            | Schema::Map(MapSchema { types: inner, .. }) => {
                let not_eq =
                    Self::deep_search_not_eq(inner, schemata_by_name, inner_not_eq, outer_not_eq)?;
                match schema.name() {
                    Some(schema_name) if not_eq => {
                        inner_not_eq.insert(schema_name.clone(), true);
                        outer_not_eq.insert(schema_name.clone(), true);
                    }
                    _ => {}
                }
                Ok(not_eq)
            }
            Schema::Record(RecordSchema { fields, .. }) => {
                for f in fields {
                    let not_eq = Self::deep_search_not_eq(
                        &f.schema,
                        schemata_by_name,
                        inner_not_eq,
                        outer_not_eq,
                    )?;

                    match schema.name() {
                        Some(schema_name) if not_eq => {
                            inner_not_eq.insert(schema_name.clone(), true);
                            outer_not_eq.insert(schema_name.clone(), true);
                        }
                        _ => {}
                    }

                    if not_eq {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Schema::Union(union) => {
                for s in union.variants() {
                    if Self::deep_search_not_eq(s, schemata_by_name, inner_not_eq, outer_not_eq)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Schema::Ref { name } => {
                let schema = schemata_by_name.get(name).ok_or_else(|| {
                    Error::Template(format!(
                        "Ref `{name:?}` is not resolved. Schema: {schema:?}",
                    ))
                })?;
                inner_not_eq.remove(name); // Force re-exploration of the ref schema
                outer_not_eq.remove(name); // Force re-exploration of the ref schema
                let not_eq =
                    Self::deep_search_not_eq(schema, schemata_by_name, inner_not_eq, outer_not_eq)?;
                match schema.name() {
                    Some(schema_name) if not_eq => {
                        inner_not_eq.insert(schema_name.clone(), true);
                        outer_not_eq.insert(schema_name.clone(), true);
                    }
                    _ => {}
                }
                Ok(not_eq)
            }
            Schema::Float | Schema::Double => Ok(true),
            _ => Ok(false),
        }
    }

    /// Fill HashSet with schemata that contains type which does not implement Eq
    fn get_not_eq_schemata(
        deps: &[Schema],
        schemata_by_name: &HashMap<Name, Schema>,
    ) -> Result<HashSet<String>> {
        let mut schemata_not_eq = HashSet::new();
        let mut outer_not_eq = HashMap::new();
        for dep in deps {
            let mut inner_not_eq = HashMap::new();
            let not_eq = Self::deep_search_not_eq(
                dep,
                schemata_by_name,
                &mut inner_not_eq,
                &mut outer_not_eq,
            )?;
            let not_eq = not_eq
                || inner_not_eq.values().any(|&not_eq| not_eq)
                || inner_not_eq
                    .keys()
                    .filter_map(|n| outer_not_eq.get(n))
                    .any(|&not_eq| not_eq);
            if not_eq {
                let str_schema = serde_json::to_string(dep).expect("Unexpected invalid schema");
                schemata_not_eq.insert(str_schema);
            }
        }
        Ok(schemata_not_eq)
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
    pub use_chrono_dates: bool,
    pub derive_builders: bool,
    pub derive_schemas: bool,
    pub extra_derives: Vec<String>,
}

impl Templater {
    /// Creates a new `Templater.`
    pub fn new() -> Result<Templater> {
        let dir = tempfile::tempdir()?;
        let mut tera = Tera::new(&format!("{}", dir.path().join("*").display()))?;

        tera.add_raw_template(RECORD_TERA, RECORD_TEMPLATE)?;
        tera.add_raw_template(ENUM_TERA, ENUM_TEMPLATE)?;
        tera.add_raw_template(FIXED_TERA, FIXED_TEMPLATE)?;
        tera.add_raw_template(UNION_TERA, UNION_TEMPLATE)?;

        Ok(Templater {
            tera,
            precision: 3,
            nullable: false,
            use_avro_rs_unions: false,
            use_chrono_dates: false,
            derive_builders: false,
            derive_schemas: false,
            extra_derives: vec![],
        })
    }

    /// Generates a Rust type based on a `Schema::Fixed` schema.
    pub fn str_fixed(&self, schema: &Schema) -> Result<String> {
        if let Schema::Fixed(FixedSchema {
            name: Name { name, .. },
            size,
            ..
        }) = schema
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
        if let Schema::Enum(EnumSchema {
            name: Name { name, .. },
            symbols,
            doc,
            ..
        }) = schema
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
        if let Schema::Record(RecordSchema {
            name: Name { name, .. },
            fields,
            doc,
            ..
        }) = schema
        {
            let mut ctx = Context::new();
            ctx.insert("name", &name.to_upper_camel_case());
            let doc = if let Some(d) = doc { d } else { "" };
            ctx.insert("doc", doc);
            ctx.insert("derive_builders", &self.derive_builders);
            ctx.insert("derive_schemas", &self.derive_schemas);
            if !self.extra_derives.is_empty() {
                ctx.insert("extra_derives", &self.extra_derives.join(", "));
            }

            let mut f = Vec::new(); // field names;
            let mut t = HashMap::new(); // field name -> field type
            let mut o = HashMap::new(); // field name -> original name
            let mut d = HashMap::new(); // field name -> default value
            let mut w = HashMap::new(); // field name -> serde with
            let mut c = HashMap::new(); // field name -> comment/doc

            let mut fields_by_pos = fields.iter().clone().collect::<Vec<_>>();
            fields_by_pos.sort_by_key(|f| f.position);

            for RecordField {
                schema,
                name,
                default,
                doc,
                ..
            } in fields_by_pos.iter()
            {
                let name_std = sanitize(name.to_snake_case());
                o.insert(name_std.clone(), name);
                if let Some(d) = doc {
                    c.insert(name_std.clone(), d);
                }

                let schema = if let Schema::Ref { name } = schema {
                    gen_state.get_schema(name).ok_or_else(|| {
                        Error::Template(format!("Schema reference '{name:?}' cannot be resolved"))
                    })?
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

                    Schema::Date if self.use_chrono_dates => {
                        f.push(name_std.clone());
                        t.insert(
                            name_std.clone(),
                            "chrono::DateTime<chrono::Utc>".to_string(),
                        );
                        w.insert(name_std.clone(), "chrono::serde::ts_seconds");
                        if let Some(default) = default {
                            let default = self.parse_default(schema, gen_state, default)?;
                            d.insert(name_std.clone(), default);
                        }
                    }

                    Schema::TimeMillis | Schema::TimestampMillis | Schema::LocalTimestampMillis
                        if self.use_chrono_dates =>
                    {
                        f.push(name_std.clone());
                        t.insert(
                            name_std.clone(),
                            "chrono::DateTime<chrono::Utc>".to_string(),
                        );
                        w.insert(name_std.clone(), "chrono::serde::ts_milliseconds");
                        if let Some(default) = default {
                            let default = self.parse_default(schema, gen_state, default)?;
                            d.insert(name_std.clone(), default);
                        }
                    }

                    Schema::TimeMicros | Schema::TimestampMicros | Schema::LocalTimestampMicros
                        if self.use_chrono_dates =>
                    {
                        f.push(name_std.clone());
                        t.insert(
                            name_std.clone(),
                            "chrono::DateTime<chrono::Utc>".to_string(),
                        );
                        w.insert(name_std.clone(), "chrono::serde::ts_microseconds");
                        if let Some(default) = default {
                            let default = self.parse_default(schema, gen_state, default)?;
                            d.insert(name_std.clone(), default);
                        }
                    }

                    Schema::TimestampNanos | Schema::LocalTimestampNanos
                        if self.use_chrono_dates =>
                    {
                        f.push(name_std.clone());
                        t.insert(
                            name_std.clone(),
                            "chrono::DateTime<chrono::Utc>".to_string(),
                        );
                        w.insert(name_std.clone(), "chrono::serde::ts_nanoseconds");
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
                    | Schema::LocalTimestampMillis
                    | Schema::TimestampMicros
                    | Schema::LocalTimestampMicros
                    | Schema::TimestampNanos
                    | Schema::LocalTimestampNanos => {
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
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "Vec<u8>".to_string());
                        w.insert(name_std.clone(), "apache_avro::serde_avro_bytes");
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
                        t.insert(name_std.clone(), "apache_avro::Uuid".to_string());
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

                    Schema::BigDecimal => {
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), "apache_avro::BigDecimal".to_string());
                        if let Some(default) = default {
                            let default = self.parse_default(schema, gen_state, default)?;
                            d.insert(name_std.clone(), default);
                        }
                    }

                    Schema::Fixed(FixedSchema {
                        name: Name { name: f_name, .. },
                        ..
                    }) => {
                        let f_name = sanitize(f_name.to_upper_camel_case());
                        f.push(name_std.clone());
                        w.insert(name_std.clone(), "apache_avro::serde_avro_fixed");
                        t.insert(name_std.clone(), f_name.clone());
                        if let Some(default) = default {
                            let default = self.parse_default(schema, gen_state, default)?;
                            d.insert(name_std.clone(), default);
                        }
                    }

                    Schema::Array(ArraySchema { items: inner, .. }) => match inner.as_ref() {
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

                    Schema::Map(MapSchema { types: inner, .. }) => match inner.as_ref() {
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

                    Schema::Record(RecordSchema {
                        name: Name { name: r_name, .. },
                        ..
                    }) => {
                        let r_name = sanitize(r_name.to_upper_camel_case());
                        f.push(name_std.clone());
                        t.insert(name_std.clone(), r_name.clone());
                        if let Some(default) = default {
                            let default = self.parse_default(schema, gen_state, default)?;
                            d.insert(name_std.clone(), default);
                        }
                    }

                    Schema::Enum(EnumSchema {
                        name: Name { name: e_name, .. },
                        ..
                    }) => {
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
                        if union.is_nullable()
                            && union.variants().len() == 2
                            && matches!(union.variants()[1], Schema::Bytes)
                        {
                            w.insert(name_std.clone(), "apache_avro::serde_avro_bytes_opt");
                        } else if union.is_nullable()
                            && union.variants().len() == 2
                            && matches!(union.variants()[1], Schema::Fixed(_))
                        {
                            w.insert(name_std.clone(), "apache_avro::serde_avro_fixed_opt");
                        } else if union.is_nullable()
                            && union.variants().len() == 2
                            && matches!(
                                union.variants()[1],
                                Schema::TimestampMillis | Schema::LocalTimestampMillis
                            )
                            && self.use_chrono_dates
                        {
                            w.insert(name_std.clone(), "chrono::serde::ts_milliseconds_option");
                        } else if union.is_nullable()
                            && union.variants().len() == 2
                            && matches!(
                                union.variants()[1],
                                Schema::TimestampMicros | Schema::LocalTimestampMicros
                            )
                            && self.use_chrono_dates
                        {
                            w.insert(name_std.clone(), "chrono::serde::ts_microseconds_option");
                        } else if union.is_nullable()
                            && union.variants().len() == 2
                            && matches!(
                                union.variants()[1],
                                Schema::TimestampNanos | Schema::LocalTimestampNanos
                            )
                            && self.use_chrono_dates
                        {
                            w.insert(name_std.clone(), "chrono::serde::ts_nanoseconds_option");
                        };
                    }

                    Schema::Null => err!("Invalid use of Schema::Null")?,
                };
            }

            ctx.insert("fields", &f);
            ctx.insert("types", &t);
            ctx.insert("originals", &o);
            ctx.insert("defaults", &d);
            ctx.insert("docs", &c);
            ctx.insert("serde_with", &w);
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
                while let Schema::Ref { name } = sc {
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
                    Schema::Bytes => {
                        r#"Bytes(#[serde(with = "apache_avro::serde_avro_bytes")] Vec<u8>)"#.into()
                    }
                    Schema::String => "String(String)".into(),
                    Schema::Array(ArraySchema { items: inner, .. }) => {
                        format!(
                            "Array{}({})",
                            union_enum_variant(inner.as_ref(), gen_state)?,
                            array_type(inner.as_ref(), gen_state)?
                        )
                    }
                    Schema::Map(MapSchema { types: inner, .. }) => format!(
                        "Map{}({})",
                        union_enum_variant(inner.as_ref(), gen_state)?,
                        map_type(sc, gen_state)?
                    ),
                    Schema::Union(union) => {
                        format!("{u}({u})", u = union_type(union, gen_state, false)?)
                    }
                    Schema::Record(RecordSchema {
                        name: Name { name, .. },
                        ..
                    }) => {
                        format!("{rec}({rec})", rec = name.to_upper_camel_case())
                    }
                    Schema::Enum(EnumSchema {
                        name: Name { name, .. },
                        ..
                    }) => {
                        format!("{e}({e})", e = sanitize(name.to_upper_camel_case()))
                    }
                    Schema::Fixed(FixedSchema {
                        name: Name { name, .. },
                        ..
                    }) => {
                        format!("{f}({f})", f = sanitize(name.to_upper_camel_case()))
                    }
                    Schema::Decimal { .. } => "Decimal(apache_avro::Decimal)".into(),
                    Schema::BigDecimal => "BigDecimal(apache_avro::BigDecimal)".into(),
                    Schema::Uuid => "Uuid(apache_avro::Uuid)".into(),
                    Schema::Date
                    | Schema::TimeMillis
                    | Schema::TimeMicros
                    | Schema::TimestampMillis
                    | Schema::TimestampMicros
                    | Schema::TimestampNanos
                    | Schema::LocalTimestampMillis
                    | Schema::LocalTimestampMicros
                    | Schema::LocalTimestampNanos
                        if self.use_chrono_dates =>
                    {
                        "NaiveDateTime(chrono::DateTime<chrono::Utc>)".into()
                    }
                    Schema::Date => "Date(i32)".into(),
                    Schema::TimeMillis => "TimeMillis(i32)".into(),
                    Schema::TimeMicros => "TimeMicros(i64)".into(),
                    Schema::TimestampMillis => "TimestampMillis(i64)".into(),
                    Schema::TimestampMicros => "TimestampMicros(i64)".into(),
                    Schema::TimestampNanos => "TimestampNanos(i64)".into(),
                    Schema::LocalTimestampMillis => "LocalTimestampMillis(i64)".into(),
                    Schema::LocalTimestampMicros => "LocalTimestampMicros(i64)".into(),
                    Schema::LocalTimestampNanos => "LocalTimestampNanos(i64)".into(),
                    Schema::Duration => "Duration(apache_avro::Duration)".into(),
                    Schema::Null => err!(
                        "Invalid Schema::Null not in first position on an UnionSchema variants"
                    )?,
                };
                symbols.push(symbol_str);

                match sc {
                    Schema::Record(RecordSchema {
                        name: Name { name, .. },
                        ..
                    }) => visitors.push(GenUnionVisitor {
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
                    Schema::Bytes => visitors.push(GenUnionVisitor {
                        variant: String::from("Bytes"),
                        rust_type: String::from("Vec<u8>"),
                        serde_visitor: String::from("&[u8]").into(),
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

            Schema::Date if self.use_chrono_dates => match default {
                Value::Number(n) if n.is_i64() => format!(
                    "chrono::DateTime::<chrono::Utc>::from_timestamp({}, 0).unwrap()",
                    n.as_i64().unwrap()
                ),
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::TimeMillis | Schema::TimestampMillis | Schema::LocalTimestampMillis
                if self.use_chrono_dates =>
            {
                match default {
                    Value::Number(n) if n.is_i64() => format!(
                        "chrono::DateTime::<chrono::Utc>::from_timestamp_millis({}).unwrap()",
                        n.as_i64().unwrap()
                    ),
                    _ => err!("Invalid default: {:?}", default)?,
                }
            }

            Schema::TimeMicros | Schema::TimestampMicros | Schema::LocalTimestampMicros
                if self.use_chrono_dates =>
            {
                match default {
                    Value::Number(n) if n.is_i64() => format!(
                        "chrono::DateTime::<chrono::Utc>::from_timestamp_micros({}).unwrap()",
                        n.as_i64().unwrap()
                    ),
                    _ => err!("Invalid default: {:?}", default)?,
                }
            }

            Schema::TimestampNanos | Schema::LocalTimestampNanos if self.use_chrono_dates => {
                match default {
                    Value::Number(n) if n.is_i64() => format!(
                        "chrono::DateTime::<chrono::Utc>::from_timestamp_nanos({}).unwrap()",
                        n.as_i64().unwrap()
                    ),
                    _ => err!("Invalid default: {:?}", default)?,
                }
            }

            Schema::Int | Schema::Date | Schema::TimeMillis => match default {
                Value::Number(n) if n.is_i64() => (n.as_i64().unwrap() as i32).to_string(),
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::Long
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => match default {
                Value::Number(n) if n.is_i64() => n.to_string(),
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::Float => match default {
                Value::Number(n) if n.is_f64() => {
                    let n = n.as_f64().unwrap() as f32;
                    if n == n.ceil() {
                        format!("{n:.1}")
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
                        format!("{n:.1}")
                    } else {
                        format!("{:.*}", self.precision, n)
                    }
                }
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::Bytes => match default {
                Value::String(s) => {
                    let bytes = s.clone().into_bytes();
                    format!("vec!{bytes:?}")
                }
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::String => match default {
                Value::String(s) => format!("\"{s}\".to_owned()"),
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::Uuid => match default {
                Value::String(s) => {
                    format!(
                        r#"apache_avro::Uuid::parse_str("{}").unwrap()"#,
                        apache_avro::Uuid::parse_str(s)
                            .map_err(|e| Error::Template(e.to_string()))?
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
                    format!("{bytes:?}")
                }
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::Decimal(DecimalSchema { inner, .. }) => match inner.as_ref() {
                Schema::Bytes => match default {
                    Value::String(s) => {
                        let bytes = s.clone().into_bytes();
                        format!("vec!{bytes:?}")
                    }
                    _ => err!("Invalid default: {:?}", default)?,
                },
                Schema::Fixed(FixedSchema { size, .. }) => match default {
                    Value::String(s) => {
                        let bytes = s.clone().into_bytes();
                        if bytes.len() != *size {
                            err!("Invalid default: {:?}", bytes)?
                        }
                        format!("{bytes:?}")
                    }
                    _ => err!("Invalid default: {:?}", default)?,
                },
                _ => err!("Invalid Decimal inner Schema: {:?}", inner)?,
            },

            Schema::BigDecimal => match default {
                Value::String(s) => {
                    format!(r#"apache_avro::BigDecimal::parse_str("{s}").unwrap()"#)
                }
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::Fixed(FixedSchema { size, .. }) => match default {
                Value::String(s) => {
                    let bytes = s.clone().into_bytes();
                    if bytes.len() != *size {
                        err!("Invalid default: {:?}", bytes)?
                    }
                    format!("{bytes:?}")
                }
                _ => err!("Invalid default: {:?}", default)?,
            },

            Schema::Array(ArraySchema { items: inner, .. }) => match inner.as_ref() {
                Schema::Null => err!("Invalid use of Schema::Null")?,
                _ => self.array_default(inner, gen_state, default)?,
            },

            Schema::Map(MapSchema { types: inner, .. }) => match inner.as_ref() {
                Schema::Null => err!("Invalid use of Schema::Null")?,
                _ => self.map_default(inner, gen_state, default)?,
            },

            Schema::Record { .. } => self.record_default(schema, gen_state, default)?,

            Schema::Enum(EnumSchema {
                name: Name { name: e_name, .. },
                symbols,
                ..
            }) => {
                let e_name = sanitize(e_name.to_upper_camel_case());
                let valids: HashSet<_> = symbols
                    .iter()
                    .map(|s| sanitize(s.to_upper_camel_case()))
                    .collect();
                match default {
                    Value::String(s) => {
                        let s = sanitize(s.to_upper_camel_case());
                        if valids.contains(&s) {
                            format!("{e_name}::{s}")
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
            Ok(format!("vec![{vals}]"))
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
                    "{{ let mut m = ::std::collections::HashMap::new(); {vals} m }}"
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
            Schema::Record(RecordSchema {
                name: Name { name, .. },
                fields,
                ..
            }) => {
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
                                Ok(format!("{f}: {d},"))
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
            Ok(format!("{e_name}::{e_variant}({default_str})"))
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

        Schema::Date
        | Schema::TimeMillis
        | Schema::TimeMicros
        | Schema::TimestampMillis
        | Schema::TimestampMicros
        | Schema::TimestampNanos
        | Schema::LocalTimestampMillis
        | Schema::LocalTimestampMicros
        | Schema::LocalTimestampNanos
            if gen_state.use_chrono_dates =>
        {
            "Vec<chrono::DateTime<chrono::Utc>>".into()
        }

        Schema::Date | Schema::TimeMillis => "Vec<i32>".into(),
        Schema::TimeMicros
        | Schema::TimestampMillis
        | Schema::TimestampMicros
        | Schema::TimestampNanos
        | Schema::LocalTimestampMillis
        | Schema::LocalTimestampMicros
        | Schema::LocalTimestampNanos => "Vec<i64>".into(),

        Schema::Uuid => "Vec<apache_avro::Uuid>".into(),
        Schema::Decimal { .. } => "Vec<apache_avro::Decimal>".into(),
        Schema::BigDecimal => "Vec<apache_avro::BigDecimal>".into(),
        Schema::Duration => "Vec<apache_avro::Duration>".into(),

        Schema::Fixed(FixedSchema {
            name: Name { name: f_name, .. },
            ..
        }) => {
            let f_name = sanitize(f_name.to_upper_camel_case());
            format!("Vec<{f_name}>")
        }

        Schema::Array(..) | Schema::Map(..) | Schema::Union(..) => {
            let nested_type = gen_state.get_type(inner).ok_or_else(|| {
                Error::Template(format!(
                    "Didn't find schema {:?} in state {:?}",
                    inner, &gen_state
                ))
            })?;
            format!("Vec<{nested_type}>")
        }

        Schema::Record(RecordSchema {
            name: Name { name, .. },
            ..
        })
        | Schema::Enum(EnumSchema {
            name: Name { name, .. },
            ..
        }) => format!("Vec<{}>", &sanitize(name.to_upper_camel_case())),

        Schema::Null => err!("Invalid use of Schema::Null")?,
    };
    Ok(type_str)
}

/// Generates the Rust type of the inner schema of an Avro map.
pub(crate) fn map_type(inner: &Schema, gen_state: &GenState) -> Result<String> {
    fn map_of(t: &str) -> String {
        format!("::std::collections::HashMap<String, {t}>")
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

        Schema::Date
        | Schema::TimeMillis
        | Schema::TimeMicros
        | Schema::TimestampMillis
        | Schema::TimestampMicros
        | Schema::LocalTimestampMillis
        | Schema::LocalTimestampMicros
            if gen_state.use_chrono_dates =>
        {
            map_of("chrono::DateTime<chrono::Utc>")
        }

        Schema::Date | Schema::TimeMillis => map_of("i32"),
        Schema::TimeMicros
        | Schema::TimestampMillis
        | Schema::TimestampMicros
        | Schema::TimestampNanos
        | Schema::LocalTimestampMillis
        | Schema::LocalTimestampMicros
        | Schema::LocalTimestampNanos => map_of("i64"),

        Schema::Uuid => map_of("apache_avro::Uuid"),
        Schema::Decimal { .. } => map_of("apache_avro::Decimal"),
        Schema::BigDecimal => map_of("apache_avro::BigDecimal"),
        Schema::Duration => map_of("apache_avro::Duration"),

        Schema::Fixed(FixedSchema {
            name: Name { name: f_name, .. },
            ..
        }) => {
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

        Schema::Record(RecordSchema {
            name: Name { name, .. },
            ..
        })
        | Schema::Enum(EnumSchema {
            name: Name { name, .. },
            ..
        }) => map_of(&sanitize(name.to_upper_camel_case())),

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
        Schema::Array(ArraySchema { items: inner, .. }) => {
            format!("Array{}", union_enum_variant(inner.as_ref(), gen_state)?)
        }
        Schema::Map(MapSchema { types: inner, .. }) => {
            format!("Map{}", union_enum_variant(inner.as_ref(), gen_state)?)
        }
        Schema::Union(union) => union_type(union, gen_state, false)?,
        Schema::Record(RecordSchema {
            name: Name { name, .. },
            ..
        }) => name.to_upper_camel_case(),
        Schema::Enum(EnumSchema {
            name: Name { name, .. },
            ..
        }) => sanitize(name.to_upper_camel_case()),
        Schema::Fixed(FixedSchema {
            name: Name { name, .. },
            ..
        }) => sanitize(name.to_upper_camel_case()),

        Schema::Decimal { .. } => "Decimal".into(),
        Schema::BigDecimal => "BigDecimal".into(),
        Schema::Uuid => "Uuid".into(),
        Schema::Date => "Date".into(),
        Schema::TimeMillis => "TimeMillis".into(),
        Schema::TimeMicros => "TimeMicros".into(),
        Schema::TimestampMillis => "TimestampMillis".into(),
        Schema::TimestampMicros => "TimestampMicros".into(),
        Schema::TimestampNanos => "TimestampNanos".into(),
        Schema::LocalTimestampMillis => "LocalTimestampMillis".into(),
        Schema::LocalTimestampMicros => "LocalTimestampMicros".into(),
        Schema::LocalTimestampNanos => "LocalTimestampNanos".into(),
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
        Ok(format!("Option<{type_str}>"))
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

        Schema::Date
        | Schema::TimeMillis
        | Schema::TimeMicros
        | Schema::TimestampMillis
        | Schema::TimestampMicros
        | Schema::TimestampNanos
        | Schema::LocalTimestampMillis
        | Schema::LocalTimestampMicros
        | Schema::LocalTimestampNanos
            if gen_state.use_chrono_dates =>
        {
            "Option<chrono::DateTime<chrono::Utc>>".into()
        }
        Schema::Date | Schema::TimeMillis => "Option<i32>".into(),
        Schema::TimeMicros
        | Schema::TimestampMillis
        | Schema::TimestampMicros
        | Schema::TimestampNanos
        | Schema::LocalTimestampMillis
        | Schema::LocalTimestampMicros
        | Schema::LocalTimestampNanos => "Option<i64>".into(),

        Schema::Uuid => "Option<apache_avro::Uuid>".into(),
        Schema::Decimal { .. } => "Option<apache_avro::Decimal>".into(),
        Schema::BigDecimal => "Option<apache_avro::BigDecimal>".into(),
        Schema::Duration => "Option<apache_avro::Duration>".into(),

        Schema::Fixed(FixedSchema {
            name: Name { name: f_name, .. },
            ..
        }) => {
            let f_name = sanitize(f_name.to_upper_camel_case());
            format!("Option<{f_name}>")
        }

        Schema::Array(..) | Schema::Map(..) | Schema::Union(..) => {
            let nested_type = gen_state.get_type(inner).ok_or_else(|| {
                Error::Template(format!(
                    "Didn't find schema {:?} in state {:?}",
                    inner, &gen_state
                ))
            })?;
            format!("Option<{nested_type}>")
        }

        Schema::Record(rec) => {
            let schema = Schema::Record(rec.clone());
            if find_recursion(&rec.name, &schema) {
                format!(
                    "Option<Box<{}>>",
                    &sanitize(rec.name.name.to_upper_camel_case())
                )
            } else {
                format!("Option<{}>", &sanitize(rec.name.name.to_upper_camel_case()))
            }
        }
        Schema::Enum(EnumSchema {
            name: Name { name, .. },
            ..
        }) => format!("Option<{}>", &sanitize(name.to_upper_camel_case())),

        Schema::Null => err!("Invalid use of Schema::Null")?,
    };
    Ok(type_str)
}

fn find_recursion(name: &Name, schema: &Schema) -> bool {
    match schema {
        Schema::Record(rec) => {
            for field in rec.fields.iter() {
                if find_recursion(name, &field.schema) {
                    return true;
                }
            }
        }
        Schema::Union(scm) => {
            for variant in scm.variants().iter() {
                if find_recursion(name, variant) {
                    return true;
                }
            }
        }
        Schema::Ref { name: r_name } => {
            if r_name == name {
                return true;
            }
        }
        _ => {}
    }
    false
}
