
/// Auto-generated type for unnamed Avro union variants.
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize, variant_access_derive::VariantAccess)]
pub enum UnionStringLongDoubleBoolean {
    String(String),
    Long(i64),
    Double(f64),
    Boolean(bool),
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Contact {
    pub extra: ::std::collections::HashMap<String, Option<UnionStringLongDoubleBoolean>>,
}
