
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Node {
    pub label: String,
    pub children: Vec<Node>,
}

#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Foo {
    pub label: String,
}

pub type Md5 = [u8; 16];

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, serde::Deserialize, serde::Serialize)]
pub enum Kind {
    A,
    B,
    C,
}

/// Auto-generated type for unnamed Avro union variants.
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub enum UnionBooleanDoubleArrayBytes {
    Boolean(bool),
    Double(f64),
    ArrayBytes(Vec<Vec<u8>>),
}

impl From<bool> for UnionBooleanDoubleArrayBytes {
    fn from(v: bool) -> Self {
        Self::Boolean(v)
    }
}

impl TryFrom<UnionBooleanDoubleArrayBytes> for bool {
    type Error = UnionBooleanDoubleArrayBytes;

    fn try_from(v: UnionBooleanDoubleArrayBytes) -> Result<Self, Self::Error> {
        if let UnionBooleanDoubleArrayBytes::Boolean(v) = v {
            Ok(v)
        } else {
            Err(v)
        }
    }
}

impl From<f64> for UnionBooleanDoubleArrayBytes {
    fn from(v: f64) -> Self {
        Self::Double(v)
    }
}

impl TryFrom<UnionBooleanDoubleArrayBytes> for f64 {
    type Error = UnionBooleanDoubleArrayBytes;

    fn try_from(v: UnionBooleanDoubleArrayBytes) -> Result<Self, Self::Error> {
        if let UnionBooleanDoubleArrayBytes::Double(v) = v {
            Ok(v)
        } else {
            Err(v)
        }
    }
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Interop {
    #[serde(rename = "intField")]
    pub int_field: i32,
    #[serde(rename = "longField")]
    pub long_field: i64,
    #[serde(rename = "stringField")]
    pub string_field: String,
    #[serde(rename = "boolField")]
    pub bool_field: bool,
    #[serde(rename = "floatField")]
    pub float_field: f32,
    #[serde(rename = "doubleField")]
    pub double_field: f64,
    #[serde(rename = "bytesField")]
    #[serde(with = "serde_bytes")]
    pub bytes_field: Vec<u8>,
    #[serde(rename = "arrayField")]
    pub array_field: Vec<f64>,
    #[serde(rename = "mapField")]
    pub map_field: ::std::collections::HashMap<String, Foo>,
    #[serde(rename = "unionField")]
    pub union_field: UnionBooleanDoubleArrayBytes,
    #[serde(rename = "enumField")]
    pub enum_field: Kind,
    #[serde(rename = "fixedField")]
    pub fixed_field: Md5,
    #[serde(rename = "recordField")]
    pub record_field: Node,
}
