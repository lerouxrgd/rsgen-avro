
/// Auto-generated type for unnamed Avro union variants.
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub enum UnionStringLongDoubleBooleanBytes {
    String(String),
    Long(i64),
    Double(f64),
    Boolean(bool),
    Bytes(#[serde(with = "apache_avro::serde_avro_bytes")] Vec<u8>),
}

impl From<String> for UnionStringLongDoubleBooleanBytes {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

impl TryFrom<UnionStringLongDoubleBooleanBytes> for String {
    type Error = UnionStringLongDoubleBooleanBytes;

    fn try_from(v: UnionStringLongDoubleBooleanBytes) -> Result<Self, Self::Error> {
        if let UnionStringLongDoubleBooleanBytes::String(v) = v {
            Ok(v)
        } else {
            Err(v)
        }
    }
}

impl From<i64> for UnionStringLongDoubleBooleanBytes {
    fn from(v: i64) -> Self {
        Self::Long(v)
    }
}

impl TryFrom<UnionStringLongDoubleBooleanBytes> for i64 {
    type Error = UnionStringLongDoubleBooleanBytes;

    fn try_from(v: UnionStringLongDoubleBooleanBytes) -> Result<Self, Self::Error> {
        if let UnionStringLongDoubleBooleanBytes::Long(v) = v {
            Ok(v)
        } else {
            Err(v)
        }
    }
}

impl From<f64> for UnionStringLongDoubleBooleanBytes {
    fn from(v: f64) -> Self {
        Self::Double(v)
    }
}

impl TryFrom<UnionStringLongDoubleBooleanBytes> for f64 {
    type Error = UnionStringLongDoubleBooleanBytes;

    fn try_from(v: UnionStringLongDoubleBooleanBytes) -> Result<Self, Self::Error> {
        if let UnionStringLongDoubleBooleanBytes::Double(v) = v {
            Ok(v)
        } else {
            Err(v)
        }
    }
}

impl From<bool> for UnionStringLongDoubleBooleanBytes {
    fn from(v: bool) -> Self {
        Self::Boolean(v)
    }
}

impl TryFrom<UnionStringLongDoubleBooleanBytes> for bool {
    type Error = UnionStringLongDoubleBooleanBytes;

    fn try_from(v: UnionStringLongDoubleBooleanBytes) -> Result<Self, Self::Error> {
        if let UnionStringLongDoubleBooleanBytes::Boolean(v) = v {
            Ok(v)
        } else {
            Err(v)
        }
    }
}

impl From<Vec<u8>> for UnionStringLongDoubleBooleanBytes {
    fn from(v: Vec<u8>) -> Self {
        Self::Bytes(v)
    }
}

impl TryFrom<UnionStringLongDoubleBooleanBytes> for Vec<u8> {
    type Error = UnionStringLongDoubleBooleanBytes;

    fn try_from(v: UnionStringLongDoubleBooleanBytes) -> Result<Self, Self::Error> {
        if let UnionStringLongDoubleBooleanBytes::Bytes(v) = v {
            Ok(v)
        } else {
            Err(v)
        }
    }
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Contact {
    pub extra: ::std::collections::HashMap<String, Option<UnionStringLongDoubleBooleanBytes>>,
}
