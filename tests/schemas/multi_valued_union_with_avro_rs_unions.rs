
/// Auto-generated type for unnamed Avro union variants.
#[derive(Debug, PartialEq, Clone, serde::Serialize)]
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

impl<'de> serde::Deserialize<'de> for UnionStringLongDoubleBooleanBytes {
    fn deserialize<D>(deserializer: D) -> Result<UnionStringLongDoubleBooleanBytes, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        /// Serde visitor for the auto-generated unnamed Avro union type.
        struct UnionStringLongDoubleBooleanBytesVisitor;

        impl<'de> serde::de::Visitor<'de> for UnionStringLongDoubleBooleanBytesVisitor {
            type Value = UnionStringLongDoubleBooleanBytes;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a UnionStringLongDoubleBooleanBytes")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(UnionStringLongDoubleBooleanBytes::String(value.into()))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(UnionStringLongDoubleBooleanBytes::Long(value.into()))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(UnionStringLongDoubleBooleanBytes::Double(value.into()))
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(UnionStringLongDoubleBooleanBytes::Boolean(value.into()))
            }

            fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(UnionStringLongDoubleBooleanBytes::Bytes(value.into()))
            }
        }

        deserializer.deserialize_any(UnionStringLongDoubleBooleanBytesVisitor)
    }
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Contact {
    pub extra: ::std::collections::HashMap<String, Option<UnionStringLongDoubleBooleanBytes>>,
}
