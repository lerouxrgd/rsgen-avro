
/// Auto-generated type for unnamed Avro union variants.
#[derive(Debug, PartialEq, Clone, serde::Serialize)]
pub enum UnionStringLongDoubleBoolean {
    String(String),
    Long(i64),
    Double(f64),
    Boolean(bool),
}

impl From<String> for UnionStringLongDoubleBoolean {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

impl From<UnionStringLongDoubleBoolean> for Option<String> {
    fn from(v: UnionStringLongDoubleBoolean) -> Self {
        if let UnionStringLongDoubleBoolean::String(v) = v {
            Some(v)
        } else {
            None
        }
    }
}

impl From<i64> for UnionStringLongDoubleBoolean {
    fn from(v: i64) -> Self {
        Self::Long(v)
    }
}

impl From<UnionStringLongDoubleBoolean> for Option<i64> {
    fn from(v: UnionStringLongDoubleBoolean) -> Self {
        if let UnionStringLongDoubleBoolean::Long(v) = v {
            Some(v)
        } else {
            None
        }
    }
}

impl From<f64> for UnionStringLongDoubleBoolean {
    fn from(v: f64) -> Self {
        Self::Double(v)
    }
}

impl From<UnionStringLongDoubleBoolean> for Option<f64> {
    fn from(v: UnionStringLongDoubleBoolean) -> Self {
        if let UnionStringLongDoubleBoolean::Double(v) = v {
            Some(v)
        } else {
            None
        }
    }
}

impl From<bool> for UnionStringLongDoubleBoolean {
    fn from(v: bool) -> Self {
        Self::Boolean(v)
    }
}

impl From<UnionStringLongDoubleBoolean> for Option<bool> {
    fn from(v: UnionStringLongDoubleBoolean) -> Self {
        if let UnionStringLongDoubleBoolean::Boolean(v) = v {
            Some(v)
        } else {
            None
        }
    }
}

impl<'de> serde::Deserialize<'de> for UnionStringLongDoubleBoolean {
    fn deserialize<D>(deserializer: D) -> Result<UnionStringLongDoubleBoolean, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        /// Serde visitor for the auto-generated unnamed Avro union type.
        struct UnionStringLongDoubleBooleanVisitor;

        impl<'de> serde::de::Visitor<'de> for UnionStringLongDoubleBooleanVisitor {
            type Value = UnionStringLongDoubleBoolean;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a UnionStringLongDoubleBoolean")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(UnionStringLongDoubleBoolean::String(value.into()))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(UnionStringLongDoubleBoolean::Long(value.into()))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(UnionStringLongDoubleBoolean::Double(value.into()))
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(UnionStringLongDoubleBoolean::Boolean(value.into()))
            }
        }

        deserializer.deserialize_any(UnionStringLongDoubleBooleanVisitor)
    }
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Contact {
    pub extra: ::std::collections::HashMap<String, Option<UnionStringLongDoubleBoolean>>,
}
