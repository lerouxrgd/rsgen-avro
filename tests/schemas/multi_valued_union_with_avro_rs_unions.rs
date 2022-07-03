
/// Auto-generated type for unnamed Avro union variants.
#[derive(Debug, PartialEq, Clone, serde::Serialize)]
pub enum UnionStringLongDoubleBoolean {
    String(String),
    Long(i64),
    Double(f64),
    Boolean(bool),
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

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(UnionStringLongDoubleBoolean::Long(value))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(UnionStringLongDoubleBoolean::Double(value))
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(UnionStringLongDoubleBoolean::Boolean(value))
            }
        }

        deserializer.deserialize_any(UnionStringLongDoubleBooleanVisitor)
    }
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Contact {
    pub extra: ::std::collections::HashMap<String, Option<UnionStringLongDoubleBoolean>>,
}
