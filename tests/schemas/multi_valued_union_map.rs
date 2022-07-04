
/// Auto-generated type for unnamed Avro union variants.
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
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

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Contact {
    pub extra: ::std::collections::HashMap<String, Option<UnionStringLongDoubleBoolean>>,
}
