
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

impl TryFrom<UnionStringLongDoubleBoolean> for String {
    type Error = UnionStringLongDoubleBoolean;

    fn try_from(v: UnionStringLongDoubleBoolean) -> Result<Self, Self::Error> {
        if let UnionStringLongDoubleBoolean::String(v) = v {
            Ok(v)
        } else {
            Err(v)
        }
    }
}

impl From<i64> for UnionStringLongDoubleBoolean {
    fn from(v: i64) -> Self {
        Self::Long(v)
    }
}

impl TryFrom<UnionStringLongDoubleBoolean> for i64 {
    type Error = UnionStringLongDoubleBoolean;

    fn try_from(v: UnionStringLongDoubleBoolean) -> Result<Self, Self::Error> {
        if let UnionStringLongDoubleBoolean::Long(v) = v {
            Ok(v)
        } else {
            Err(v)
        }
    }
}

impl From<f64> for UnionStringLongDoubleBoolean {
    fn from(v: f64) -> Self {
        Self::Double(v)
    }
}

impl TryFrom<UnionStringLongDoubleBoolean> for f64 {
    type Error = UnionStringLongDoubleBoolean;

    fn try_from(v: UnionStringLongDoubleBoolean) -> Result<Self, Self::Error> {
        if let UnionStringLongDoubleBoolean::Double(v) = v {
            Ok(v)
        } else {
            Err(v)
        }
    }
}

impl From<bool> for UnionStringLongDoubleBoolean {
    fn from(v: bool) -> Self {
        Self::Boolean(v)
    }
}

impl TryFrom<UnionStringLongDoubleBoolean> for bool {
    type Error = UnionStringLongDoubleBoolean;

    fn try_from(v: UnionStringLongDoubleBoolean) -> Result<Self, Self::Error> {
        if let UnionStringLongDoubleBoolean::Boolean(v) = v {
            Ok(v)
        } else {
            Err(v)
        }
    }
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Contact {
    pub extra: ::std::collections::HashMap<String, Option<UnionStringLongDoubleBoolean>>,
}
