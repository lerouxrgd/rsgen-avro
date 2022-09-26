
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, serde::Deserialize, serde::Serialize)]
pub enum C {
    U,
    S,
    D,
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct B {
    pub pass: i64,
    pub direction: C,
    pub depth: f64,
}

/// Auto-generated type for unnamed Avro union variants.
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub enum UnionLongDoubleB {
    Long(i64),
    Double(f64),
    B(B),
}

impl From<i64> for UnionLongDoubleB {
    fn from(v: i64) -> Self {
        Self::Long(v)
    }
}

impl TryFrom<UnionLongDoubleB> for i64 {
    type Error = UnionLongDoubleB;

    fn try_from(v: UnionLongDoubleB) -> Result<Self, Self::Error> {
        if let UnionLongDoubleB::Long(v) = v {
            Ok(v)
        } else {
            Err(v)
        }
    }
}

impl From<f64> for UnionLongDoubleB {
    fn from(v: f64) -> Self {
        Self::Double(v)
    }
}

impl TryFrom<UnionLongDoubleB> for f64 {
    type Error = UnionLongDoubleB;

    fn try_from(v: UnionLongDoubleB) -> Result<Self, Self::Error> {
        if let UnionLongDoubleB::Double(v) = v {
            Ok(v)
        } else {
            Err(v)
        }
    }
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct A {
    pub item: Option<UnionLongDoubleB>,
}
