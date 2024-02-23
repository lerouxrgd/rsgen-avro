
/// Auto-generated type for unnamed Avro union variants.
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub enum UnionLong {
    Long(i64),
}

impl From<i64> for UnionLong {
    fn from(v: i64) -> Self {
        Self::Long(v)
    }
}

impl From<UnionLong> for i64 {
    fn from(v: UnionLong) -> Self {
        let UnionLong::Long(v) = v;
        v
    }
}

#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Mono {
    #[serde(rename = "myField")]
    pub my_field: UnionLong,
}
