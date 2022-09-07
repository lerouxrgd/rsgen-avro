
/// Auto-generated type for unnamed Avro union variants.
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub enum UnionLongInt {
    Long(i64),
    Int(i32),
}

impl From<i64> for UnionLongInt {
    fn from(v: i64) -> Self {
        Self::Long(v)
    }
}

impl TryFrom<UnionLongInt> for i64 {
    type Error = UnionLongInt;

    fn try_from(v: UnionLongInt) -> Result<Self, Self::Error> {
        if let UnionLongInt::Long(v) = v {
            Ok(v)
        } else {
            Err(v)
        }
    }
}

impl From<i32> for UnionLongInt {
    fn from(v: i32) -> Self {
        Self::Int(v)
    }
}

impl TryFrom<UnionLongInt> for i32 {
    type Error = UnionLongInt;

    fn try_from(v: UnionLongInt) -> Result<Self, Self::Error> {
        if let UnionLongInt::Int(v) = v {
            Ok(v)
        } else {
            Err(v)
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Test {
    #[serde(deserialize_with = "nullable_test_a")]
    #[serde(default = "default_test_a")]
    pub a: i64,
    #[serde(rename = "b-b", deserialize_with = "nullable_test_b_b")]
    #[serde(default = "default_test_b_b")]
    pub b_b: String,
    #[serde(default = "default_test_c")]
    pub c: Option<i32>,
    #[serde(default = "default_test_d")]
    pub d: Option<i32>,
    pub e: Option<UnionLongInt>,
}

#[inline(always)]
fn nullable_test_a<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_else(|| 42))
}

#[inline(always)]
fn nullable_test_b_b<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_else(|| "na".to_owned()))
}

#[inline(always)]
fn default_test_a() -> i64 { 42 }

#[inline(always)]
fn default_test_b_b() -> String { "na".to_owned() }

#[inline(always)]
fn default_test_c() -> Option<i32> { None }

#[inline(always)]
fn default_test_d() -> Option<i32> { Some(123) }
