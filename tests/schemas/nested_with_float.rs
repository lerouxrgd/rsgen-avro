
/// Auto-generated type for unnamed Avro union variants.
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub enum UnionIntLong {
    Int(i32),
    Long(i64),
}

impl From<i32> for UnionIntLong {
    fn from(v: i32) -> Self {
        Self::Int(v)
    }
}

impl TryFrom<UnionIntLong> for i32 {
    type Error = UnionIntLong;

    fn try_from(v: UnionIntLong) -> Result<Self, Self::Error> {
        if let UnionIntLong::Int(v) = v {
            Ok(v)
        } else {
            Err(v)
        }
    }
}

impl From<i64> for UnionIntLong {
    fn from(v: i64) -> Self {
        Self::Long(v)
    }
}

impl TryFrom<UnionIntLong> for i64 {
    type Error = UnionIntLong;

    fn try_from(v: UnionIntLong) -> Result<Self, Self::Error> {
        if let UnionIntLong::Long(v) = v {
            Ok(v)
        } else {
            Err(v)
        }
    }
}

/// Auto-generated type for unnamed Avro union variants.
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub enum UnionFloatInt {
    Float(f32),
    Int(i32),
}

impl From<f32> for UnionFloatInt {
    fn from(v: f32) -> Self {
        Self::Float(v)
    }
}

impl TryFrom<UnionFloatInt> for f32 {
    type Error = UnionFloatInt;

    fn try_from(v: UnionFloatInt) -> Result<Self, Self::Error> {
        if let UnionFloatInt::Float(v) = v {
            Ok(v)
        } else {
            Err(v)
        }
    }
}

impl From<i32> for UnionFloatInt {
    fn from(v: i32) -> Self {
        Self::Int(v)
    }
}

impl TryFrom<UnionFloatInt> for i32 {
    type Error = UnionFloatInt;

    fn try_from(v: UnionFloatInt) -> Result<Self, Self::Error> {
        if let UnionFloatInt::Int(v) = v {
            Ok(v)
        } else {
            Err(v)
        }
    }
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct FooBar {
    pub test_vec: Vec<f32>,
    pub float_union: UnionFloatInt,
    pub int_union: UnionIntLong,
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct FooFoo {
    pub test_map: ::std::collections::HashMap<String, f32>,
}

#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct BarFoo {
    pub nested_int: i32,
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Baz {
    #[serde(rename = "FooFoo")]
    pub foo_foo: FooFoo,
    #[serde(rename = "FooBar")]
    pub foo_bar: FooBar,
}

#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct FooBaz {
    #[serde(rename = "BarFoo")]
    pub bar_foo: BarFoo,
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Bar {
    #[serde(rename = "Baz")]
    pub baz: Baz,
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Foo {
    #[serde(rename = "Bar")]
    pub bar: Bar,
    #[serde(rename = "FooBaz")]
    pub foo_baz: FooBaz,
}
