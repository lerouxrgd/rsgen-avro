
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Baz {
    pub d: f32,
}

#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Bar {
    pub c: i64,
}

/// Auto-generated type for unnamed Avro union variants.
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub enum UnionBarBaz {
    Bar(Bar),
    Baz(Baz),
}

impl From<Bar> for UnionBarBaz {
    fn from(v: Bar) -> Self {
        Self::Bar(v)
    }
}

impl TryFrom<UnionBarBaz> for Bar {
    type Error = UnionBarBaz;

    fn try_from(v: UnionBarBaz) -> Result<Self, Self::Error> {
        if let UnionBarBaz::Bar(v) = v {
            Ok(v)
        } else {
            Err(v)
        }
    }
}

impl From<Baz> for UnionBarBaz {
    fn from(v: Baz) -> Self {
        Self::Baz(v)
    }
}

impl TryFrom<UnionBarBaz> for Baz {
    type Error = UnionBarBaz;

    fn try_from(v: UnionBarBaz) -> Result<Self, Self::Error> {
        if let UnionBarBaz::Baz(v) = v {
            Ok(v)
        } else {
            Err(v)
        }
    }
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Foo {
    pub a: i32,
    pub b: UnionBarBaz,
}
