
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Copy, serde::Deserialize, serde::Serialize)]
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
#[serde(remote = "Self")]
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

impl From<B> for UnionLongDoubleB {
    fn from(v: B) -> Self {
        Self::B(v)
    }
}

impl TryFrom<UnionLongDoubleB> for B {
    type Error = UnionLongDoubleB;

    fn try_from(v: UnionLongDoubleB) -> Result<Self, Self::Error> {
        if let UnionLongDoubleB::B(v) = v {
            Ok(v)
        } else {
            Err(v)
        }
    }
}

impl serde::Serialize for UnionLongDoubleB {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        struct NewtypeVariantSerializer<S>(S);

        impl<S> serde::Serializer for NewtypeVariantSerializer<S>
        where
            S: serde::Serializer,
        {
            type Ok = S::Ok;
            type Error = S::Error;
            type SerializeSeq = serde::ser::Impossible<S::Ok, S::Error>;
            type SerializeTuple = serde::ser::Impossible<S::Ok, S::Error>;
            type SerializeTupleStruct = serde::ser::Impossible<S::Ok, S::Error>;
            type SerializeTupleVariant = serde::ser::Impossible<S::Ok, S::Error>;
            type SerializeMap = serde::ser::Impossible<S::Ok, S::Error>;
            type SerializeStruct = serde::ser::Impossible<S::Ok, S::Error>;
            type SerializeStructVariant = serde::ser::Impossible<S::Ok, S::Error>;
            fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_none(self) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_some<T: ?Sized + serde::Serialize>(self, _value: &T) -> Result<Self::Ok, Self::Error>{ unimplemented!() }
            fn serialize_unit(self) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_unit_variant(self ,_name: &'static str, _variant_index: u32, _variant: &'static str) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_newtype_struct<T: ?Sized + serde::Serialize>(self, _name: &'static str, _value: &T,) -> Result<Self::Ok, Self::Error> { unimplemented!() }
            fn serialize_seq(self,_len: Option<usize>,) -> Result<Self::SerializeSeq, Self::Error> { unimplemented!() }
            fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> { unimplemented!() }
            fn serialize_tuple_struct(self,_name: &'static str,_len: usize) -> Result<Self::SerializeTupleStruct, Self::Error> { unimplemented!() }
            fn serialize_tuple_variant(self,_name: &'static str,_variant_index: u32,_variant: &'static str,_len: usize) -> Result<Self::SerializeTupleVariant, Self::Error> { unimplemented!() }
            fn serialize_map(self,_len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> { unimplemented!() }
            fn serialize_struct(self,_name: &'static str,_len: usize) -> Result<Self::SerializeStruct, Self::Error> { unimplemented!() }
            fn serialize_struct_variant(self,_name: &'static str,_variant_index: u32,_variant: &'static str,_len: usize) -> Result<Self::SerializeStructVariant, Self::Error> { unimplemented!() }
            fn serialize_newtype_variant<T: ?Sized + serde::Serialize>(
                self,
                _name: &'static str,
                _variant_index: u32,
                _variant: &'static str,
                value: &T,
            ) -> Result<Self::Ok, Self::Error> {
                value.serialize(self.0)
            }
        }

        Self::serialize(self, NewtypeVariantSerializer(serializer))
    }
}

impl<'de> serde::Deserialize<'de> for UnionLongDoubleB {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::deserialize(deserializer)
    }
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct A {
    pub item: Option<UnionLongDoubleB>,
}
