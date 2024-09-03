
/// Auto-generated type for unnamed Avro union variants.
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(remote = "Self")]
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

impl serde::Serialize for UnionIntLong {
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

impl<'de> serde::Deserialize<'de> for UnionIntLong {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::deserialize(deserializer)
    }
}

/// Auto-generated type for unnamed Avro union variants.
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(remote = "Self")]
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

impl serde::Serialize for UnionFloatInt {
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

impl<'de> serde::Deserialize<'de> for UnionFloatInt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::deserialize(deserializer)
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
