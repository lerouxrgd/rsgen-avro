#![allow(clippy::all)]
pub mod array_3d;
pub mod complex;
pub mod decimals;
pub mod enums;
pub mod enums_casing;
pub mod enums_multiline_doc;
pub mod enums_sanitize;
#[allow(dead_code)]
pub mod fixed;
pub mod interop;
pub mod logical_dates;
pub mod chrono_logical_dates;
pub mod map_default;
pub mod map_multiple_def;
pub mod mono_valued_union;
pub mod multi_valued_union;
pub mod multi_valued_union_map;
pub mod multi_valued_union_nested;
pub mod multi_valued_union_records;
pub mod multi_valued_union_with_avro_rs_unions;
pub mod nested_record_default;
pub mod nested_record_partial_default;
pub mod nullable;
pub mod nullable_bytes;
pub mod nullable_logical_dates;
pub mod nullable_chrono_logical_dates;
pub mod optional_array;
pub mod optional_arrays;
pub mod record;
pub mod record_default;
pub mod record_multiline_doc;
pub mod recursive;
pub mod simple;
pub mod simple_with_builders;
pub mod simple_with_override;
pub mod simple_with_schemas;
pub mod simple_with_schemas_impl;
pub mod nested_with_schemas_impl;
pub mod nested_with_float;

/// For testing field overrides in simple_with_override
mod ip_addr_serde {
    use std::{fmt::Formatter, net::IpAddr};
    use serde::{de::{Error, Visitor}, Deserializer, Serializer};

    pub fn serialize<S>(value: &IpAddr, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        match value {
            IpAddr::V4(ipv4_addr) => serializer.serialize_bytes(&ipv4_addr.octets()),
            IpAddr::V6(ipv6_addr) => serializer.serialize_bytes(&ipv6_addr.octets()),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<IpAddr, D::Error> where D: Deserializer<'de> {
        struct IpVisitor;
        impl Visitor<'_> for IpVisitor {
            type Value = IpAddr;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("an IPv4 address as bytes")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E> 
            where E: Error {
                match v.len() {
                    4 => {
                        let bytes: [u8; 4] = v.try_into().unwrap();
                        Ok(IpAddr::from(bytes))
                    }
                    16 => {
                        let bytes: [u8; 16] = v.try_into().unwrap();
                        Ok(IpAddr::from(bytes))
                    }
                    _ => Err(Error::custom("Invalid IP address byte length")),
                }
            }
        }

        deserializer.deserialize_bytes(IpVisitor)
    }
}
