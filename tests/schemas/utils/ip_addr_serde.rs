//! Serde implementation for `IpAddr` for testing field overrides in `simple_with_override`.

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
