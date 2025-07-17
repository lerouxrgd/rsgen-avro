
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct TestOverride {
    #[serde(default = "default_testoverride_a")]
    pub a: i64,
    pub b: String,
    /// The IP-address associated with this Avro
    #[serde(with = "super::utils::ip_addr_serde")]
    pub ip_addr: std::net::IpAddr,
}

#[inline(always)]
fn default_testoverride_a() -> i64 { 42 }
