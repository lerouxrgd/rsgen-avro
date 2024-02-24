
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Decimals {
    pub a_decimal: apache_avro::Decimal,
    #[serde(default = "default_decimals_a_big_decimal")]
    pub a_big_decimal: Option<apache_avro::BigDecimal>,
}

#[inline(always)]
fn default_decimals_a_big_decimal() -> Option<apache_avro::BigDecimal> { None }
