
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct BytesData {
    #[serde(deserialize_with = "nullable_bytesdata_b")]
    #[serde(serialize_with = "serde_bytes::serialize")]
    pub b: Vec<u8>,
    pub nb: Option<Vec<u8>>,
}

#[inline(always)]
fn nullable_bytesdata_b<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    #[derive(serde::Deserialize)]
    struct Wrapper(#[serde(with = "serde_bytes")] Vec<u8>);
    let opt = Option::<Wrapper>::deserialize(deserializer)?.map(|w| w.0);
    Ok(opt.unwrap_or_else(|| default_bytesdata_b() ))
}

#[inline(always)]
fn default_bytesdata_b() -> Vec<u8> { vec![195, 191] }

#[inline(always)]
fn default_bytesdata_nb() -> Option<Vec<u8>> { None }

impl Default for BytesData {
    fn default() -> BytesData {
        BytesData {
            b: default_bytesdata_b(),
            nb: default_bytesdata_nb(),
        }
    }
}
