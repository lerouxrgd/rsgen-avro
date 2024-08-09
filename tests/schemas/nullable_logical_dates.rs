
/// Date type
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct DateLogicalType {
    #[serde(deserialize_with = "nullable_datelogicaltype_birthday")]
    #[serde(serialize_with = "chrono::serde::ts_seconds::serialize")]
    pub birthday: chrono::DateTime<chrono::Utc>,
    pub meeting_time: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(deserialize_with = "nullable_datelogicaltype_release_datetime_micro")]
    #[serde(serialize_with = "chrono::serde::ts_microseconds::serialize")]
    pub release_datetime_micro: chrono::DateTime<chrono::Utc>,
}

#[inline(always)]
fn nullable_datelogicaltype_birthday<'de, D>(deserializer: D) -> Result<chrono::DateTime<chrono::Utc>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    #[derive(serde::Deserialize)]
    struct Wrapper(#[serde(with = "chrono::serde::ts_seconds")] chrono::DateTime<chrono::Utc>);
    let opt = Option::<Wrapper>::deserialize(deserializer)?.map(|w| w.0);
    Ok(opt.unwrap_or_else(|| default_datelogicaltype_birthday() ))
}

#[inline(always)]
fn nullable_datelogicaltype_release_datetime_micro<'de, D>(deserializer: D) -> Result<chrono::DateTime<chrono::Utc>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    #[derive(serde::Deserialize)]
    struct Wrapper(#[serde(with = "chrono::serde::ts_microseconds")] chrono::DateTime<chrono::Utc>);
    let opt = Option::<Wrapper>::deserialize(deserializer)?.map(|w| w.0);
    Ok(opt.unwrap_or_else(|| default_datelogicaltype_release_datetime_micro() ))
}

#[inline(always)]
fn default_datelogicaltype_birthday() -> chrono::DateTime<chrono::Utc> { chrono::DateTime::<chrono::Utc>::from_timestamp(1681601653, 0).unwrap() }

#[inline(always)]
fn default_datelogicaltype_meeting_time() -> Option<chrono::DateTime<chrono::Utc>> { None }

#[inline(always)]
fn default_datelogicaltype_release_datetime_micro() -> chrono::DateTime<chrono::Utc> { chrono::DateTime::<chrono::Utc>::from_timestamp_micros(1570903062000000).unwrap() }

impl Default for DateLogicalType {
    fn default() -> DateLogicalType {
        DateLogicalType {
            birthday: default_datelogicaltype_birthday(),
            meeting_time: default_datelogicaltype_meeting_time(),
            release_datetime_micro: default_datelogicaltype_release_datetime_micro(),
        }
    }
}
