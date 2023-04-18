
/// Date type
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct DateLogicalType {
    #[serde(deserialize_with = "nullable_datelogicaltype_birthday")]
    #[serde(serialize_with = "chrono::naive::serde::ts_seconds::serialize")]
    pub birthday: chrono::NaiveDateTime,
    pub meeting_time: Option<chrono::NaiveDateTime>,
    #[serde(deserialize_with = "nullable_datelogicaltype_release_datetime_micro")]
    #[serde(serialize_with = "chrono::naive::serde::ts_microseconds::serialize")]
    pub release_datetime_micro: chrono::NaiveDateTime,
}

#[inline(always)]
fn nullable_datelogicaltype_birthday<'de, D>(deserializer: D) -> Result<chrono::NaiveDateTime, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    #[derive(serde::Deserialize)]
    struct Wrapper(#[serde(with = "chrono::naive::serde::ts_seconds")] chrono::NaiveDateTime);
    let opt = Option::<Wrapper>::deserialize(deserializer)?.map(|w| w.0);
    Ok(opt.unwrap_or_else(|| default_datelogicaltype_birthday() ))
}

#[inline(always)]
fn nullable_datelogicaltype_release_datetime_micro<'de, D>(deserializer: D) -> Result<chrono::NaiveDateTime, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    #[derive(serde::Deserialize)]
    struct Wrapper(#[serde(with = "chrono::naive::serde::ts_microseconds")] chrono::NaiveDateTime);
    let opt = Option::<Wrapper>::deserialize(deserializer)?.map(|w| w.0);
    Ok(opt.unwrap_or_else(|| default_datelogicaltype_release_datetime_micro() ))
}

#[inline(always)]
fn default_datelogicaltype_birthday() -> chrono::NaiveDateTime { chrono::NaiveDateTime::from_timestamp_opt(1681601653, 0).unwrap() }

#[inline(always)]
fn default_datelogicaltype_meeting_time() -> Option<chrono::NaiveDateTime> { None }

#[inline(always)]
fn default_datelogicaltype_release_datetime_micro() -> chrono::NaiveDateTime { chrono::NaiveDateTime::from_timestamp_micros(1570903062000000).unwrap() }

impl Default for DateLogicalType {
    fn default() -> DateLogicalType {
        DateLogicalType {
            birthday: default_datelogicaltype_birthday(),
            meeting_time: default_datelogicaltype_meeting_time(),
            release_datetime_micro: default_datelogicaltype_release_datetime_micro(),
        }
    }
}
