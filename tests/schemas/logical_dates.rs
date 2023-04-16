
/// Date type
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct DateLogicalType {
    #[serde(with = "chrono::naive::serde::ts_seconds")]
    pub birthday: chrono::NaiveDateTime,
    #[serde(default = "default_datelogicaltype_meeting_time")]
    pub meeting_time: Option<chrono::NaiveDateTime>,
    #[serde(with = "chrono::naive::serde::ts_microseconds")]
    #[serde(default = "default_datelogicaltype_release_datetime_micro")]
    pub release_datetime_micro: chrono::NaiveDateTime,
}

#[inline(always)]
fn default_datelogicaltype_meeting_time() -> Option<chrono::NaiveDateTime> { None }

#[inline(always)]
fn default_datelogicaltype_release_datetime_micro() -> chrono::NaiveDateTime { chrono::NaiveDateTime::from_timestamp_micros(1570903062000000).unwrap() }
