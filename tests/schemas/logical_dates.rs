
/// Date type
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct DateLogicalType {
    #[serde(with = "chrono::serde::ts_seconds")]
    pub birthday: chrono::DateTime<chrono::Utc>,
    #[serde(default = "default_datelogicaltype_meeting_time")]
    pub meeting_time: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(with = "chrono::serde::ts_microseconds")]
    #[serde(default = "default_datelogicaltype_release_datetime_micro")]
    pub release_datetime_micro: chrono::DateTime<chrono::Utc>,
}

#[inline(always)]
fn default_datelogicaltype_meeting_time() -> Option<chrono::DateTime<chrono::Utc>> { None }

#[inline(always)]
fn default_datelogicaltype_release_datetime_micro() -> chrono::DateTime<chrono::Utc> { chrono::DateTime::<chrono::Utc>::from_timestamp_micros(1570903062000000).unwrap() }
