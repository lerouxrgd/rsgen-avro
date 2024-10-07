
/// Date type
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct DateLogicalType {
    pub birthday: i32,
    #[serde(default = "default_datelogicaltype_meeting_time")]
    pub meeting_time: Option<i64>,
    #[serde(default = "default_datelogicaltype_release_datetime_micro")]
    pub release_datetime_micro: i64,
}

#[inline(always)]
fn default_datelogicaltype_meeting_time() -> Option<i64> { None }

#[inline(always)]
fn default_datelogicaltype_release_datetime_micro() -> i64 { 1570903062000000 }
