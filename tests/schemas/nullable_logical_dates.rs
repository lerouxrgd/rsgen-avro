
/// Date type
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct DateLogicalType {
    #[serde(deserialize_with = "nullable_datelogicaltype_birthday")]
    pub birthday: i32,
    pub meeting_time: Option<i64>,
    #[serde(deserialize_with = "nullable_datelogicaltype_release_datetime_micro")]
    pub release_datetime_micro: i64,
}

#[inline(always)]
fn nullable_datelogicaltype_birthday<'de, D>(deserializer: D) -> Result<i32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_else(|| default_datelogicaltype_birthday() ))
}

#[inline(always)]
fn nullable_datelogicaltype_release_datetime_micro<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_else(|| default_datelogicaltype_release_datetime_micro() ))
}

#[inline(always)]
fn default_datelogicaltype_birthday() -> i32 { 1681601653 }

#[inline(always)]
fn default_datelogicaltype_meeting_time() -> Option<i64> { None }

#[inline(always)]
fn default_datelogicaltype_release_datetime_micro() -> i64 { 1570903062000000 }

impl Default for DateLogicalType {
    fn default() -> DateLogicalType {
        DateLogicalType {
            birthday: default_datelogicaltype_birthday(),
            meeting_time: default_datelogicaltype_meeting_time(),
            release_datetime_micro: default_datelogicaltype_release_datetime_micro(),
        }
    }
}
