
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct Test {
    #[serde(deserialize_with = "nullable_test_a")]
    pub a: i64,
    #[serde(rename = "b-b", deserialize_with = "nullable_test_b_b")]
    pub b_b: String,
    pub c: Option<i32>,
}

#[inline(always)]
fn nullable_test_a<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_else(|| 42))
}

#[inline(always)]
fn nullable_test_b_b<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_else(|| "na".to_owned()))
}

#[inline(always)]
fn default_test_a() -> i64 { 42 }

#[inline(always)]
fn default_test_b_b() -> String { "na".to_owned() }

#[inline(always)]
fn default_test_c() -> Option<i32> { None }

impl Default for Test {
    fn default() -> Test {
        Test {
            a: default_test_a(),
            b_b: default_test_b_b(),
            c: default_test_c(),
        }
    }
}
