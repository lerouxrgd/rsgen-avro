
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize,std::fmt::Display)]
pub struct Test {
    #[serde(default = "default_test_a")]
    pub a: i64,
    pub b: String,
}

#[inline(always)]
fn default_test_a() -> i64 { 42 }
