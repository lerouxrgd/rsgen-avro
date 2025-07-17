
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct TestOverrideNoEq {
    #[serde(default = "default_testoverridenoeq_a")]
    pub a: f64,
    pub b: String,
}

#[inline(always)]
fn default_testoverridenoeq_a() -> f64 { 42.0 }
