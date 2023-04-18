
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct TestB {
}

impl Default for TestB {
    fn default() -> TestB {
        TestB {
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct TestA {
    pub a: i64,
    pub b: String,
    #[serde(default = "default_testa_c")]
    pub c: Option<String>,
}

#[inline(always)]
fn default_testa_c() -> Option<String> { None }
