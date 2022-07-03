
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Test {
    #[serde(default = "default_test_a")]
    pub a: i64,
    pub b: String,
}

fn default_test_a() -> i64 { 42 }
