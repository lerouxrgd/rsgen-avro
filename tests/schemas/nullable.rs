
macro_rules! deser(
    ($name:ident, $rtype:ty, $val:expr) => (
        fn $name<'de, D>(deserializer: D) -> Result<$rtype, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            use serde::Deserialize;
            let opt = Option::deserialize(deserializer)?;
            Ok(opt.unwrap_or_else(|| $val))
        }
    );
);

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct Test {
    #[serde(deserialize_with = "nullable_test_a")]
    pub a: i64,
    #[serde(rename = "b-b", deserialize_with = "nullable_test_b_b")]
    pub b_b: String,
    pub c: Option<i32>,
}
deser!(nullable_test_a, i64, 42);
deser!(nullable_test_b_b, String, "na".to_owned());

fn default_test_a() -> i64 { 42 }

fn default_test_b_b() -> String { "na".to_owned() }

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
