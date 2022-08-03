
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct User {
    #[serde(rename = "m-f64")]
    pub m_f64: ::std::collections::HashMap<String, f64>,
}

#[inline(always)]
fn default_user_m_f64() -> ::std::collections::HashMap<String, f64> { { let mut m = ::std::collections::HashMap::new(); m.insert("a".to_owned(), 12.0); m.insert("b".to_owned(), 42.100); m } }

impl Default for User {
    fn default() -> User {
        User {
            m_f64: default_user_m_f64(),
        }
    }
}
