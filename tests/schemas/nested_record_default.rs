
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Inner {
    pub a: bool,
}

#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct User {
    pub m_f64: Inner,
}

#[inline(always)]
fn default_user_m_f64() -> Inner { Inner { a: true, } }

impl Default for User {
    fn default() -> User {
        User {
            m_f64: default_user_m_f64(),
        }
    }
}
