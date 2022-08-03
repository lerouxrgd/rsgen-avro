
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Info {
    pub name: String,
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct User {
    pub info: Info,
}

#[inline(always)]
fn default_user_info() -> Info { Info { name: "bob".to_owned(), } }

impl Default for User {
    fn default() -> User {
        User {
            info: default_user_info(),
        }
    }
}
