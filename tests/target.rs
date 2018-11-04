extern crate serde;
extern crate serde_json;

use serde::{Deserialize, Deserializer, Serialize};

macro_rules! deser(
    ($name:ident, $rtype:ty, $val:expr) => (
        fn $name<'de, D>(deserializer: D) -> Result<$rtype, D::Error>
        where
            D: Deserializer<'de>,
        {
            let opt = Option::deserialize(deserializer)?;
            Ok(opt.unwrap_or_else(|| $val))
        }
    );
);

#[serde(default)]
#[derive(Debug, Deserialize, Serialize)]
pub struct User {
    #[serde(rename = "a-bool", deserialize_with = "nullable_user_a_bool")]
    pub a_bool: Vec<bool>,
    #[serde(rename = "a-i32", deserialize_with = "nullable_user_a_i32")]
    pub a_i32: Vec<i32>,
    #[serde(rename = "as", deserialize_with = "nullable_user_as_")]
    pub as_: String,
    #[serde(deserialize_with = "nullable_user_b")]
    pub b: Vec<u8>,
    #[serde(
        rename = "favoriteNumber",
        deserialize_with = "nullable_user_favorite_number"
    )]
    pub favorite_number: i32,
    #[serde(deserialize_with = "nullable_user_likes_pizza")]
    pub likes_pizza: bool,
    #[serde(rename = "m-f64", deserialize_with = "nullable_user_m_f64")]
    pub m_f64: ::std::collections::HashMap<String, f64>,
}
deser!(nullable_user_a_bool, Vec<bool>, vec![true, false]);
deser!(nullable_user_a_i32, Vec<i32>, vec![12, -1]);
deser!(nullable_user_as_, String, String::default());
deser!(nullable_user_b, Vec<u8>, vec![195, 191]);
deser!(nullable_user_favorite_number, i32, 7);
deser!(nullable_user_likes_pizza, bool, false);
deser!(nullable_user_m_f64, ::std::collections::HashMap<String, f64>, ::std::collections::HashMap::new());

impl Default for User {
    fn default() -> User {
        User {
            a_bool: vec![true, false],
            a_i32: vec![12, -1],
            as_: String::default(),
            b: vec![195, 191],
            favorite_number: 7,
            likes_pizza: false,
            m_f64: ::std::collections::HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate() {
        // let u: User = Default::default();
        // println!("{:?}", u);

        let json = r#"{"a": null}"#;
        let u: User = serde_json::from_str(json).unwrap();
        println!("{:?}", u);

        let s = serde_json::to_string(&u).unwrap();
        println!("{}", s);
    }
}
