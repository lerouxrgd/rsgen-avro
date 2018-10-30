#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;

use serde::{Deserialize, Deserializer};

/// TODO add as an option for templating
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

deser!(nullable_user_a, i64, 2);

#[serde(default)]
#[derive(Debug, Deserialize, Serialize)]
pub struct User {
    #[serde(deserialize_with = "nullable_user_a")]
    a: i64,
}

impl Default for User {
    fn default() -> User {
        User { a: 2 }
    }
}

// fn nullable_a<'de, D>(deserializer: D) -> Result<i64, D::Error>
// where
//     D: Deserializer<'de>,
// {
//     let opt = Option::deserialize(deserializer)?;
//     Ok(opt.unwrap_or_else(|| 2))
// }

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
