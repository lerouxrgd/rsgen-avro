extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

#[serde(default)]
#[derive(Debug, Deserialize, Serialize)]
struct User {
    as_: String,
    favorite_number: i32,
    likes_pizza: bool,
    b: Vec<u8>,
}

impl Default for User {
    fn default() -> User {
        User {
            as_: String::default(),
            favorite_number: 7,
            likes_pizza: false,
            b: vec![],
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Colors {
    #[serde(rename = "BLUE")]
    Blue,
    #[serde(rename = "GREEN")]
    Green,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate() {
        // let user: User = Default::default();
        // println!("{:?}", user);
        let json = r#"{}"#;
        let u: User = serde_json::from_str(json).unwrap();
        println!("{:?}", u);
        let s = serde_json::to_string(&u).unwrap();
        println!("{}", s);
    }
}
