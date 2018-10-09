extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

#[serde(default)]
#[derive(Debug, Deserialize, Serialize)]
pub struct User {
    #[serde(rename = "as")]
    pub as_: String,
    pub b: Vec<u8>,
    #[serde(rename = "favoriteNumber")]
    pub favorite_number: i32,
    pub likes_pizza: bool,
    #[serde(rename = "t-bool")]
    pub t_bool: Vec<bool>,
    #[serde(rename = "t-i32")]
    pub t_i32: Vec<i32>,
}

impl Default for User {
    fn default() -> User {
        User {
            as_: String::default(),
            b: vec![195, 191],
            favorite_number: 7,
            likes_pizza: false,
            t_bool: vec![true, false],
            t_i32: vec![12, -1],
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
