#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;

#[serde(default)]
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Aaa {
    pub a: i32,
}

#[serde(default)]
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Bbb {
    pub b: Aaa,
}

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
    pub m: Md5,
    pub aaa: Aaa,
    pub bbb: Bbb,
    pub map: ::std::collections::HashMap<String, i32>,
}

pub type Md5 = [u8; 2];

impl Default for User {
    fn default() -> User {
        User {
            as_: String::default(),
            b: vec![195, 191],
            favorite_number: 7,
            likes_pizza: false,
            t_bool: vec![true, false],
            t_i32: vec![12, -1],
            m: Md5::default(),
            aaa: Aaa::default(),
            bbb: {
                let mut x = Bbb::default();
                x.b = {
                    let mut x = Aaa::default();
                    x.a = 12;
                    x
                };
                x
            },
            map: ::std::collections::HashMap::new(),
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
        // let u: User = Default::default();
        // println!("{:?}", u);

        let json = r#"{}"#;
        let u: User = serde_json::from_str(json).unwrap();
        println!("{:?}", u);

        let s = serde_json::to_string(&u).unwrap();
        println!("{}", s);
    }
}
