extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

#[serde(default)]
#[derive(Debug, Default, Deserialize)]
struct User {
    name: String,
    favorite_number: i32,
    likes_pizza: bool,
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
        let json = "{}";
        let u: User = serde_json::from_str(json).unwrap();
        println!("{:?}", u);
    }
}
