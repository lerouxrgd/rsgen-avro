
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct User {
    pub r#as: String,
    #[serde(rename = "favoriteNumber")]
    #[serde(default = "default_user_favorite_number")]
    pub favorite_number: i32,
    #[serde(default = "default_user_likes_pizza")]
    pub likes_pizza: bool,
    #[serde(default = "default_user_b")]
    pub b: Vec<u8>,
    #[serde(rename = "a-bool")]
    #[serde(default = "default_user_a_bool")]
    pub a_bool: Vec<bool>,
    #[serde(rename = "a-i32")]
    #[serde(default = "default_user_a_i32")]
    pub a_i32: Vec<i32>,
    #[serde(rename = "m-f64")]
    pub m_f64: ::std::collections::HashMap<String, f64>,
}

fn default_user_favorite_number() -> i32 { 7 }

fn default_user_likes_pizza() -> bool { false }

fn default_user_b() -> Vec<u8> { vec![195, 191] }

fn default_user_a_bool() -> Vec<bool> { vec![true, false] }

fn default_user_a_i32() -> Vec<i32> { vec![12, -1] }