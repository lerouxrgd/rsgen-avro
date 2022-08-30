
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct User {
    pub r#as: String,
    #[serde(rename = "favoriteNumber")]
    #[serde(default = "default_user_favorite_number")]
    pub favorite_number: i32,
    #[serde(default = "default_user_likes_pizza")]
    pub likes_pizza: bool,
    #[serde(default = "default_user_b")]
    #[serde(with = "serde_bytes")]
    pub b: Vec<u8>,
    #[serde(default = "default_user_union_b")]
    #[serde(with = "serde_bytes")]
    pub union_b: Option<Vec<u8>>,
    #[serde(rename = "a-bool")]
    #[serde(default = "default_user_a_bool")]
    pub a_bool: Vec<bool>,
    #[serde(rename = "a-i32")]
    #[serde(default = "default_user_a_i32")]
    pub a_i32: Vec<i32>,
    #[serde(rename = "m-f64")]
    pub m_f64: ::std::collections::HashMap<String, f64>,
}

#[inline(always)]
fn default_user_favorite_number() -> i32 { 7 }

#[inline(always)]
fn default_user_likes_pizza() -> bool { false }

#[inline(always)]
fn default_user_b() -> Vec<u8> { vec![195, 191] }

#[inline(always)]
fn default_user_union_b() -> Option<Vec<u8>> { None }

#[inline(always)]
fn default_user_a_bool() -> Vec<bool> { vec![true, false] }

#[inline(always)]
fn default_user_a_i32() -> Vec<i32> { vec![12, -1] }
