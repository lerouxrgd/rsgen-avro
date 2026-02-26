
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct User {
    pub r#as: String,
    #[serde(rename = "favoriteNumber")]
    #[serde(default = "default_user_favorite_number")]
    pub favorite_number: i32,
    #[serde(default = "default_user_likes_pizza")]
    pub likes_pizza: bool,
    #[serde(with = "apache_avro::serde_avro_bytes")]
    #[serde(default = "default_user_b")]
    pub b: Vec<u8>,
    #[serde(with = "apache_avro::serde_avro_bytes_opt")]
    #[serde(default = "default_user_union_b")]
    pub union_b: Option<Vec<u8>>,
    #[serde(rename = "A_Bool")]
    #[serde(default = "default_user_a_bool")]
    pub a_bool: Vec<bool>,
    #[serde(rename = "SomeInteger")]
    #[serde(default = "default_user_some_integer")]
    pub some_integer: Vec<i32>,
    pub map_of_f64: ::std::collections::HashMap<String, f64>,
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
fn default_user_some_integer() -> Vec<i32> { vec![12, -1] }
