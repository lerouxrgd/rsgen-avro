
/// Hi there.
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct User {
    pub name: String,
    pub favorite_number: i32,
    pub likes_pizza: bool,
    pub oye: f32,
    #[serde(rename = "aa-i32")]
    pub aa_i32: Vec<Vec<i32>>,
}

fn default_user_name() -> String { "".to_owned() }

fn default_user_favorite_number() -> i32 { 7 }

fn default_user_likes_pizza() -> bool { false }

fn default_user_oye() -> f32 { 1.100 }

fn default_user_aa_i32() -> Vec<Vec<i32>> { vec![vec![0], vec![12, -1]] }

impl Default for User {
    fn default() -> User {
        User {
            name: default_user_name(),
            favorite_number: default_user_favorite_number(),
            likes_pizza: default_user_likes_pizza(),
            oye: default_user_oye(),
            aa_i32: default_user_aa_i32(),
        }
    }
}
