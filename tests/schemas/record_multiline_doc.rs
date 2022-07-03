
/// Some user representation
/// Users love pizzas!
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct User {
    pub likes_pizza: bool,
}

fn default_user_likes_pizza() -> bool { false }

impl Default for User {
    fn default() -> User {
        User {
            likes_pizza: default_user_likes_pizza(),
        }
    }
}
