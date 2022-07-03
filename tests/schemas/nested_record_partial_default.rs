
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Nested {
    pub a: i32,
    #[serde(default = "default_nested_b")]
    pub b: i32,
}

fn default_nested_b() -> i32 { 20 }

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct User {
    pub nested: Nested,
}

fn default_user_nested() -> Nested { Nested { a: 10, b: default_nested_b(), } }

impl Default for User {
    fn default() -> User {
        User {
            nested: default_user_nested(),
        }
    }
}
