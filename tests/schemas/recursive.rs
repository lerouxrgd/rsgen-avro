
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Node {
    pub label: String,
    pub children: Vec<Node>,
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Rec {
    pub label: String,
    pub children: Vec<Rec>,
    #[serde(rename = "floatField")]
    pub float_field: f32,
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct RecursiveType {
    pub field_a: Rec,
    pub field_b: Node,
}
