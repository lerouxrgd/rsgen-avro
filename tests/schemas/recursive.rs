
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Additional {
    pub field_x: String,
    #[serde(default = "default_additional_parent")]
    pub parent: Option<Box<Metadata>>,
}

#[inline(always)]
fn default_additional_parent() -> Option<Box<Metadata>> { None }

#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Metadata {
    pub label: String,
    pub additional: Additional,
}

#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Parent {
    pub label: String,
    #[serde(default = "default_parent_parent")]
    pub parent: Option<Box<Parent>>,
}

#[inline(always)]
fn default_parent_parent() -> Option<Box<Parent>> { None }

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
    pub direct_parent: Parent,
    pub nested_parent: Metadata,
}
