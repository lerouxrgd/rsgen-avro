
/// Roses are red
/// violets are blue.
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, serde::Deserialize, serde::Serialize)]
pub enum Colors {
    #[serde(rename = "RED")]
    Red,
}
