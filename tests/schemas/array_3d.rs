
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Array3d {
    pub coordinates: Vec<Vec<Vec<f64>>>,
}
