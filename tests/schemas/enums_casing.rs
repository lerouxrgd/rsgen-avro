
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Copy, serde::Deserialize, serde::Serialize)]
pub enum MeasurementUnit {
    #[serde(rename = "mJ")]
    MJ,
}

/// Record doc
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct MyRecord {
    /// Identifier
    pub id: i32,
    /// Measurement unit
    #[serde(default = "default_myrecord_unit")]
    pub unit: MeasurementUnit,
}

#[inline(always)]
fn default_myrecord_unit() -> MeasurementUnit { MeasurementUnit::MJ }
