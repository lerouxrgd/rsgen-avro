
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, serde::Deserialize, serde::Serialize)]
pub enum ContextScopeKind {
    #[serde(rename = "self")]
    r#Self_,
    #[serde(rename = "sources")]
    Sources,
    #[serde(rename = "targets")]
    Targets,
    #[serde(rename = "sourcesOrSelf")]
    SourcesOrSelf,
    #[serde(rename = "targetsOrSelf")]
    TargetsOrSelf,
}
