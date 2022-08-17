
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct KsqlDataSourceSchema {
    #[serde(rename = "ID")]
    pub id: Option<String>,
    #[serde(rename = "GROUP_IDS")]
    pub group_ids: Option<Vec<Option<String>>>,
    #[serde(rename = "GROUP_NAMES")]
    pub group_names: Option<Vec<Option<String>>>,
}

#[inline(always)]
fn default_ksqldatasourceschema_id() -> Option<String> { None }

#[inline(always)]
fn default_ksqldatasourceschema_group_ids() -> Option<Vec<Option<String>>> { None }

#[inline(always)]
fn default_ksqldatasourceschema_group_names() -> Option<Vec<Option<String>>> { None }

impl Default for KsqlDataSourceSchema {
    fn default() -> KsqlDataSourceSchema {
        KsqlDataSourceSchema {
            id: default_ksqldatasourceschema_id(),
            group_ids: default_ksqldatasourceschema_group_ids(),
            group_names: default_ksqldatasourceschema_group_names(),
        }
    }
}
