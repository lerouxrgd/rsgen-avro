
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct Optional {
    pub field3: Option<::std::collections::HashMap<String, String>>,
}

#[inline(always)]
fn default_optional_field3() -> Option<::std::collections::HashMap<String, String>> { None }

impl Default for Optional {
    fn default() -> Optional {
        Optional {
            field3: default_optional_field3(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct Payload {
    pub field1: Option<::std::collections::HashMap<String, String>>,
    pub field2: Option<Optional>,
}

#[inline(always)]
fn default_payload_field1() -> Option<::std::collections::HashMap<String, String>> { None }

#[inline(always)]
fn default_payload_field2() -> Option<Optional> { None }

impl Default for Payload {
    fn default() -> Payload {
        Payload {
            field1: default_payload_field1(),
            field2: default_payload_field2(),
        }
    }
}
