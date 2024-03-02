use apache_avro::Error;
use serde_json::{to_string, Value};

pub struct Protocol {
    // pub name: String,
    // pub doc: String,
    // pub schema: Schema,
}

#[derive(Debug, Clone)]
struct ExpectedArrayError;

impl Protocol {
    pub fn get_schemas(input: &str) -> Result<Vec<String>, Error> {
        let schema: Value = serde_json::from_str(input).map_err(Error::ParseSchemaJson)?;
        let types = schema["types"].as_array();
        match types {
            Some(t) => Ok(t
                .into_iter()
                .flat_map(|json| to_string(json))
                .collect()
            ),
            None => Err(Error::GetArrayItemsField),
        }
    }
}