
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Test {
    #[serde(default = "default_test_a")]
    pub a: i64,
    pub b: String,
}

impl apache_avro::schema::derive::AvroSchemaComponent for Test {
    fn get_schema_in_ctxt(_: &mut apache_avro::schema::Names, _: &apache_avro::schema::Namespace) -> apache_avro::Schema {
        apache_avro::Schema::parse_str(r#"{"type":"record","name":"test","fields":[{"name":"a","type":"long","default":42},{"name":"b","type":"string"}]}"#).unwrap()
    }
}

#[inline(always)]
fn default_test_a() -> i64 { 42 }
