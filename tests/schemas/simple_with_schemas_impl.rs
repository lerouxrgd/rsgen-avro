
#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Test {
    #[serde(default = "default_test_a")]
    pub a: i64,
    pub b: String,
}

#[inline(always)]
fn default_test_a() -> i64 { 42 }
impl ::apache_avro::schema::AvroSchema for Test {
    fn get_schema() -> ::apache_avro::schema::Schema {
        ::apache_avro::schema::Schema::parse_str(r#"{"name":"test","type":"record","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}"#).expect("parsing of canonical form cannot fail")
    }
}
