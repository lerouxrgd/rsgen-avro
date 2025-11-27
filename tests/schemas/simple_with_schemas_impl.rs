
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
        static SCHEMA: std::sync::OnceLock::<apache_avro::Schema> = std::sync::OnceLock::new();
        SCHEMA.get_or_init(|| {
            ::apache_avro::schema::Schema::parse_str(r#"{"name":"test","type":"record","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}"#).expect("parsing of canonical form cannot fail")
        }).clone()
    }
}
#[cfg(test)]
#[test]
fn test_test_avro_schema_impl() {
    <Test as ::apache_avro::schema::AvroSchema>::get_schema();
}
