use rsgen_avro::{Generator, Source};

#[test]
#[should_panic(
    expected = r#"Avro error: `default`'s value type of field "m_f64" in "User" must be "{\"type\":\"array\",\"items\":\"double\"}""#
)]
fn bad_default_for_array() {
    let raw_schema = r#"
{
  "type": "record",
  "name": "User",
  "fields": [ {
    "name": "m_f64",
    "type": {"type": "array", "items": "double"},
    "default": {}
  } ]
}
"#;

    let g = Generator::new().unwrap();
    let src = Source::SchemaStr(raw_schema);
    let mut buf = vec![];
    g.generate(&src, &mut buf).map_err(|e| panic!("{e}")).ok();
}

#[test]
#[should_panic(
    expected = r#"Avro error: `default`'s value type of field "m_f64" in "User" must be "{\"type\":\"map\",\"values\":\"double\"}""#
)]
fn bad_default_for_map() {
    let raw_schema = r#"
{
  "type": "record",
  "name": "User",
  "fields": [ {
    "name": "m_f64",
    "type": {"type": "map", "values": "double"},
    "default": []
  } ]
}
"#;

    let g = Generator::new().unwrap();
    let src = Source::SchemaStr(raw_schema);
    let mut buf = vec![];
    g.generate(&src, &mut buf).map_err(|e| panic!("{e}")).ok();
}

#[test]
#[should_panic(
    expected = r#"Avro error: `default`'s value type of field "m_f64" in "User" must be "{\"name\":\"Inner\",\"type\":\"record\",\"fields\":[{\"name\":\"a\",\"type\":\"boolean\"}]}""#
)]
fn bad_default_for_record() {
    let raw_schema = r#"
{
  "type": "record",
  "name": "User",
  "fields": [ {
    "name": "m_f64",
    "type": {
      "type": "record",
      "name": "Inner",
      "fields": [ {
        "name": "a",
        "type": "boolean"
      } ]
    },
    "default": []
  } ]
}
"#;

    let g = Generator::new().unwrap();
    let src = Source::SchemaStr(raw_schema);
    let mut buf = vec![];
    g.generate(&src, &mut buf).map_err(|e| panic!("{e}")).ok();
}

#[test]
#[should_panic(expected = r#"Names must be unique within a Union: 'MyRecord'"#)]
fn duplicate_names_for_union() {
    let raw_schema = r#"
[
  {
    "type": "record",
    "name": "MyRecord",
    "namespace": "com.foo",
    "fields": [
      { "name": "foo", "type": "string" }
    ]
  },
  {
    "type": "record",
    "name": "MyRecord",
    "namespace": "com.bar",
    "fields": [
      { "name": "bar", "type": "string" }
    ]
  }
]
"#;

    let g = Generator::new().unwrap();
    let src = Source::SchemaStr(raw_schema);
    let mut buf = vec![];
    g.generate(&src, &mut buf).unwrap();
}
