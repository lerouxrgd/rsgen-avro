use rsgen_avro::{Generator, Source};

#[test]
#[should_panic(expected = "Invalid default: Object({}), expected: Array")]
fn bad_default_for_array() {
    let raw_schema = r#"
{
  "type": "record",
  "name": "User",
  "fields": [ {
    "name": "m-f64",
    "type": {"type": "array", "items": "double"},
    "default": {}
  } ]
}
"#;

    let g = Generator::new().unwrap();
    let src = Source::SchemaStr(&raw_schema);
    let mut buf = vec![];
    g.gen(&src, &mut buf).unwrap()
}

#[test]
#[should_panic(expected = "Invalid default: Array([]), expected: Map")]
fn bad_default_for_map() {
    let raw_schema = r#"
{
  "type": "record",
  "name": "User",
  "fields": [ {
    "name": "m-f64",
    "type": {"type": "map", "values": "double"},
    "default": []
  } ]
}
"#;

    let g = Generator::new().unwrap();
    let src = Source::SchemaStr(&raw_schema);
    let mut buf = vec![];
    g.gen(&src, &mut buf).unwrap()
}

#[test]
#[should_panic(expected = "Invalid default: Array([]), expected: Object")]
fn bad_default_for_record() {
    let raw_schema = r#"
{
  "type": "record",
  "name": "User",
  "fields": [ {
    "name": "m-f64",
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
    let src = Source::SchemaStr(&raw_schema);
    let mut buf = vec![];
    g.gen(&src, &mut buf).unwrap()
}
