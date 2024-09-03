#[rustfmt::skip]
mod schemas;

use std::collections::HashMap;

use crate::schemas::multi_valued_union_with_avro_rs_unions::Contact;

#[test]
fn multi_valued_union_serde() {
    let expected = Contact {
        extra: HashMap::from_iter([
            ("bytes".into(), Some("value".as_bytes().to_vec().into())),
            ("string".into(), Some("value".to_string().into())),
            ("long".into(), Some(12.into())),
        ]),
    };

    let schema = apache_avro::Schema::parse_str(include_str!(
        "schemas/multi_valued_union_with_avro_rs_unions.avsc"
    ))
    .unwrap();

    let value = apache_avro::to_value(expected.clone()).unwrap();
    let value = value.resolve(&schema).unwrap();
    let value: Contact = apache_avro::from_value(&value).unwrap();
    assert_eq!(expected, value);
}
