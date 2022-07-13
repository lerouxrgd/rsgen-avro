#[rustfmt::skip]
mod schemas;

#[test]
fn nullable_deser() {
    let serialized = r#"{"a":12,"b_b":null}"#;

    let val = serde_json::from_str::<schemas::nullable::Test>(serialized).unwrap();
    assert!(val.a == 12);
    assert!(val.b_b == "na".to_owned());
    assert!(val.c.is_none());
}
