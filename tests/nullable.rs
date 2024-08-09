#[rustfmt::skip]
mod schemas;

#[test]
fn deser_nullable() {
    let serialized = r#"{"a":12,"b_b":null}"#;

    let val = serde_json::from_str::<schemas::nullable::Test>(serialized).unwrap();
    assert!(
        val.b_b == "na",
        "Should use schema-defined default value when null"
    );
    assert!(
        val.c.is_none(),
        "Schema-defined optional should remain optional in Rust"
    );
    assert!(
        val.a == 12,
        "Deserialized value is different from payload value"
    );
}

#[test]
fn deser_nullable_bytes() {
    let serialized = r#"{"b":null,"nb":[1,2,3]}"#;

    // See: https://www.compart.com/fr/unicode/U+00FF
    let val = serde_json::from_str::<schemas::nullable_bytes::BytesData>(serialized).unwrap();
    assert!(
        val.b == [0xC3, 0xBF],
        "Should use schema-defined default value when null"
    );
    assert!(
        val.nb == Some(vec![1, 2, 3]),
        "Deserialized value is different from payload value"
    );
}

#[test]
fn deser_nullable_logical_dates() {
    let serialized =
        r#"{"birthday":null,"meeting_time":null,"release_datetime_micro":1681601301000000}"#;

    let val = serde_json::from_str::<schemas::nullable_logical_dates::DateLogicalType>(serialized)
        .unwrap();
    assert!(
        val.birthday == chrono::DateTime::<chrono::Utc>::from_timestamp(1681601653, 0).unwrap(),
        "Should use schema-defined default value when null"
    );
    assert!(
        val.meeting_time.is_none(),
        "Schema-defined optional should remain optional in Rust"
    );
    assert!(
        val.release_datetime_micro
            == chrono::DateTime::<chrono::Utc>::from_timestamp_micros(1681601301000000).unwrap(),
        "Deserialized value is different from payload value"
    );
}
