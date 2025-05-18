#[rustfmt::skip]
mod schemas;

use pretty_assertions::assert_eq;
use rsgen_avro::{Generator, Source};

fn validate_generation(file_name: &str, g: Generator) {
    let schema = format!("tests/schemas/{file_name}.avsc");
    let src = Source::GlobPattern(&schema);
    let mut buf = vec![];
    g.generate(&src, &mut buf).unwrap();

    let generated = String::from_utf8(buf).unwrap();
    let expected = std::fs::read_to_string(format!("tests/schemas/{file_name}.rs")).unwrap();
    validate(expected, generated)
}

fn validate(expected: String, generated: String) {
    assert_eq!(
        expected, generated,
        "\n\n>>>>>>>>>>>>>>>>> Expected: \n{}\n>>>>>>>>>>>>>>>>> But generated: \n{}",
        expected, generated
    );
}

#[test]
fn gen_simple() {
    validate_generation("simple", Generator::new().unwrap());
}

#[test]
fn gen_simple_with_builder() {
    validate_generation(
        "simple_with_builders",
        Generator::builder().derive_builders(true).build().unwrap(),
    );
}

#[test]
fn gen_simple_with_schema() {
    validate_generation(
        "simple_with_schemas",
        Generator::builder().derive_schemas(true).build().unwrap(),
    );
}

#[test]
fn gen_complex() {
    validate_generation("complex", Generator::new().unwrap());
}

#[test]
fn gen_optional_array() {
    validate_generation("optional_array", Generator::new().unwrap());
}

#[test]
fn gen_optional_arrays() {
    validate_generation("optional_arrays", Generator::new().unwrap());
}

#[test]
fn gen_array_3d() {
    validate_generation("array_3d", Generator::new().unwrap());
}

#[test]
fn gen_mono_valued_union() {
    validate_generation("mono_valued_union", Generator::new().unwrap());
}

#[test]
fn gen_multi_valued_union() {
    validate_generation("multi_valued_union", Generator::new().unwrap());
}

#[test]
fn gen_multi_valued_union_map() {
    validate_generation("multi_valued_union_map", Generator::new().unwrap());
}

#[test]
fn gen_multi_valued_union_nested() {
    let schemas = "tests/schemas/multi_valued_union_nested_*.avsc";
    let src = Source::GlobPattern(schemas);
    let mut buf = vec![];
    Generator::new().unwrap().generate(&src, &mut buf).unwrap();
    let generated = String::from_utf8(buf).unwrap();
    let expected = std::fs::read_to_string("tests/schemas/multi_valued_union_nested.rs").unwrap();
    validate(expected, generated)
}

#[test]
fn gen_multi_valued_union_records() {
    validate_generation("multi_valued_union_records", Generator::new().unwrap());
}

#[test]
fn gen_multi_valued_union_with_avro_rs_unions() {
    validate_generation(
        "multi_valued_union_with_avro_rs_unions",
        Generator::builder()
            .use_avro_rs_unions(true)
            .build()
            .unwrap(),
    );
}

#[test]
fn gen_nullable() {
    validate_generation(
        "nullable",
        Generator::builder().nullable(true).build().unwrap(),
    );
}

#[test]
fn gen_nullable_bytes() {
    validate_generation(
        "nullable_bytes",
        Generator::builder().nullable(true).build().unwrap(),
    );
}

#[test]
fn gen_nullable_logical_dates() {
    validate_generation(
        "nullable_logical_dates",
        Generator::builder().nullable(true).build().unwrap(),
    );
}

#[test]
fn gen_nullable_chrono_logical_dates() {
    validate_generation(
        "nullable_chrono_logical_dates",
        Generator::builder()
            .nullable(true)
            .use_chrono_dates(true)
            .build()
            .unwrap(),
    );
}

#[test]
fn gen_decimals() {
    validate_generation("decimals", Generator::builder().build().unwrap());
}

#[test]
fn gen_logical_dates() {
    validate_generation("logical_dates", Generator::builder().build().unwrap());
}

#[test]
fn gen_chrono_logical_dates() {
    validate_generation(
        "chrono_logical_dates",
        Generator::builder().use_chrono_dates(true).build().unwrap(),
    );
}

#[test]
fn gen_record() {
    validate_generation("record", Generator::new().unwrap());
}

#[test]
fn gen_record_default() {
    validate_generation("record_default", Generator::new().unwrap());
}

#[test]
fn gen_record_multiline_doc() {
    validate_generation("record_multiline_doc", Generator::new().unwrap());
}

#[test]
fn gen_nested_record_default() {
    validate_generation("nested_record_default", Generator::new().unwrap());
}

#[test]
fn gen_nested_record_partial_default() {
    validate_generation("nested_record_partial_default", Generator::new().unwrap());
}

#[test]
fn gen_map_default() {
    validate_generation("map_default", Generator::new().unwrap());
}

#[test]
fn gen_map_multiple_def() {
    validate_generation("map_multiple_def", Generator::new().unwrap());
}

#[test]
fn gen_enums() {
    validate_generation("enums", Generator::new().unwrap());
}

#[test]
fn gen_enums_casing() {
    validate_generation("enums_casing", Generator::new().unwrap());
}

#[test]
fn gen_enums_multiline_doc() {
    validate_generation("enums_multiline_doc", Generator::new().unwrap());
}

#[test]
fn gen_fixed() {
    validate_generation("fixed", Generator::new().unwrap());
}

#[test]
fn gen_nested_with_float() {
    validate_generation("nested_with_float", Generator::new().unwrap());
}

#[test]
fn gen_recursive() {
    validate_generation("recursive", Generator::new().unwrap());
}

#[test]
fn gen_interop() {
    validate_generation("interop", Generator::new().unwrap());
}

#[test]
fn gen_one_extra_derives() {
    validate_generation(
        "one_extra_derive",
        Generator::builder()
            .extra_derives(vec!["std::fmt::Display".to_string()])
            .build()
            .unwrap(),
    );
}

#[test]
fn gen_two_extra_derives() {
    validate_generation(
        "two_extra_derives",
        Generator::builder()
            .extra_derives(vec![
                "std::fmt::Display".to_string(),
                "std::string::ToString".to_string(),
            ])
            .build()
            .unwrap(),
    );
}
