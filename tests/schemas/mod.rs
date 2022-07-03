extern crate derive_builder;
extern crate variant_access_derive;
extern crate variant_access_traits;

pub mod complex;
pub mod enums;
pub mod enums_multiline_doc;
#[allow(dead_code)]
pub mod fixed;
pub mod map_default;
pub mod multi_valued_union;
pub mod multi_valued_union_map;
pub mod multi_valued_union_with_avro_rs_unions;
pub mod multi_valued_union_with_variant_access;
pub mod nested_record_default;
pub mod nested_record_partial_default;
pub mod nullable;
pub mod optional_array;
pub mod optional_arrays;
pub mod record;
pub mod record_default;
pub mod record_multiline_doc;
pub mod simple;
pub mod simple_with_builders;
