use criterion::{black_box, criterion_group, criterion_main, Criterion};
use apache_avro::Schema;

// Simulate old behavior: parse every time
fn get_schema_no_cache() -> Schema {
    Schema::parse_str(r#"{"name":"A","type":"record","fields":[{"name":"item","type":["null","long","double",{"name":"B","type":"record","fields":[{"name":"pass","type":"long"},{"name":"direction","type":{"name":"C","type":"enum","symbols":["U","S","D"]}},{"name":"depth","type":"double"}]}]}]}"#).expect("parsing of canonical form cannot fail")
}

// New behavior: cache with LazyLock
fn get_schema_with_cache() -> Schema {
    static SCHEMA: ::std::sync::LazyLock<Schema> = ::std::sync::LazyLock::new(|| {
        Schema::parse_str(r#"{"name":"A","type":"record","fields":[{"name":"item","type":["null","long","double",{"name":"B","type":"record","fields":[{"name":"pass","type":"long"},{"name":"direction","type":{"name":"C","type":"enum","symbols":["U","S","D"]}},{"name":"depth","type":"double"}]}]}]}"#).expect("parsing of canonical form cannot fail")
    });
    SCHEMA.clone()
}

fn benchmark_schema_access(c: &mut Criterion) {
    c.bench_function("get_schema_no_cache", |b| {
        b.iter(|| {
            black_box(get_schema_no_cache());
        })
    });

    c.bench_function("get_schema_with_cache", |b| {
        b.iter(|| {
            black_box(get_schema_with_cache());
        })
    });
}

criterion_group!(benches, benchmark_schema_access);
criterion_main!(benches);
