use arrow::util::bench_util::{create_primitive_array, create_string_array};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::{arrow, logical_expr::Accumulator};
use datafusion_functions_extra::common::mode::{BytesModeAccumulator, PrimitiveModeAccumulator};
use std::{hint, sync};

fn prepare_primitive_mode_accumulator() -> Box<dyn Accumulator> {
    Box::new(
        PrimitiveModeAccumulator::<arrow::datatypes::Int32Type>::new(
            &arrow::datatypes::DataType::Int32,
        ),
    )
}

fn prepare_bytes_mode_accumulator() -> Box<dyn Accumulator> {
    Box::new(BytesModeAccumulator::new(&arrow::datatypes::DataType::Utf8))
}

fn mode_bench_primitive(c: &mut Criterion, name: &str, values: arrow::array::ArrayRef) {
    let mut accumulator = prepare_primitive_mode_accumulator();
    c.bench_function(name, |b| {
        b.iter(|| {
            accumulator.update_batch(&[values.clone()]).unwrap();
            hint::black_box(accumulator.evaluate().unwrap());
        });
    });
}

fn mode_bench_bytes(c: &mut Criterion, name: &str, values: arrow::array::ArrayRef) {
    let mut accumulator = prepare_bytes_mode_accumulator();
    c.bench_function(name, |b| {
        b.iter(|| {
            accumulator.update_batch(&[values.clone()]).unwrap();
            hint::black_box(accumulator.evaluate().unwrap());
        });
    });
}

fn mode_benchmark(c: &mut Criterion) {
    let sizes = [100_000, 1_000_000];
    let null_percentages = [0.0, 0.3, 0.7];

    for &size in &sizes {
        for &null_percentage in &null_percentages {
            let values = sync::Arc::new(create_primitive_array::<arrow::datatypes::Int32Type>(
                size,
                null_percentage,
            )) as arrow::array::ArrayRef;
            let name = format!(
                "PrimitiveModeAccumulator: {} elements, {}% nulls",
                size,
                null_percentage * 100.0
            );
            mode_bench_primitive(c, &name, values);
        }
    }

    for &size in &sizes {
        for &null_percentage in &null_percentages {
            let values = sync::Arc::new(create_string_array::<i32>(size, null_percentage))
                as arrow::array::ArrayRef;
            let name = format!(
                "BytesModeAccumulator: {} elements, {}% nulls",
                size,
                null_percentage * 100.0
            );
            mode_bench_bytes(c, &name, values);
        }
    }
}

criterion_group!(benches, mode_benchmark);
criterion_main!(benches);
