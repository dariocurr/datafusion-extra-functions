use arrow::util::bench_util::{create_primitive_array, create_string_array};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::{
    arrow::{
        self,
        array::ArrayRef,
        datatypes::{DataType, Int32Type},
    },
    logical_expr::Accumulator,
};
use datafusion_functions_extra::common::mode::{BytesModeAccumulator, PrimitiveModeAccumulator};
use std::hint::black_box;
use std::sync::Arc;

fn prepare_primitive_mode_accumulator() -> Box<dyn Accumulator> {
    Box::new(PrimitiveModeAccumulator::<Int32Type>::new(&DataType::Int32))
}

fn prepare_bytes_mode_accumulator() -> Box<dyn Accumulator> {
    Box::new(BytesModeAccumulator::new(&DataType::Utf8))
}

fn mode_bench_primitive(c: &mut Criterion, name: &str, values: ArrayRef) {
    let mut accumulator = prepare_primitive_mode_accumulator();
    c.bench_function(name, |b| {
        b.iter(|| {
            accumulator.update_batch(&[values.clone()]).unwrap();
            black_box(accumulator.evaluate().unwrap());
        });
    });
}

fn mode_bench_bytes(c: &mut Criterion, name: &str, values: ArrayRef) {
    let mut accumulator = prepare_bytes_mode_accumulator();
    c.bench_function(name, |b| {
        b.iter(|| {
            accumulator.update_batch(&[values.clone()]).unwrap();
            black_box(accumulator.evaluate().unwrap());
        });
    });
}

fn mode_benchmark(c: &mut Criterion) {
    let sizes = [100_000, 1_000_000];
    let null_percentages = [0.0, 0.3, 0.7];

    for &size in &sizes {
        for &null_percentage in &null_percentages {
            let values = Arc::new(create_primitive_array::<Int32Type>(size, null_percentage)) as ArrayRef;
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
            let values = Arc::new(create_string_array::<i32>(size, null_percentage)) as ArrayRef;
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
