use std::sync::Arc;

use codecrafters_redis::storage::Storage;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

// ── helpers ────────────────────────────────────────────────────────────────

fn make_tokio_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("failed to build Tokio runtime")
}

/// Populate a list key with `size` string elements.
async fn seed_list(storage: &Storage, key: &str, size: usize) {
    let batch: Vec<String> = (0..size).map(|i| format!("item-{i}")).collect();
    storage.append(key.to_string(), batch).await;
}

/// Populate a stream key with `size` entries (each entry has one field "v" = "<i>").
async fn seed_stream(storage: &Storage, key: &str, size: usize) {
    for i in 1..=(size as i64) {
        storage
            .add_to_stream(key, Some(i), Some(0), vec![("v".into(), i.to_string())])
            .await
            .expect("seed_stream: add_to_stream failed");
    }
}

// ── LRANGE benchmarks ──────────────────────────────────────────────────────

fn bench_lrange(c: &mut Criterion) {
    let rt = make_tokio_rt();

    const DATASET_SIZES: &[usize] = &[100, 1_000, 10_000];

    // (label, start_fn, end_fn): closures receive the list length
    let range_configs: &[(&str, fn(usize) -> i64, fn(usize) -> i64)] = &[
        ("full", |_| 0, |n| n as i64 - 1),
        ("half", |_| 0, |n| (n / 2) as i64),
        ("first_10pct", |_| 0, |n| (n / 10).max(1) as i64),
    ];

    let mut group = c.benchmark_group("lrange");
    group.sample_size(50);

    for &size in DATASET_SIZES {
        for &(range_label, start_fn, end_fn) in range_configs {
            let id = BenchmarkId::new(range_label, size);
            let start = start_fn(size);
            let end = end_fn(size);

            // Build and seed a fresh storage once per parameter combination.
            let storage = Arc::new(Storage::new());
            rt.block_on(seed_list(&storage, "bench-list", size));

            group.bench_with_input(id, &size, |b, _| {
                let storage = storage.clone();
                b.to_async(&rt).iter(|| async {
                    let result = storage
                        .get_list_range("bench-list", start, end)
                        .await;
                    std::hint::black_box(result)
                });
            });
        }
    }

    group.finish();
}

// ── XRANGE benchmarks ──────────────────────────────────────────────────────

fn bench_xrange(c: &mut Criterion) {
    let rt = make_tokio_rt();

    const DATASET_SIZES: &[usize] = &[100, 1_000, 10_000];

    // Min/max bounds for a full-range XRANGE (same as "-" / "+" in RESP)
    let min_bound = (0_i64, Some(0_i64));
    let max_bound = (i64::MAX, Some(i64::MAX));

    let mut group = c.benchmark_group("xrange");
    group.sample_size(50);

    for &size in DATASET_SIZES {
        let id = BenchmarkId::new("full_range", size);

        let storage = Arc::new(Storage::new());
        rt.block_on(seed_stream(&storage, "bench-stream", size));

        group.bench_with_input(id, &size, |b, _| {
            let storage = storage.clone();
            b.to_async(&rt).iter(|| async {
                let result = storage
                    .with_stream_range("bench-stream", min_bound, max_bound, |entries| {
                        entries.count()
                    })
                    .await;
                std::hint::black_box(result)
            });
        });
    }

    group.finish();
}

// ── registration ───────────────────────────────────────────────────────────

criterion_group!(benches, bench_lrange, bench_xrange);
criterion_main!(benches);
