use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use vaultgres::storage::btree::{BTree, TupleId};
use vaultgres::storage::{BufferPool, PageId};

fn bench_buffer_pool_fetch(c: &mut Criterion) {
    let pool = BufferPool::new(100);
    c.bench_function("buffer_pool_fetch", |b| {
        b.iter(|| {
            pool.fetch(black_box(PageId(1))).unwrap();
            pool.unpin(black_box(PageId(1)), false).unwrap();
        });
    });
}

fn bench_btree_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree_insert");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut tree = BTree::new();
                for i in 0..size {
                    let key = vec![i as u8];
                    let value = TupleId { page_id: PageId(i as u32), slot: 0 };
                    tree.insert(key, value).unwrap();
                }
            });
        });
    }

    group.finish();
}

fn bench_btree_lookup(c: &mut Criterion) {
    let mut tree = BTree::new();
    for i in 0..10000 {
        let key = vec![(i % 256) as u8];
        let value = TupleId { page_id: PageId(i), slot: 0 };
        tree.insert(key, value).unwrap();
    }

    c.bench_function("btree_lookup", |b| {
        b.iter(|| {
            let key = vec![black_box(128)];
            tree.get(&key)
        });
    });
}

criterion_group!(benches, bench_buffer_pool_fetch, bench_btree_insert, bench_btree_lookup);
criterion_main!(benches);
