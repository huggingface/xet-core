# Benchmark results

SCCache implementation requires an exact range match, the test accounts for this.

get: runs random gets, almost certain to all be misses
get_hit: runs gets guarenteed to be hits
put: before the measuring, the cache is filled so all puts required evictions.
get_mt: multithreaded, each run is 8 gets run asynchronously on 8 spawned tokio tasks.
put_mt: mutlithreaded put, cache is pre-filled, so all puts require evictions, 8 tasks concurrently.

## Latest on Assaf's M2 Macbook Pro

Summarized:

```text
cache_get_std_1_GB: 314.53 ns
cache_get_sccache: 691.71 ns
cache_get_hit_std_1_GB: 780.54 µs
cache_get_hit_sccache: 197.97 µs
cache_put_std_1_GB: 130.54 ms
cache_put_sccache: 122.87 ms
cache_get_mt/std_1_GB: 12.236 µs
cache_get_mt/sccache: 16.222 µs
cache_put_mt/std_1_GB: 220.85 ms
cache_put_mt/sccache: 218.82 ms
```

Summary: current implementation compared to sccache has faster misses, but slower hits

Raw:

```text
     Running unittests src/bin/cache_resiliancy_test.rs (/Users/assafvayner/hf/xet-core/target/release/deps/cache_resiliancy_test-1a9e8997cb2b07aa)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running benches/cache_bench.rs (/Users/assafvayner/hf/xet-core/target/release/deps/cache_bench-610aadf20abf5452)
cache_get_std_1_GB      time:   [312.59 ns 314.53 ns 316.66 ns]
                        change: [+21.781% +22.810% +23.797%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 4 outliers among 100 measurements (4.00%)
  4 (4.00%) high mild

cache_get_sccache       time:   [684.70 ns 691.71 ns 698.51 ns]
                        change: [+11.059% +12.152% +13.177%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 4 outliers among 100 measurements (4.00%)
  4 (4.00%) high mild

cache_get_hit_std_1_GB  time:   [778.33 µs 780.54 µs 782.78 µs]
                        change: [+1.5561% +2.1907% +2.8432%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 6 outliers among 100 measurements (6.00%)
  4 (4.00%) high mild
  2 (2.00%) high severe

cache_get_hit_sccache   time:   [196.34 µs 197.97 µs 199.75 µs]
                        change: [+0.5284% +1.9270% +3.3375%] (p = 0.01 < 0.05)
                        Change within noise threshold.
Found 3 outliers among 100 measurements (3.00%)
  2 (2.00%) high mild
  1 (1.00%) high severe

cache_put_std_1_GB      time:   [129.95 ms 130.54 ms 131.18 ms]
                        change: [-14.703% -14.191% -13.668%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 2 outliers among 100 measurements (2.00%)
  1 (1.00%) high mild
  1 (1.00%) high severe

cache_put_sccache       time:   [122.42 ms 122.87 ms 123.34 ms]
                        change: [-16.653% -16.263% -15.827%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high mild

cache_get_mt/std_1_GB   time:   [12.169 µs 12.236 µs 12.304 µs]
                        change: [-1.2254% +0.0094% +1.2885%] (p = 0.99 > 0.05)
                        No change in performance detected.
Found 7 outliers among 100 measurements (7.00%)
  3 (3.00%) high mild
  4 (4.00%) high severe

cache_get_mt/sccache    time:   [16.051 µs 16.222 µs 16.397 µs]
                        change: [+0.6889% +2.0261% +3.4757%] (p = 0.01 < 0.05)
                        Change within noise threshold.
Found 8 outliers among 100 measurements (8.00%)
  6 (6.00%) high mild
  2 (2.00%) high severe

cache_put_mt/std_1_GB   time:   [219.94 ms 220.85 ms 221.84 ms]
Found 5 outliers among 100 measurements (5.00%)
  5 (5.00%) high mild

cache_put_mt/sccache    time:   [218.19 ms 218.82 ms 219.54 ms]
                        change: [-0.7315% -0.2334% +0.2445%] (p = 0.37 > 0.05)
                        No change in performance detected.
Found 18 outliers among 100 measurements (18.00%)
  9 (9.00%) high mild
  9 (9.00%) high severe
```
