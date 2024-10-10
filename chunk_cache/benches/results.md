# Benchmark results

! To run use: `cargo bench -F bench`

SCCache implementation requires an exact range match, the test accounts for this.
See SolidCache at the bottom.

get: runs random gets, almost certain to all be misses
get_hit: runs gets guarenteed to be hits
put: before the measuring, the cache is filled so all puts required evictions.
get_mt: multithreaded, each run is 8 gets run asynchronously on 8 spawned tokio tasks.
put_mt: mutlithreaded put, cache is pre-filled, so all puts require evictions, 8 tasks concurrently.

## Latest on Assaf's M2 Macbook Pro

Summarized:

```text
cache_get_disk: 306.68 ns
cache_get_sccache: 699.44 ns
cache_get_solidcache: 108.52 µs
cache_get_hit_disk: 768.17 µs
cache_get_hit_sccache: 190.92 µs
cache_get_hit_solidcache: 639.24 µs
cache_put_disk: 143.50 ms
cache_put_sccache: 138.32 ms
cache_put_solidcache: 139.73 ms
cache_get_mt/disk: 12.320 µs
cache_get_mt/sccache: 15.925 µs
cache_put_mt/disk: 194.89 ms
cache_put_mt/sccache: 194.66 ms
```

Summary: current implementation compared to sccache has faster misses, but slower hits. solid cache is always slower on all gets

Raw:

```text
     Running unittests src/bin/cache_resiliancy_test.rs (/Users/assafvayner/hf/xet-core/target/release/deps/cache_resiliancy_test-2be7a25162a1f92a)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running benches/cache_bench.rs (/Users/assafvayner/hf/xet-core/target/release/deps/cache_bench-dd797ecaad4ad5cf)
cache_get_disk          time:   [305.47 ns 306.68 ns 308.12 ns]
                        change: [-1.7675% -1.1644% -0.5527%] (p = 0.00 < 0.05)
                        Change within noise threshold.
Found 11 outliers among 100 measurements (11.00%)
  5 (5.00%) high mild
  6 (6.00%) high severe

cache_get_sccache       time:   [693.65 ns 699.44 ns 705.13 ns]
                        change: [+2.5004% +3.3652% +4.2747%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high mild

cache_get_solidcache    time:   [107.46 µs 108.52 µs 109.82 µs]
                        change: [-1.9770% -0.1215% +2.5478%] (p = 0.92 > 0.05)
                        No change in performance detected.
Found 12 outliers among 100 measurements (12.00%)
  1 (1.00%) low severe
  6 (6.00%) high mild
  5 (5.00%) high severe

cache_get_hit_disk      time:   [764.35 µs 768.17 µs 772.64 µs]
                        change: [-1.4676% -0.8421% -0.2313%] (p = 0.01 < 0.05)
                        Change within noise threshold.
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high severe

cache_get_hit_sccache   time:   [189.60 µs 190.92 µs 192.29 µs]
                        change: [-5.9041% -3.7603% -1.7176%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 8 outliers among 100 measurements (8.00%)
  6 (6.00%) high mild
  2 (2.00%) high severe

cache_get_hit_solidcache
                        time:   [632.55 µs 639.24 µs 645.96 µs]
                        change: [-2.5973% -1.1670% +0.1879%] (p = 0.11 > 0.05)
                        No change in performance detected.
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high severe

cache_put_disk          time:   [143.15 ms 143.50 ms 143.87 ms]
                        change: [-0.5999% -0.2079% +0.1703%] (p = 0.30 > 0.05)
                        No change in performance detected.
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high mild

cache_put_sccache       time:   [137.89 ms 138.32 ms 138.78 ms]
                        change: [-0.5939% -0.1998% +0.2238%] (p = 0.36 > 0.05)
                        No change in performance detected.
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high mild

cache_put_solidcache    time:   [138.73 ms 139.73 ms 140.85 ms]
                        change: [+0.9078% +1.6841% +2.5619%] (p = 0.00 < 0.05)
                        Change within noise threshold.
Found 9 outliers among 100 measurements (9.00%)
  4 (4.00%) high mild
  5 (5.00%) high severe

cache_get_mt/disk       time:   [12.236 µs 12.320 µs 12.426 µs]
                        change: [-1.7576% -0.4392% +0.8819%] (p = 0.53 > 0.05)
                        No change in performance detected.
Found 16 outliers among 100 measurements (16.00%)
  8 (8.00%) high mild
  8 (8.00%) high severe

cache_get_mt/sccache    time:   [15.732 µs 15.925 µs 16.129 µs]
                        change: [-0.6044% +1.0649% +2.7932%] (p = 0.23 > 0.05)
                        No change in performance detected.
Found 7 outliers among 100 measurements (7.00%)
  5 (5.00%) high mild
  2 (2.00%) high severe

cache_put_mt/disk       time:   [193.27 ms 194.89 ms 196.59 ms]
                        change: [+2.2292% +3.2226% +4.3099%] (p = 0.00 < 0.05)
                        Performance has regressed.

cache_put_mt/sccache    time:   [192.84 ms 194.66 ms 196.65 ms]
Found 6 outliers among 100 measurements (6.00%)
  5 (5.00%) high mild
  1 (1.00%) high severe
```
