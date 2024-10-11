# Benchmark results

! To run use: `cargo bench`

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
cache_get_disk: 312.56 ns
cache_get_sccache: 679.03 ns
cache_get_solidcache: 103.49 µs
cache_get_hit_disk: 800.39 µs
cache_get_hit_sccache: 264.21 µs
cache_get_hit_solidcache: 606.35 µs
cache_put_disk: 146.59 ms
cache_put_sccache: 141.81 ms
cache_put_solidcache: 143.19 ms
cache_get_mt/disk: 13.727 µs
cache_get_mt/sccache: 16.921 µs
cache_put_mt/disk: 209.48 ms
cache_put_mt/sccache: 192.42 ms
```

Summary: current implementation compared to sccache has faster misses, but slower hits. solid cache is always slower on all gets

Raw:

```text
     Running unittests src/bin/cache_resiliancy_test.rs (/Users/assafvayner/hf/xet-core/target/release/deps/cache_resiliancy_test-2be7a25162a1f92a)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running benches/cache_bench.rs (/Users/assafvayner/hf/xet-core/target/release/deps/cache_bench-dd797ecaad4ad5cf)
cache_get_disk          time:   [312.04 ns 312.56 ns 313.07 ns]
                        change: [-0.2817% +0.4868% +1.2121%] (p = 0.21 > 0.05)
                        No change in performance detected.
Found 3 outliers among 100 measurements (3.00%)
  2 (2.00%) high mild
  1 (1.00%) high severe

cache_get_sccache       time:   [672.03 ns 679.03 ns 686.55 ns]
                        change: [-1.5370% +1.6150% +5.5661%] (p = 0.44 > 0.05)
                        No change in performance detected.
Found 4 outliers among 100 measurements (4.00%)
  1 (1.00%) high mild
  3 (3.00%) high severe

cache_get_solidcache    time:   [103.14 µs 103.49 µs 103.83 µs]
                        change: [-5.9734% -4.9473% -4.0204%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 12 outliers among 100 measurements (12.00%)
  2 (2.00%) low severe
  4 (4.00%) low mild
  3 (3.00%) high mild
  3 (3.00%) high severe

cache_get_hit_disk      time:   [798.38 µs 800.39 µs 802.69 µs]
                        change: [+4.7304% +5.5839% +6.4705%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 12 outliers among 100 measurements (12.00%)
  10 (10.00%) high mild
  2 (2.00%) high severe

cache_get_hit_sccache   time:   [245.37 µs 264.21 µs 280.90 µs]
                        change: [+11.172% +16.499% +21.758%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 20 outliers among 100 measurements (20.00%)
  20 (20.00%) high severe

cache_get_hit_solidcache
                        time:   [602.09 µs 606.35 µs 610.80 µs]
                        change: [-6.8824% -4.6297% -2.3691%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 7 outliers among 100 measurements (7.00%)
  2 (2.00%) high mild
  5 (5.00%) high severe

cache_put_disk          time:   [146.29 ms 146.59 ms 146.90 ms]
                        change: [+0.1479% +0.5906% +1.0184%] (p = 0.01 < 0.05)
                        Change within noise threshold.
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high mild

cache_put_sccache       time:   [141.48 ms 141.81 ms 142.19 ms]
                        change: [+0.7751% +1.3329% +1.8605%] (p = 0.00 < 0.05)
                        Change within noise threshold.
Found 6 outliers among 100 measurements (6.00%)
  5 (5.00%) high mild
  1 (1.00%) high severe

cache_put_solidcache    time:   [142.35 ms 143.19 ms 144.17 ms]
                        change: [+1.8559% +2.6035% +3.5314%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 3 outliers among 100 measurements (3.00%)
  2 (2.00%) high mild
  1 (1.00%) high severe

cache_get_mt/disk       time:   [13.567 µs 13.727 µs 13.870 µs]
                        change: [-0.6124% +3.6122% +6.9046%] (p = 0.04 < 0.05)
                        Change within noise threshold.
Found 2 outliers among 100 measurements (2.00%)
  1 (1.00%) low severe
  1 (1.00%) low mild

cache_get_mt/sccache    time:   [16.803 µs 16.921 µs 17.040 µs]
                        change: [+3.6952% +5.6220% +7.4356%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 5 outliers among 100 measurements (5.00%)
  2 (2.00%) low mild
  1 (1.00%) high mild
  2 (2.00%) high severe

cache_put_mt/disk       time:   [208.76 ms 209.48 ms 210.23 ms]
                        change: [+6.4973% +7.4875% +8.4682%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high mild

cache_put_mt/sccache    time:   [191.02 ms 192.42 ms 193.99 ms]
                        change: [-2.3362% -1.1473% +0.0861%] (p = 0.08 > 0.05)
                        No change in performance detected.
Found 22 outliers among 100 measurements (22.00%)
  2 (2.00%) high mild
  20 (20.00%) high severe
```
