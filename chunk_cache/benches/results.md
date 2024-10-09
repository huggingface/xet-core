# Benchmark results

SCCache implementation requires an exact range match, the test accounts for this.

get: runs random gets, almost certain to all be misses
get_hit: runs gets guarenteed to be hits
put: benchmarks puts; this test is less accurate as the cache can get filled up and later evictions
  can cause for put slowdowns

## Latest on Assaf's M2 Macbook Pro

Summarized:

```text
cache_get_std_1_GB: 301.31 ns
cache_get_sccache: 682.33 ns
cache_get_hit_std_1_GB: 5.8981 ms
cache_get_hit_sccache: 2.1029 ms
cache_put_std_1_GB: 152.29 ms
cache_put_sccache: 147.67 ms
```

Raw:

```text
     Running benches/cache_bench.rs (/Users/assafvayner/hf/xet-core/target/release/deps/cache_bench-744086e161f5c0f4)
cache_get_std_1_GB      time:   [299.60 ns 301.31 ns 303.18 ns]
                        change: [-3.1707% -2.5619% -1.8745%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 17 outliers among 100 measurements (17.00%)
  9 (9.00%) high mild
  8 (8.00%) high severe

cache_get_sccache       time:   [676.31 ns 682.33 ns 688.60 ns]
                        change: [-4.7133% -2.2257% -0.4238%] (p = 0.03 < 0.05)
                        Change within noise threshold.

cache_get_hit_std_1_GB  time:   [5.8783 ms 5.8981 ms 5.9225 ms]
                        change: [+1.0984% +1.6728% +2.2407%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 7 outliers among 100 measurements (7.00%)
  3 (3.00%) high mild
  4 (4.00%) high severe

cache_get_hit_sccache   time:   [2.0719 ms 2.1029 ms 2.1388 ms]
                        change: [-2.4425% -0.6005% +1.4956%] (p = 0.54 > 0.05)
                        No change in performance detected.
Found 13 outliers among 100 measurements (13.00%)
  8 (8.00%) high mild
  5 (5.00%) high severe

Benchmarking cache_put_std_1_GB: Warming up for 3.0000 s
Warning: Unable to complete 100 samples in 15.0s. You may wish to increase target time to 15.3s, or reduce sample count to 90.
```
