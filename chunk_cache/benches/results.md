# Benchmark results

SCCache implementation requires an exact range match, the test accounts for this.

get: runs random gets, almost certain to all be misses
get_hit: runs gets guarenteed to be hits
put: before the measuring, the cache is filled so all puts required evictions.
get_mt: multithreaded, each run is 4 gets run asynchronously on 4 spawned tokio tasks.

## Latest on Assaf's M2 Macbook Pro

Summarized:

```text
cache_get_std_1_GB: 317.15 ns
cache_get_sccache: 725.92 ns
cache_get_hit_std_1_GB: 802.49 µs
cache_get_hit_sccache: 226.15 µs
cache_put_std_1_GB: 159.90 ms
cache_put_sccache: 160.64 ms
cache_get_mt_std_1_GB: 9.5891 µs
cache_get_mt_sccache: 11.531 µs
```

Summary: current implementation compared to sccache has faster misses, but slower hits

Raw:

```text
cache_get_std_1_GB      time:   [313.62 ns 317.15 ns 323.18 ns]
                        change: [-1.7216% -0.8301% +0.5408%] (p = 0.15 > 0.05)
                        No change in performance detected.
Found 15 outliers among 100 measurements (15.00%)
  2 (2.00%) low mild
  7 (7.00%) high mild
  6 (6.00%) high severe

cache_get_sccache       time:   [719.92 ns 725.92 ns 731.85 ns]
                        change: [+1.9459% +2.8220% +3.8241%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high mild

cache_get_hit_std_1_GB  time:   [794.26 µs 802.49 µs 811.65 µs]
                        change: [-0.8794% -0.0982% +0.6609%] (p = 0.81 > 0.05)
                        No change in performance detected.
Found 5 outliers among 100 measurements (5.00%)
  1 (1.00%) high mild
  4 (4.00%) high severe

cache_get_hit_sccache   time:   [223.65 µs 226.15 µs 229.10 µs]
                        change: [+6.2922% +7.7773% +9.4043%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 5 outliers among 100 measurements (5.00%)
  3 (3.00%) high mild
  2 (2.00%) high severe

cache_put_std_1_GB      time:   [158.49 ms 159.90 ms 161.51 ms]
                        change: [-8.4353% -7.3834% -6.2071%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 10 outliers among 100 measurements (10.00%)
  6 (6.00%) high mild
  4 (4.00%) high severe

cache_put_sccache       time:   [159.66 ms 160.64 ms 161.81 ms]
                        change: [-6.6452% -5.4062% -4.1045%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 4 outliers among 100 measurements (4.00%)
  1 (1.00%) high mild
  3 (3.00%) high severe

cache_get_mt_std_1_GB/  time:   [9.5020 µs 9.5891 µs 9.6961 µs]
Found 12 outliers among 100 measurements (12.00%)
  1 (1.00%) low severe
  5 (5.00%) high mild
  6 (6.00%) high severe

cache_get_mt_sccache/   time:   [11.424 µs 11.531 µs 11.651 µs]
Found 3 outliers among 100 measurements (3.00%)
  2 (2.00%) high mild
  1 (1.00%) high severe
```
