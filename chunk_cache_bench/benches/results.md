# Benchmark results

! To run use: `cargo bench`

SCCache implementation requires an exact range match, the test accounts for this.
See SolidCache at the bottom.

get: runs random gets, almost certain to all be misses
get_hit: runs gets guaranteed to be hits
put: before the measuring, the cache is filled so all puts required evictions.
get_mt: multithreaded, each run is 8 gets run asynchronously on 8 spawned tokio tasks.
put_mt: multithreaded put, cache is pre-filled, so all puts require evictions, 8 tasks concurrently.

## Latest on Assaf's M2 Macbook Pro

### Summarized

```text
cache_get_disk: 302.49 ns
cache_get_sccache: 676.06 ns
cache_get_solidcache: 106.57 µs
cache_get_hit_disk: 674.79 µs
cache_get_hit_sccache: 193.26 µs
cache_get_hit_solidcache: 619.22 µs
cache_put_disk: 143.20 ms
cache_put_sccache: 137.96 ms
cache_put_solidcache: 138.68 ms
cache_get_mt/disk: 12.656 µs
cache_get_mt/sccache: 15.327 µs
cache_put_mt/disk: 196.23 ms
cache_put_mt/sccache: 194.32 ms
```

- misses: current implementation compared to sccache & solidcache.
- hits: current implementation is slower on hits compared to sccache.
  - there are a lot of factors that could affect this and introduce room for later improvement
    - effect of LRU vs random eviction
    - does sccache validate file contents like we do with the blake3 hash, maybe we do extra passes on data in comparison
    - does our splicing of the requested data range add a lot of extra time? (sccache is direct kv store)
- put: speeds are comparable across all implementations
- multithreaded: current impl on par with sccache.
- misc: solid cache is slower than standard by a factor of 300x on cache misses.

### Raw

```text
     Running benches/cache_bench.rs (/Users/assafvayner/hf/xet-core/target/release/deps/cache_bench-acb21e9a66cc1664)
cache_get_disk          time:   [301.85 ns 302.49 ns 303.24 ns]
                        change: [-4.0902% -3.4202% -2.8684%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 10 outliers among 100 measurements (10.00%)
  4 (4.00%) high mild
  6 (6.00%) high severe

cache_get_sccache       time:   [667.37 ns 676.06 ns 683.48 ns]
                        change: [-5.5258% -4.4523% -3.4316%] (p = 0.00 < 0.05)
                        Performance has improved.

cache_get_solidcache    time:   [105.92 µs 106.57 µs 107.29 µs]
                        change: [-4.0546% -3.4103% -2.7400%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 4 outliers among 100 measurements (4.00%)
  1 (1.00%) low mild
  2 (2.00%) high mild
  1 (1.00%) high severe

cache_get_hit_disk      time:   [670.96 µs 674.79 µs 679.30 µs]
                        change: [-14.498% -13.298% -12.252%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 6 outliers among 100 measurements (6.00%)
  4 (4.00%) high mild
  2 (2.00%) high severe

cache_get_hit_sccache   time:   [191.48 µs 193.26 µs 195.39 µs]
                        change: [-6.1415% -4.9539% -3.4732%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 8 outliers among 100 measurements (8.00%)
  4 (4.00%) high mild
  4 (4.00%) high severe

cache_get_hit_solidcache
                        time:   [611.71 µs 619.22 µs 627.41 µs]
                        change: [-2.1092% -0.8433% +0.4552%] (p = 0.17 > 0.05)
                        No change in performance detected.
Found 9 outliers among 100 measurements (9.00%)
  8 (8.00%) high mild
  1 (1.00%) high severe

cache_put_disk          time:   [142.72 ms 143.20 ms 143.71 ms]
                        change: [-5.7893% -4.8375% -3.9496%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 9 outliers among 100 measurements (9.00%)
  9 (9.00%) high mild

cache_put_sccache       time:   [137.57 ms 137.96 ms 138.39 ms]
                        change: [+0.6631% +1.0843% +1.5067%] (p = 0.00 < 0.05)
                        Change within noise threshold.
Found 8 outliers among 100 measurements (8.00%)
  7 (7.00%) high mild
  1 (1.00%) high severe

cache_put_solidcache    time:   [138.07 ms 138.68 ms 139.35 ms]
                        change: [+1.6395% +2.1834% +2.7363%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 4 outliers among 100 measurements (4.00%)
  3 (3.00%) high mild
  1 (1.00%) high severe

cache_get_mt/disk       time:   [12.570 µs 12.656 µs 12.781 µs]
                        change: [+3.0291% +3.5879% +4.3416%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 7 outliers among 100 measurements (7.00%)
  3 (3.00%) high mild
  4 (4.00%) high severe

cache_get_mt/sccache    time:   [15.207 µs 15.327 µs 15.473 µs]
                        change: [-2.5502% -1.6692% -0.7229%] (p = 0.00 < 0.05)
                        Change within noise threshold.
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high mild

cache_put_mt/disk       time:   [194.52 ms 196.23 ms 198.10 ms]
                        change: [+1.8060% +2.8133% +3.8883%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 4 outliers among 100 measurements (4.00%)
  3 (3.00%) high mild
  1 (1.00%) high severe

cache_put_mt/sccache    time:   [192.82 ms 194.32 ms 195.94 ms]
                        change: [+1.2712% +2.1359% +3.0719%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 6 outliers among 100 measurements (6.00%)
  6 (6.00%) high mild
```

## SolidCache table setup

The implementation in solid_cache.rs expects the following configured attributes: `host=localhost user=postgres dbname=cache`.

Below are the scripts and schemas to create the tables.

Note that for evictions we create a function and a trigger on the cache table.

### Summary of below

```sql
-- create the table

CREATE TABLE public.cache (
    key character varying(128) NOT NULL,
    start integer NOT NULL,
    "end" integer NOT NULL,
    chunk_byte_indicies integer[] NOT NULL,
    data bytea NOT NULL
);
```

```sql
-- cleaning function
CREATE OR REPLACE FUNCTION delete_random_if_full() 
RETURNS TRIGGER AS $$
BEGIN
    -- Check if the number of rows is greater than or equal to 1000
    IF (SELECT COUNT(*) FROM cache) >= 1000 THEN
        -- Delete a random row
        DELETE FROM cache 
        WHERE id = (SELECT id FROM cache ORDER BY random() LIMIT 1);
    END IF;
    RETURN NEW;
END;
```

```sql
-- create the trigger to evict data
CREATE TRIGGER maintain_size_trigger AFTER INSERT ON cache FOR EACH ROW EXECUTE FUNCTION delete_random_if_full();
```

### Entire backup for the table setup

*This was run on postgres running on macos using the postgres gui app.*

For this script to work you will also need to create the following function before creating the trigger

```sql
CREATE OR REPLACE FUNCTION delete_random_if_full() 
RETURNS TRIGGER AS $$
BEGIN
    -- Check if the number of rows is greater than or equal to 1000
    IF (SELECT COUNT(*) FROM cache) >= 1000 THEN
        -- Delete a random row
        DELETE FROM cache 
        WHERE id = (SELECT id FROM cache ORDER BY random() LIMIT 1);
    END IF;
    RETURN NEW;
END;
```

#### The following is a dump from pgAdmin 4

```sql
--
-- PostgreSQL database dump
--

-- Dumped from database version 16.4 (Postgres.app)
-- Dumped by pg_dump version 16.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: cache; Type: DATABASE; Schema: -; Owner: postgres
--

CREATE DATABASE cache WITH TEMPLATE = template0 ENCODING = 'UTF8' LOCALE_PROVIDER = libc LOCALE = 'en_US.UTF-8';


ALTER DATABASE cache OWNER TO postgres;

\connect cache

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: cache; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.cache (
    key character varying(128) NOT NULL,
    start integer NOT NULL,
    "end" integer NOT NULL,
    chunk_byte_indicies integer[] NOT NULL,
    data bytea NOT NULL
);


ALTER TABLE public.cache OWNER TO postgres;

--
-- Name: cache cache_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cache
    ADD CONSTRAINT cache_pkey PRIMARY KEY (key);


--
-- Name: cache maintain_size_trigger; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER maintain_size_trigger AFTER INSERT ON public.cache FOR EACH ROW EXECUTE FUNCTION public.delete_random_if_full();


--
-- PostgreSQL database dump complete
--
```

## Ajit's results

> m1 results - similar to above trends

```text
     Running benches/cache_bench.rs (target/release/deps/cache_bench-18100c02f1a37ee6)
Benchmarking cache_get_disk: Collecting 100 samples in estimated 5.0002 s (18M i
cache_get_disk          time:   [266.89 ns 267.62 ns 268.44 ns]
                        change: [+2.8393% +3.2068% +3.5712%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 11 outliers among 100 measurements (11.00%)
  9 (9.00%) high mild
  2 (2.00%) high severe

Benchmarking cache_get_sccache: Collecting 100 samples in estimated 5.0009 s (10
cache_get_sccache       time:   [486.10 ns 487.96 ns 490.31 ns]
                        change: [+0.8240% +1.1720% +1.5348%] (p = 0.00 < 0.05)
                        Change within noise threshold.
Found 6 outliers among 100 measurements (6.00%)
  3 (3.00%) high mild
  3 (3.00%) high severe

Benchmarking cache_get_solidcache: Collecting 100 samples in estimated 5.0562 s
cache_get_solidcache    time:   [100.82 µs 101.65 µs 102.59 µs]
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high mild

Benchmarking cache_get_hit_disk: Collecting 100 samples in estimated 30.439 s (4
cache_get_hit_disk      time:   [748.58 µs 751.81 µs 755.62 µs]
Found 16 outliers among 100 measurements (16.00%)
  7 (7.00%) low mild
  4 (4.00%) high mild
  5 (5.00%) high severe

Benchmarking cache_get_hit_sccache: Collecting 100 samples in estimated 30.142 s
cache_get_hit_sccache   time:   [224.39 µs 227.03 µs 229.78 µs]
Found 8 outliers among 100 measurements (8.00%)
  6 (6.00%) high mild
  2 (2.00%) high severe

Benchmarking cache_get_hit_solidcache: Collecting 100 samples in estimated 31.24
cache_get_hit_solidcache
                        time:   [543.03 µs 552.37 µs 563.47 µs]
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high mild

Benchmarking cache_put_disk: Collecting 100 samples in estimated 31.855 s (200 i
cache_put_disk          time:   [160.87 ms 161.46 ms 162.09 ms]
Found 3 outliers among 100 measurements (3.00%)
  3 (3.00%) high mild

Benchmarking cache_put_sccache: Collecting 100 samples in estimated 31.554 s (20
cache_put_sccache       time:   [154.95 ms 155.84 ms 156.81 ms]
Found 13 outliers among 100 measurements (13.00%)
  5 (5.00%) high mild
  8 (8.00%) high severe

Benchmarking cache_put_solidcache: Collecting 100 samples in estimated 37.927 s
cache_put_solidcache    time:   [189.04 ms 191.27 ms 194.19 ms]
Found 9 outliers among 100 measurements (9.00%)
  4 (4.00%) high mild
  5 (5.00%) high severe

Benchmarking cache_get_mt/disk: Collecting 100 samples in estimated 5.0415 s (50
cache_get_mt/disk       time:   [10.003 µs 10.085 µs 10.211 µs]
Found 4 outliers among 100 measurements (4.00%)
  1 (1.00%) low mild
  2 (2.00%) high mild
  1 (1.00%) high severe

Benchmarking cache_get_mt/sccache: Collecting 100 samples in estimated 5.0613 s
cache_get_mt/sccache    time:   [12.928 µs 13.221 µs 13.566 µs]
Found 7 outliers among 100 measurements (7.00%)
  4 (4.00%) high mild
  3 (3.00%) high severe

Benchmarking cache_put_mt/disk: Collecting 100 samples in estimated 51.927 s (20
cache_put_mt/disk       time:   [256.95 ms 258.25 ms 259.65 ms]
Found 8 outliers among 100 measurements (8.00%)
  7 (7.00%) high mild
  1 (1.00%) high severe

Benchmarking cache_put_mt/sccache: Collecting 100 samples in estimated 53.090 s
cache_put_mt/sccache    time:   [254.23 ms 255.07 ms 256.07 ms]
Found 13 outliers among 100 measurements (13.00%)
  1 (1.00%) low mild
  7 (7.00%) high mild
  5 (5.00%) high severe
```
