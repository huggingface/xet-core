# API Update: V2 Reconstruction with Multi-Range Fetch Support (2026-03-16)

## Overview

The CAS reconstruction API now supports a V2 endpoint that returns optimized
multi-range fetch descriptors.  The client auto-detects V2 and falls back to V1
transparently.  Two new config options control reconstruction behavior.

---

## 1. New CAS Endpoint

`GET /v2/reconstructions/{file_id}` returns `QueryReconstructionResponseV2`:

```json
{
  "terms": [...],
  "offset_into_first_range": 0,
  "xorbs": {
    "<hex_hash>": [
      {
        "url": "https://...",
        "ranges": [
          { "chunks": { "start": 0, "end": 3 }, "bytes": { "start": 0, "end": 1023 } },
          { "chunks": { "start": 5, "end": 8 }, "bytes": { "start": 2048, "end": 3071 } }
        ]
      }
    ]
  }
}
```

Each `XorbMultiRangeFetch` entry groups multiple disjoint chunk ranges under a
single presigned URL, enabling multi-range HTTP requests.

The client tries V2 first.  On 404 or 501 it falls back to V1 and caches the
result so subsequent calls skip the V2 attempt.  Setting
`HF_XET_CLIENT_RECONSTRUCTION_API_VERSION=1` or `=2` forces a specific version
with no fallback.

The `Client::get_reconstruction` trait method now always returns
`QueryReconstructionResponseV2`.  When the server returns V1, the client
converts it internally.

---

## 2. New Config Options

### `HF_XET_CLIENT_RECONSTRUCTION_API_VERSION`

Forces a specific reconstruction API version (1 or 2).  When unset, the client
auto-detects by trying V2 first.

### `HF_XET_CLIENT_ENABLE_MULTIRANGE_FETCHING`

Default: `false`.  When false, V2 multi-range fetch entries are split into
individual single-range requests executed in parallel.  When true, multi-range
requests are sent as-is (using `multipart/byteranges` responses).

---

## 3. Default Concurrency Changes

- `ac_initial_upload_concurrency`: 1 → 2
- `ac_initial_download_concurrency`: 1 → 4

These align the defaults with the documented values.

---

## 4. New Types in `xet_client::cas_types`

- `QueryReconstructionResponseV2` — V2 reconstruction response
- `XorbMultiRangeFetch` — A presigned URL with associated chunk/byte ranges
- `XorbRangeDescriptor` — A single chunk range + byte range pair

---

## 5. Multipart/Byteranges Parsing

`xet_client::cas_client::multipart::parse_multipart_byteranges` parses RFC 7233
`multipart/byteranges` HTTP responses.  Used when `enable_multirange_fetching`
is true and the presigned URL server returns multiple byte ranges in a single
response.

---

## 6. Downstream Impact

- `Client::get_reconstruction` return type changed to `QueryReconstructionResponseV2`
  (all trait implementations updated).
- `URLProvider::retrieve_url` now returns `Vec<HttpRange>` instead of a single
  `HttpRange` to support multi-range blocks.
- No wire format or serialization changes; V1 responses are converted client-side.
