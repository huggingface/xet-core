# xet-client

[![crates.io](https://img.shields.io/crates/v/xet-client.svg)](https://crates.io/crates/xet-client)
[![docs.rs](https://docs.rs/xet-client/badge.svg)](https://docs.rs/xet-client)
[![License](https://img.shields.io/crates/l/xet-client.svg)](https://github.com/huggingface/xet-core/blob/main/LICENSE)

Client for communicating with Hugging Face Xet storage servers.

## Overview

Upload and download data and metadata objects from the backend Hugging Face Xet storage servers.  Features automatic concurrency adaptations, connection pooling, and retry resiliency.  Intended to be used through the API in the hf-xet package.

This crate is part of [xet-core](https://github.com/huggingface/xet-core).

## Xorb uploads

`RemoteClient::upload_xorb` uploads each xorb through an upload grant:

1. **Grant**: `POST /v1/xorb-grants/{prefix}/{hash}` with the serialized length and CRC-64/NVME checksum. A 201 returns a grant (method, URL, and headers); a 200 means the xorb already exists and nothing is uploaded.
2. **Upload**: send the serialized xorb to the grant URL with exactly the grant's headers and no CAS credentials. A 412 means an earlier attempt already uploaded it. A 403 (e.g. an expired grant) starts a new grant.
3. **Commit**: `POST /v1/xorb-commits/{prefix}/{hash}/grant/{grant_id}`. A 404 means no data was uploaded for the grant, so the client requests a new grant and uploads again, up to three grants in total, counting grants replaced after a 403 upload.

If any grant request for a xorb returns 404, the client uploads that xorb through `POST /v1/xorbs/{prefix}/{hash}`; every xorb requests a grant first. Any other error after retries fails the upload. Set `HF_XET_CLIENT_LEGACY_DIRECT_XORB_UPLOAD=1` to always use `/v1/xorbs`.

The upload permit is acquired before the first grant request and is held through every grant, upload, and commit until the xorb is stored.

## License

Apache-2.0
