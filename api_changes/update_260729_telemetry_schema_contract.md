# Telemetry metric schema is now a generated, published contract

**Date**: 2026-07-29
**Crate**: `xet-data` (`xet_data::telemetry`)
**Artifact**: `telemetry/metrics.schema.json`

Follows [`update_260728_client_transfer_telemetry.md`](./update_260728_client_transfer_telemetry.md),
which introduced the telemetry payload itself.

## What changed

The telemetry metric vocabulary is now exported as a generated JSON Schema at
`telemetry/metrics.schema.json`, committed alongside the code that produces it.

**That file is the source of truth for what the client collects.** Every property carries a
description of what it measures and its unit, so a consumer needs nothing from this repo's source
to interpret a document. `xet_data/src/telemetry/payload.rs` remains the source of truth for the
*code*; the schema is generated from it and must never be hand-edited.

**The path is a published contract and must not move or be renamed.** Consumers pin it by tag:

```
https://raw.githubusercontent.com/huggingface/xet-core/<tag>/telemetry/metrics.schema.json
```

## Scope: what this repo does and does not publish

This publishes **what the client emits** — property names, JSON types, and meanings.

It deliberately does **not** describe how any consumer stores, indexes, or aggregates those
documents. That is the consumer's concern, this repo is public, and it has no way to keep a
description of someone else's storage layer correct. A consumer that needs to type its own storage
derives that from each property's `type`:

| JSON Schema | Meaning |
|---|---|
| `"integer"` | fits an unsigned 64-bit integer |
| `"number"` | finite double; never NaN or infinity |
| `"boolean"` | — |
| `"string"` | short, low-cardinality except `transfer_id` |

Every value is a scalar: never null, never nested, never an array.

## Regenerating

```bash
UPDATE_TELEMETRY_SCHEMA=1 cargo test -p xet-data --lib telemetry::schema
```

Without that variable, the same test asserts the committed file is current — so changing a payload
struct without committing the regenerated schema fails `cargo test`.

`schemars` was added as a **dev-dependency**, and the `JsonSchema` derives are `cfg(test)`.
`cargo tree -p xet-data -e normal` shows zero occurrences of it or its transitive dependencies, so
nothing new ships in a release build.

## Every metric must be documented

A test fails the build if any property reaches the schema without a `description`. Add a `///` doc
comment to the field in `payload.rs` and regenerate. A metric documented only by its name is a
metric nobody outside this repo can interpret.

## Compatibility rules, now enforced in CI

`scripts/check_telemetry_schema_compat.py` diffs the branch's schema against the merge target and
fails the `telemetry-schema-compat` job on a breaking change:

| Change | Verdict | Why |
|---|---|---|
| Property added | allowed | a consumer that has not seen it yet simply ignores it |
| Property removed | **rejected** | dashboards and alerts built on it silently go empty |
| Property type changed | **rejected** | consumers type each field on first sight and cannot change it in place; recovering means rebuilding stored data |

If a metric's meaning or unit changes, **add a new property** rather than repurposing the old one —
`duration_us` alongside `duration_ms`, not `duration_ms` becoming a float.

Descriptions, formats, and property ordering are free to change; only `type` is compared.

## Note for the receiving service

The numeric properties must be stored as numbers, not strings, or the throughput and duration
metrics cannot be averaged or range-queried — which is the entire point of collecting them. Storing
the `metrics` object as an opaque or string-typed blob defeats the regression alerting this exists
to enable.

Whatever mapping the receiving service derives should also tolerate a property it has not seen:
consumers pin a tag, so a client released ahead of a consumer rebuild will emit properties the
pinned schema does not contain. Those should be retained and ignored, not rejected.
