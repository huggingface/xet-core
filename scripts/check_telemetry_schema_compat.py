#!/usr/bin/env python3
"""Rejects backward-incompatible changes to the telemetry metrics schema.

Consumers of this schema assign each property a field type on first sight and cannot change it in
place afterwards. That makes the compatibility rules asymmetric:

  * Adding a property is fine - a consumer that has not seen it yet simply ignores it.
  * Removing one silently breaks dashboards and alerts built on it.
  * Changing one's type breaks ingestion for every document carrying the new type, and recovering
    means rebuilding the stored data.

So this compares the schema on the current branch against a baseline (normally `main`) and fails
on removals and type changes while allowing additions.

Usage:
    check_telemetry_schema_compat.py --baseline <file> [--current <file>]
    check_telemetry_schema_compat.py --baseline-ref origin/main

Exits 0 when compatible and non-zero otherwise, including on a malformed schema. A missing
baseline is treated as compatible: that is the commit that introduces the schema.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

SCHEMA_PATH = "telemetry/metrics.schema.json"

# The two direction-specific definitions inside the combined document.
DEFS_KEY = "$defs"


def load_json(text: str, source: str) -> dict:
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        sys.exit(f"error: {source} is not valid JSON: {e}")


def read_ref(ref: str, path: str) -> str | None:
    """Reads a file at a git ref, or None when it does not exist there."""
    result = subprocess.run(
        ["git", "show", f"{ref}:{path}"],
        capture_output=True,
        text=True,
    )
    return result.stdout if result.returncode == 0 else None


def properties_by_definition(schema: dict, source: str) -> dict[str, dict[str, str]]:
    """Maps definition name -> {property name -> JSON Schema type}.

    Only the `type` is compared. Descriptions, formats, and ordering are free to change: none of
    them affect how a consumer types its storage.
    """
    defs = schema.get(DEFS_KEY)
    if not isinstance(defs, dict):
        sys.exit(f"error: {source} has no {DEFS_KEY!r} object; is it the combined metrics schema?")

    out: dict[str, dict[str, str]] = {}
    for name, definition in defs.items():
        props = definition.get("properties")
        if not isinstance(props, dict):
            sys.exit(f"error: {source} definition {name!r} has no properties")
        out[name] = {
            prop: spec.get("type", "<untyped>") for prop, spec in props.items()
        }
    return out


def compare(baseline: dict, current: dict) -> tuple[list[str], list[str]]:
    """Returns (breaking changes, additions)."""
    breaking: list[str] = []
    additions: list[str] = []

    for definition, base_props in baseline.items():
        cur_props = current.get(definition)
        if cur_props is None:
            breaking.append(
                f"{definition}: definition removed - anything querying this shape breaks"
            )
            continue

        for prop, base_type in base_props.items():
            if prop not in cur_props:
                breaking.append(
                    f"{definition}.{prop}: removed (was {base_type}) - "
                    f"dashboards and alerts using it will silently go empty"
                )
            elif cur_props[prop] != base_type:
                breaking.append(
                    f"{definition}.{prop}: type changed {base_type} -> {cur_props[prop]} - "
                    f"consumers cannot retype a field in place, so this needs a new "
                    f"property name"
                )

        for prop, cur_type in cur_props.items():
            if prop not in base_props:
                additions.append(f"{definition}.{prop}: added ({cur_type})")

    for definition in current:
        if definition not in baseline:
            additions.append(f"{definition}: new definition")

    return breaking, additions


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--baseline", type=Path, help="baseline schema file")
    parser.add_argument("--baseline-ref", help="git ref to read the baseline schema from, e.g. origin/main")
    parser.add_argument("--current", type=Path, default=Path(SCHEMA_PATH), help=f"current schema (default: {SCHEMA_PATH})")
    args = parser.parse_args()

    if bool(args.baseline) == bool(args.baseline_ref):
        parser.error("pass exactly one of --baseline or --baseline-ref")

    if args.baseline_ref:
        baseline_text = read_ref(args.baseline_ref, SCHEMA_PATH)
        baseline_source = f"{args.baseline_ref}:{SCHEMA_PATH}"
        if baseline_text is None:
            print(
                f"note: {baseline_source} does not exist - treating as the commit that introduces "
                f"the schema, so there is nothing to compare against."
            )
            return 0
    else:
        if not args.baseline.exists():
            print(f"note: {args.baseline} does not exist - nothing to compare against.")
            return 0
        baseline_text = args.baseline.read_text()
        baseline_source = str(args.baseline)

    if not args.current.exists():
        sys.exit(
            f"error: {args.current} is missing. Generate it with:\n"
            f"    UPDATE_TELEMETRY_SCHEMA=1 cargo test -p xet-data --lib telemetry::schema"
        )

    baseline = properties_by_definition(load_json(baseline_text, baseline_source), baseline_source)
    current = properties_by_definition(load_json(args.current.read_text(), str(args.current)), str(args.current))

    breaking, additions = compare(baseline, current)

    for addition in sorted(additions):
        print(f"  + {addition}")

    if not breaking:
        summary = f"{len(additions)} addition(s)" if additions else "no changes"
        print(f"telemetry schema is backward compatible ({summary}).")
        return 0

    print("\nBREAKING telemetry schema changes:", file=sys.stderr)
    for change in sorted(breaking):
        print(f"  - {change}", file=sys.stderr)
    print(
        "\nConsumers of this schema type each property on first sight and cannot change it in "
        "place.\nIf a metric's meaning or unit changed, add a new property rather than "
        "repurposing the old one\n(for example duration_us alongside duration_ms).",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
