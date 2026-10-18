#!/usr/bin/env bash
# Builds the rustdoc the site's API links resolve against, into target/doc: this crate with every
# feature, as docs.rs renders it, and the core release the lock file pins with every feature, as
# docs.rs renders the core. A page may link items of either crate.
set -euo pipefail
cd "$(dirname "$0")/.."
features="$(cargo metadata --locked --format-version 1 | python3 -c '
import json, sys
packages = {p["name"]: p for p in json.load(sys.stdin)["packages"]}
lapin = packages["ruststream-lapin"]
docs_rs = lapin["metadata"]["docs"]["rs"]
crate = (
    [name for name in lapin["features"] if name != "default"]
    if docs_rs.get("all-features")
    else docs_rs.get("features", [])
)
core = [f"ruststream/{name}" for name in packages["ruststream"]["features"] if name != "default"]
print(",".join(crate + core))
')"
cargo doc --locked --no-deps -p ruststream-lapin -p ruststream --features "$features"
