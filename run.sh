#!/usr/bin/env bash
set -euo pipefail

demo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$demo_dir"

# Public credentials for this demo's isolated, read-only fixture account.
export DEMO_DATABASE_URL="${DEMO_DATABASE_URL:-postgres://connector_reader:demo_reader_password@127.0.0.1:55432/connector_demo}"

if [[ "${1:-}" == "--prebuilt" ]]; then
    shift
    demo_target_dir="${CARGO_TARGET_DIR:-$demo_dir/target}"
    demo_binary="${DEMO_BINARY:-$demo_target_dir/debug/postgres-connector-demo}"
    if [[ ! -x "$demo_binary" ]]; then
        echo 'Demo binary is missing. Build it first, or set DEMO_BINARY.' >&2
        exit 1
    fi
    exec "$demo_binary" "$@"
fi

exec cargo run --locked -- "$@"
