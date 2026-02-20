#!/usr/bin/env bash
# Regenerate protobuf Rust code from vendored .proto files.
#
# Prerequisites: protoc must be on $PATH.
#   brew install protobuf        # macOS
#   apt install protobuf-compiler # Debian/Ubuntu

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"

echo "==> Cleaning src/gen/"
rm -rf "$REPO_ROOT/src/gen"
mkdir -p "$REPO_ROOT/src/gen"

echo "==> Running gen-protos"
cargo run --manifest-path "$REPO_ROOT/tools/gen-protos/Cargo.toml"

echo "==> Done. Generated files:"
ls -1 "$REPO_ROOT/src/gen/"
