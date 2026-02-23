# CLAUDE.md

## Build & Test

- `cargo build` — compile the library
- `cargo test` — run all unit tests
- `cargo clippy -- -D warnings` — lint (must pass with no warnings)
- `cargo doc --no-deps` — generate docs
- `cargo build --examples` — compile all examples

## Project Structure

- `src/` — library source code (HTTP client, gRPC client, auth, config, types, offline query builder)
- `src/gen/` — pre-generated protobuf Rust code (do not edit by hand; regenerate with `./generate.sh`)
- `protos/` — vendored `.proto` definitions
- `tools/gen-protos/` — code generation tool
- `examples/` — standalone example binaries (require env vars to run)

## Examples

Examples require these environment variables:
- `CHALK_CLIENT_ID`
- `CHALK_CLIENT_SECRET`
- `CHALK_API_SERVER`
- `CHALK_ACTIVE_ENVIRONMENT`

Run an example: `cargo run --example online_query`

## Notes

- The crate is published as `chalk-client` on crates.io
- Proto files are vendored in `protos/` and generated code is committed in `src/gen/`
- To regenerate proto code: `./generate.sh` (requires `protoc` on PATH)
