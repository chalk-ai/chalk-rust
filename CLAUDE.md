# CLAUDE.md

## Build & Test

- `cargo build` — compile the library
- `cargo test` — run all unit tests
- `cargo clippy -- -D warnings` — lint (must pass with no warnings)
- `cargo doc --no-deps` — generate docs
- `cargo build --examples` — compile all examples

## Project Structure

- `src/` — library source code (HTTP client, auth, config, types, offline query builder)
- `examples/` — standalone example binaries (require env vars to run)

## Examples

Examples require these environment variables:
- `CHALK_CLIENT_ID`
- `CHALK_CLIENT_SECRET`
- `CHALK_API_SERVER`
- `CHALK_ACTIVE_ENVIRONMENT`

Run an example: `cargo run --example online_query`

## Notes

- gRPC support is excluded for now (will be added later as a cargo feature)
- The crate is published as `chalk-rs` on crates.io
