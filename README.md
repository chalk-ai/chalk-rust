[![Crates.io](https://img.shields.io/crates/v/chalk-client.svg)](https://crates.io/crates/chalk-client)
[![Documentation](https://docs.rs/chalk-client/badge.svg)](https://docs.rs/chalk-client)
[![Test](https://github.com/chalk-ai/chalk-rust/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/chalk-ai/chalk-rust/actions/workflows/test.yml)

# Rust Chalk

The official [Chalk](https://chalk.ai) client library for Rust.

## Installation

Add `chalk-client` to your `Cargo.toml`:

```toml
[dependencies]
chalk-client = "0.1"
```

Or install via the command line:

```sh
cargo add chalk-client
```

## Connect to Chalk

Create a client using `ChalkClient::new()`. The returned client gets its
configuration from the first available source in this order:

1. **Explicit values** passed to the builder:
    ```rust,no_run
    use chalk_client::ChalkClient;

    # async fn example() -> chalk_client::error::Result<()> {
    let client = ChalkClient::new()
        .client_id("your-client-id")
        .client_secret("your-client-secret")
        .api_server("https://api.chalk.ai")
        .environment("production")
        .build()
        .await?;
    # Ok(())
    # }
    ```

2. **Environment variables**: `CHALK_CLIENT_ID`, `CHALK_CLIENT_SECRET`,
   `CHALK_API_SERVER`, `CHALK_ACTIVE_ENVIRONMENT`:
    ```rust,no_run
    use chalk_client::ChalkClient;

    # async fn example() -> chalk_client::error::Result<()> {
    let client = ChalkClient::new().build().await?;
    # Ok(())
    # }
    ```

3. **`~/.chalk.yml`** file, created by running `chalk login`.

## Online Query

Query features for a single entity using JSON request/response:

```rust,no_run
use chalk_client::ChalkClient;
use chalk_client::types::QueryOptions;
use std::collections::HashMap;

# async fn example() -> chalk_client::error::Result<()> {
let client = ChalkClient::new().build().await?;

let inputs = HashMap::from([
    ("user.id".to_string(), serde_json::json!(42)),
]);
let outputs = vec!["user.age".to_string(), "user.name".to_string()];

let response = client.query(inputs, outputs, QueryOptions::default()).await?;

for feature in &response.data {
    println!("{}: {:?}", feature.field, feature.value);
}
# Ok(())
# }
```

## Online Query Bulk

Query features for multiple entities using the Arrow IPC (feather) protocol:

```rust,no_run
use chalk_client::ChalkClient;
use chalk_client::types::QueryOptions;
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

# async fn example() -> chalk_client::error::Result<()> {
let client = ChalkClient::new().build().await?;

let schema = Arc::new(Schema::new(vec![
    Field::new("user.id", DataType::Int64, false),
]));
let batch = RecordBatch::try_new(
    schema,
    vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
)?;

let outputs = vec!["user.id".to_string(), "user.name".to_string()];
let result = client.query_bulk(&batch, outputs, QueryOptions::default()).await?;

println!("has_data: {}", result.has_data);
println!("scalar_data: {} bytes", result.scalar_data.len());
# Ok(())
# }
```

## Offline Query

Run offline (historical) queries that produce datasets:

```rust,no_run
use chalk_client::{ChalkClient, OfflineQueryParams};
use std::time::Duration;

# async fn example() -> chalk_client::error::Result<()> {
let client = ChalkClient::new().build().await?;

let params = OfflineQueryParams::new()
    .with_input("user.id", vec![serde_json::json!(1), serde_json::json!(2)])
    .with_output("user.email")
    .with_output("user.ltv");

let response = client.offline_query(params).await?;

// Wait for the job to complete
client.wait_for_offline_query(&response, Some(Duration::from_secs(300))).await?;

// Get download URLs for result Parquet files
let urls = client
    .get_offline_query_download_urls(&response, Some(Duration::from_secs(60)))
    .await?;

for url in &urls {
    println!("Download: {}", url);
}
# Ok(())
# }
```

## Upload Features

Push pre-computed feature values into the Chalk feature store:

```rust,no_run
use chalk_client::ChalkClient;
use std::collections::HashMap;

# async fn example() -> chalk_client::error::Result<()> {
let client = ChalkClient::new().build().await?;

let inputs = HashMap::from([
    ("user.id".to_string(), vec![serde_json::json!(1), serde_json::json!(2)]),
    ("user.name".to_string(), vec![serde_json::json!("Alice"), serde_json::json!("Bob")]),
]);

let result = client.upload_features_map(inputs).await?;
println!("operation_id: {:?}", result.operation_id);
# Ok(())
# }
```

## gRPC Client

For latency-sensitive, high-throughput workloads, use the gRPC client instead
of the REST/JSON client. gRPC uses Protocol Buffers over HTTP/2, which means
smaller payloads, no JSON parsing, and multiplexed requests over a single
connection.

The gRPC client supports `query`, `query_bulk`, and `upload_features`. Offline
queries are only available via the REST client.

```rust,no_run
use chalk_client::ChalkGrpcClient;
use chalk_client::gen::chalk::common::v1::{OnlineQueryRequest, OutputExpr};
use std::collections::HashMap;

# async fn example() -> chalk_client::error::Result<()> {
let client = ChalkGrpcClient::new()
    .build()
    .await?;

let request = OnlineQueryRequest {
    inputs: HashMap::from([(
        "user.id".to_string(),
        prost_types::Value {
            kind: Some(prost_types::value::Kind::NumberValue(42.0)),
        },
    )]),
    outputs: vec![OutputExpr {
        expr: Some(chalk_client::gen::chalk::common::v1::output_expr::Expr::FeatureFqn(
            "user.name".to_string(),
        )),
    }],
    ..Default::default()
};

let response = client.query(request).await?;
if let Some(data) = &response.data {
    for result in &data.results {
        println!("{}: {:?}", result.field, result.value);
    }
}
# Ok(())
# }
```

## Error Handling

All methods return `chalk_client::error::Result<T>`, which uses `ChalkClientError`:

```rust,no_run
use chalk_client::error::ChalkClientError;

# fn example(err: ChalkClientError) {
match err {
    ChalkClientError::Config(msg) => eprintln!("Configuration error: {}", msg),
    ChalkClientError::Auth(msg) => eprintln!("Authentication error: {}", msg),
    ChalkClientError::Http(e) => eprintln!("HTTP error: {}", e),
    ChalkClientError::Api { status, message } => {
        eprintln!("API error ({}): {}", status, message)
    }
    ChalkClientError::ServerErrors(errors) => {
        for err in &errors {
            eprintln!("Server error: {} - {}", err.code, err.message);
        }
    }
    _ => eprintln!("Other error: {}", err),
}
# }
```

## Configuration Reference

| Source | Client ID | Client Secret | API Server | Environment |
|--------|-----------|---------------|------------|-------------|
| Builder | `.client_id()` | `.client_secret()` | `.api_server()` | `.environment()` |
| Env var | `CHALK_CLIENT_ID` | `CHALK_CLIENT_SECRET` | `CHALK_API_SERVER` | `CHALK_ACTIVE_ENVIRONMENT` |
| YAML | `clientId` | `clientSecret` | `apiServer` | `activeEnvironment` |
| Default | — | — | `https://api.chalk.ai` | from token |

## License

Apache 2.0 - See [LICENSE](LICENSE) for more information.
