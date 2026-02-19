//! End-to-end integration test against a live Chalk environment.
//!
//! Prerequisites: run `chalk login` or `chalkadmin customer login` to
//! populate ~/.chalk.yml with credentials for the target environment.
//!
//! ```sh
//! cargo run --example integration_test
//! ```

use chalk_rs::gen::chalk::common::v1::{OnlineQueryRequest, OutputExpr};
use chalk_rs::types::QueryOptions;
use chalk_rs::{ChalkClient, ChalkGrpcClient, OfflineQueryParams};
use std::collections::HashMap;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use std::io::Cursor;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ---- HTTP Client ----

    let client = ChalkClient::new().build().await?;
    println!("HTTP client connected");
    println!("  environment: {}", client.environment_id());
    println!("  query_server: {}", client.query_server());

    // Test 1: Online query
    println!("\n=== Test 1: Online Query (user.id=1) ===");
    let response = client
        .query(
            HashMap::from([("user.id".to_string(), serde_json::json!(1))]),
            vec![
                "user.id".to_string(),
                "user.name".to_string(),
                "user.email".to_string(),
                "user.age".to_string(),
            ],
            QueryOptions::default(),
        )
        .await?;
    for feature in &response.data {
        println!("  {}: {:?}", feature.field, feature.value);
    }
    for err in &response.errors {
        eprintln!("  error: code={:?} message={:?}", err.code, err.message);
    }

    // Test 2: Online query (different user)
    println!("\n=== Test 2: Online Query (user.id=2) ===");
    let response2 = client
        .query(
            HashMap::from([("user.id".to_string(), serde_json::json!(2))]),
            vec![
                "user.id".to_string(),
                "user.name".to_string(),
                "user.age".to_string(),
            ],
            QueryOptions::default(),
        )
        .await?;
    for feature in &response2.data {
        println!("  {}: {:?}", feature.field, feature.value);
    }

    // Test 3: Bulk query
    println!("\n=== Test 3: Bulk Query (user.id=[1,2,3]) ===");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "user.id",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )?;
    let bulk_result = client
        .query_bulk(
            &batch,
            vec![
                "user.id".to_string(),
                "user.name".to_string(),
                "user.age".to_string(),
            ],
            QueryOptions::default(),
        )
        .await?;
    println!("  has_data: {}", bulk_result.has_data);
    if !bulk_result.scalar_data.is_empty() {
        let cursor = Cursor::new(&bulk_result.scalar_data);
        let reader = FileReader::try_new(cursor, None)?;
        for batch_result in reader {
            let batch = batch_result?;
            println!("  {} rows x {} cols", batch.num_rows(), batch.num_columns());
        }
    }
    for err in &bulk_result.errors {
        eprintln!("  error: {}", err);
    }

    // Test 4: Upload features
    println!("\n=== Test 4: Upload Features ===");
    let upload_result = client
        .upload_features_map(HashMap::from([
            ("user.id".to_string(), vec![serde_json::json!(99)]),
            (
                "user.name".to_string(),
                vec![serde_json::json!("RustTestUser")],
            ),
            ("user.age".to_string(), vec![serde_json::json!(42)]),
        ]))
        .await?;
    println!("  operation_id: {:?}", upload_result.operation_id);
    println!("  errors: {:?}", upload_result.errors);

    // Test 5: Offline query
    println!("\n=== Test 5: Offline Query (user.id=[1,2,3]) ===");
    let offline_response = client
        .offline_query(
            OfflineQueryParams::new()
                .with_input(
                    "user.id",
                    vec![
                        serde_json::json!(1),
                        serde_json::json!(2),
                        serde_json::json!(3),
                    ],
                )
                .with_output("user.id")
                .with_output("user.name")
                .with_output("user.age"),
        )
        .await?;
    println!("  dataset_id: {:?}", offline_response.dataset_id);
    for rev in &offline_response.revisions {
        println!(
            "  revision: id={:?} status={:?}",
            rev.revision_id, rev.status
        );
    }
    println!("  Waiting for completion...");
    client
        .wait_for_offline_query(&offline_response, Some(Duration::from_secs(120)))
        .await?;
    println!("  Completed!");
    let urls = client
        .get_offline_query_download_urls(&offline_response, Some(Duration::from_secs(60)))
        .await?;
    println!("  Download URLs: {}", urls.len());

    // ---- gRPC Client ----

    let grpc_client = ChalkGrpcClient::new().build().await?;
    println!("\ngRPC client connected");
    println!("  environment: {}", grpc_client.environment_id());

    // Test 6: gRPC online query
    println!("\n=== Test 6: gRPC Online Query (user.id=1) ===");
    let grpc_response = grpc_client
        .query(OnlineQueryRequest {
            inputs: HashMap::from([(
                "user.id".to_string(),
                prost_types::Value {
                    kind: Some(prost_types::value::Kind::NumberValue(1.0)),
                },
            )]),
            outputs: vec![
                OutputExpr {
                    expr: Some(
                        chalk_rs::gen::chalk::common::v1::output_expr::Expr::FeatureFqn(
                            "user.id".to_string(),
                        ),
                    ),
                },
                OutputExpr {
                    expr: Some(
                        chalk_rs::gen::chalk::common::v1::output_expr::Expr::FeatureFqn(
                            "user.name".to_string(),
                        ),
                    ),
                },
                OutputExpr {
                    expr: Some(
                        chalk_rs::gen::chalk::common::v1::output_expr::Expr::FeatureFqn(
                            "user.age".to_string(),
                        ),
                    ),
                },
            ],
            ..Default::default()
        })
        .await?;
    if let Some(data) = &grpc_response.data {
        for result in &data.results {
            println!("  {}: {:?}", result.field, result.value);
        }
    }
    for err in &grpc_response.errors {
        eprintln!("  error: {}", err.message);
    }

    // Test 7: gRPC online query (different user)
    println!("\n=== Test 7: gRPC Online Query (user.id=2) ===");
    let grpc_response2 = grpc_client
        .query(OnlineQueryRequest {
            inputs: HashMap::from([(
                "user.id".to_string(),
                prost_types::Value {
                    kind: Some(prost_types::value::Kind::NumberValue(2.0)),
                },
            )]),
            outputs: vec![
                OutputExpr {
                    expr: Some(
                        chalk_rs::gen::chalk::common::v1::output_expr::Expr::FeatureFqn(
                            "user.name".to_string(),
                        ),
                    ),
                },
                OutputExpr {
                    expr: Some(
                        chalk_rs::gen::chalk::common::v1::output_expr::Expr::FeatureFqn(
                            "user.age".to_string(),
                        ),
                    ),
                },
            ],
            ..Default::default()
        })
        .await?;
    if let Some(data) = &grpc_response2.data {
        for result in &data.results {
            println!("  {}: {:?}", result.field, result.value);
        }
    }
    for err in &grpc_response2.errors {
        eprintln!("  error: {}", err.message);
    }

    println!("\nAll tests passed!");
    Ok(())
}
