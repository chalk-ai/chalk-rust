//! End-to-end integration test against a live Chalk environment.
//!
//! Automatically runs `chalkadmin customer login` to refresh credentials
//! before executing tests. Requires `chalkadmin` on PATH.
//!
//! ```sh
//! cargo run --example integration_test
//! ```

use chalk_rs::gen::chalk::common::v1::{
    OnlineQueryBulkRequest, OnlineQueryRequest, OutputExpr, UploadFeaturesBulkRequest,
};
use chalk_rs::types::QueryOptions;
use chalk_rs::{ChalkClient, ChalkGrpcClient, OfflineQueryParams};
use std::collections::HashMap;
use std::process::Command;
use std::time::Duration;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use std::io::Cursor;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Refresh credentials before running tests.
    println!("Refreshing credentials via chalkadmin...");
    let status = Command::new("chalkadmin")
        .args(["customer", "login", "--name", "sandbox_support"])
        .status()?;
    if !status.success() {
        return Err(format!("chalkadmin exited with {}", status).into());
    }
    println!();

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

    // Test 6: gRPC online query (retry — sandbox gRPC engine may be cold)
    println!("\n=== Test 6: gRPC Online Query (user.id=1) ===");
    let grpc_request = OnlineQueryRequest {
        inputs: HashMap::from([(
            "user.id".to_string(),
            prost_types::Value {
                kind: Some(prost_types::value::Kind::NumberValue(1.0)),
            },
        )]),
        outputs: vec![
            output_expr("user.id"),
            output_expr("user.name"),
            output_expr("user.age"),
        ],
        ..Default::default()
    };
    let grpc_response = retry_grpc(&grpc_client, grpc_request).await?;
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
        .query_proto(OnlineQueryRequest {
            inputs: HashMap::from([(
                "user.id".to_string(),
                prost_types::Value {
                    kind: Some(prost_types::value::Kind::NumberValue(2.0)),
                },
            )]),
            outputs: vec![output_expr("user.name"), output_expr("user.age")],
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

    // Test 8: gRPC bulk query
    println!("\n=== Test 8: gRPC Bulk Query (user.id=[1,2,3]) ===");
    let grpc_bulk_schema = Arc::new(Schema::new(vec![Field::new(
        "user.id",
        DataType::Int64,
        false,
    )]));
    let grpc_bulk_batch = RecordBatch::try_new(
        grpc_bulk_schema,
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )?;
    let grpc_bulk_feather = serialize_to_feather(&grpc_bulk_batch)?;
    let grpc_bulk_response = grpc_client
        .query_bulk_proto(OnlineQueryBulkRequest {
            inputs: Some(
                chalk_rs::gen::chalk::common::v1::online_query_bulk_request::Inputs::InputsFeather(
                    grpc_bulk_feather,
                ),
            ),
            outputs: vec![
                output_expr("user.id"),
                output_expr("user.name"),
                output_expr("user.age"),
            ],
            ..Default::default()
        })
        .await?;
    if !grpc_bulk_response.scalars_data.is_empty() {
        let cursor = Cursor::new(&grpc_bulk_response.scalars_data);
        let reader = FileReader::try_new(cursor, None)?;
        for batch_result in reader {
            let batch = batch_result?;
            println!("  {} rows x {} cols", batch.num_rows(), batch.num_columns());
        }
    }
    for err in &grpc_bulk_response.errors {
        eprintln!("  error: {}", err.message);
    }

    // Test 9: gRPC upload features
    println!("\n=== Test 9: gRPC Upload Features ===");
    let upload_schema = Arc::new(Schema::new(vec![
        Field::new("user.id", DataType::Int64, false),
        Field::new("user.name", DataType::Utf8, true),
        Field::new("user.age", DataType::Int64, true),
    ]));
    let upload_batch = RecordBatch::try_new(
        upload_schema,
        vec![
            Arc::new(Int64Array::from(vec![98])),
            Arc::new(StringArray::from(vec!["GrpcTestUser"])),
            Arc::new(Int64Array::from(vec![33])),
        ],
    )?;
    let upload_feather = serialize_to_feather(&upload_batch)?;
    let grpc_upload_response = grpc_client
        .upload_features_proto(UploadFeaturesBulkRequest {
            inputs_feather: upload_feather,
            ..Default::default()
        })
        .await?;
    println!("  errors: {:?}", grpc_upload_response.errors);

    println!("\nAll tests passed!");
    Ok(())
}

fn output_expr(fqn: &str) -> OutputExpr {
    OutputExpr {
        expr: Some(
            chalk_rs::gen::chalk::common::v1::output_expr::Expr::FeatureFqn(fqn.to_string()),
        ),
    }
}

fn serialize_to_feather(batch: &RecordBatch) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut buf = Vec::new();
    let mut writer = FileWriter::try_new(&mut buf, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(buf)
}

/// Retry a gRPC query up to 3 times with backoff. Sandbox environments
/// may have a cold gRPC engine that returns Unavailable on first connect.
async fn retry_grpc(
    client: &ChalkGrpcClient,
    request: OnlineQueryRequest,
) -> Result<chalk_rs::gen::chalk::common::v1::OnlineQueryResponse, Box<dyn std::error::Error>> {
    let mut last_err = None;
    for attempt in 0..3 {
        match client.query_proto(request.clone()).await {
            Ok(resp) => return Ok(resp),
            Err(e) => {
                println!("  attempt {} failed: {}, retrying...", attempt + 1, e);
                last_err = Some(e);
                tokio::time::sleep(Duration::from_secs(5 * (attempt + 1))).await;
            }
        }
    }
    Err(last_err.unwrap().into())
}
