//! Upload features example — push feature values into the Chalk online/offline store.
//!
//! Run with:
//! ```sh
//! cargo run --example upload_features
//! ```

use chalk_client::ChalkClient;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::main]
async fn main() -> chalk_client::error::Result<()> {
    let client = ChalkClient::new().build().await?;

    println!("Environment: {}", client.environment_id());
    println!("Query server: {}", client.query_server());

    // =====================================================================
    // Method 1: Upload via Arrow RecordBatch (recommended for typed data)
    // =====================================================================
    println!("\n=== Upload Features (Arrow RecordBatch) ===");

    let schema = Arc::new(Schema::new(vec![
        Field::new("user.id", DataType::Int64, false),
        Field::new("user.name", DataType::Utf8, true),
        Field::new("user.age", DataType::Int64, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1001, 1002, 1003])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Int64Array::from(vec![30, 25, 35])),
        ],
    )
    .expect("failed to create RecordBatch");

    println!(
        "Uploading {} rows x {} columns...",
        batch.num_rows(),
        batch.num_columns()
    );

    let result = client.upload_features(&batch).await?;

    println!("Upload result:");
    println!("  operation_id: {:?}", result.operation_id);
    if !result.errors.is_empty() {
        eprintln!("  Errors:");
        for err in &result.errors {
            eprintln!("    {:?}", err);
        }
    } else {
        println!("  No errors — upload successful!");
    }

    // =====================================================================
    // Method 2: Upload via HashMap (convenience for simple cases)
    // =====================================================================
    println!("\n=== Upload Features (HashMap) ===");

    let inputs = HashMap::from([
        (
            "user.id".to_string(),
            vec![serde_json::json!(2001), serde_json::json!(2002)],
        ),
        (
            "user.name".to_string(),
            vec![serde_json::json!("Diana"), serde_json::json!("Eve")],
        ),
    ]);

    println!("Uploading {} features for {} entities...", inputs.len(), 2);

    let result2 = client.upload_features_map(inputs).await?;

    println!("Upload result:");
    println!("  operation_id: {:?}", result2.operation_id);
    if !result2.errors.is_empty() {
        eprintln!("  Errors:");
        for err in &result2.errors {
            eprintln!("    {:?}", err);
        }
    } else {
        println!("  No errors — upload successful!");
    }

    println!("\nUpload features example completed!");
    Ok(())
}
