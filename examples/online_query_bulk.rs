//! Bulk online query example — feather (Arrow IPC) protocol.
//!
//! Run with:
//! ```sh
//! cargo run --example online_query_bulk
//! ```

use chalk_rs::ChalkClient;
use chalk_rs::types::QueryOptions;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use std::io::Cursor;
use std::sync::Arc;

#[tokio::main]
async fn main() -> chalk_rs::error::Result<()> {
    let client = ChalkClient::new().build().await?;

    println!("Environment: {}", client.environment_id());
    println!("Query server: {}", client.query_server());

    // --- Bulk query for multiple users ---
    println!("\n=== Bulk Online Query ===");

    let schema = Arc::new(Schema::new(vec![
        Field::new("user.id", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .expect("failed to create RecordBatch");

    let outputs = vec![
        "user.id".to_string(),
        "user.full_name".to_string(),
        "user.gender".to_string(),
        "user.socure_score".to_string(),
    ];

    let options = QueryOptions {
        include_meta: Some(true),
        query_name: Some("rust_bulk_example".to_string()),
        ..Default::default()
    };

    let result = client.query_bulk(&batch, outputs, options).await?;

    println!("has_data: {}", result.has_data);
    println!("scalar_data: {} bytes", result.scalar_data.len());

    if !result.errors.is_empty() {
        eprintln!("Errors:");
        for err in &result.errors {
            eprintln!("  {}", err);
        }
    }

    // Parse the scalar_data as an Arrow IPC file and print results
    if !result.scalar_data.is_empty() {
        let cursor = Cursor::new(&result.scalar_data);
        match FileReader::try_new(cursor, None) {
            Ok(reader) => {
                let schema = reader.schema();
                println!("\nOutput columns:");
                for field in schema.fields() {
                    println!("  {} ({})", field.name(), field.data_type());
                }

                for batch_result in reader {
                    let batch = batch_result.expect("failed to read batch");
                    println!("\n{} rows returned:", batch.num_rows());

                    let schema = batch.schema();
                    for row in 0..batch.num_rows() {
                        println!("  Row {}:", row);
                        for col in 0..batch.num_columns() {
                            let col_name = schema.field(col).name();
                            let array = batch.column(col);

                            if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
                                println!("    {}: {}", col_name, arr.value(row));
                            } else if let Some(arr) = array.as_any().downcast_ref::<StringArray>()
                            {
                                println!("    {}: {}", col_name, arr.value(row));
                            } else {
                                println!("    {}: {:?}", col_name, array);
                            }
                        }
                    }
                }
            }
            Err(e) => eprintln!("Could not parse scalar_data as Arrow IPC: {}", e),
        }
    }

    if let Some(ref meta) = result.meta {
        println!("\nQuery metadata: {}", meta);
    }

    println!("\nBulk query example completed!");
    Ok(())
}
