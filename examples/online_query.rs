//! Online query example — single-entity JSON query.
//!
//! Run with:
//! ```sh
//! cargo run --example online_query
//! ```

use chalk_rs::ChalkClient;
use chalk_rs::types::QueryOptions;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> chalk_rs::error::Result<()> {
    let client = ChalkClient::new().build().await?;

    println!("Environment: {}", client.environment_id());
    println!("Query server: {}", client.query_server());

    // --- Simple online query ---
    println!("\n=== Online Query ===");

    let inputs = HashMap::from([
        ("user.id".to_string(), serde_json::json!(1)),
    ]);
    let outputs = vec![
        "user.name".to_string(),
        "user.age".to_string(),
    ];
    let options = QueryOptions {
        include_meta: Some(true),
        ..Default::default()
    };

    let response = client.query(inputs, outputs, options).await?;

    if !response.errors.is_empty() {
        eprintln!("Query returned errors:");
        for err in &response.errors {
            eprintln!("  code={:?} message={:?}", err.code, err.message);
        }
    }

    for feature in &response.data {
        println!("  {}: {:?}", feature.field, feature.value);
        if let Some(ref meta) = feature.meta {
            println!("    resolver: {:?}", meta.chosen_resolver_fqn);
            println!("    cache_hit: {:?}", meta.cache_hit);
        }
    }

    if let Some(ref meta) = response.meta {
        println!("\nQuery metadata:");
        println!("  execution_duration: {:?}s", meta.execution_duration_s);
        println!("  query_id: {:?}", meta.query_id);
        println!("  deployment_id: {:?}", meta.deployment_id);
        println!("  environment_id: {:?}", meta.environment_id);
    }

    println!("\nOnline query example completed!");
    Ok(())
}
