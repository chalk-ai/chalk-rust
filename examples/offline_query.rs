//! Offline query example — submit job, wait for completion, get download URLs.
//!
//! Run with:
//! ```sh
//! cargo run --example offline_query
//! ```

use chalk_rs::ChalkClient;
use chalk_rs::offline::OfflineQueryParams;
use chalk_rs::types::ResourceRequests;
use std::time::Duration;

#[tokio::main]
async fn main() -> chalk_rs::error::Result<()> {
    let client = ChalkClient::new().build().await?;

    println!("Environment: {}", client.environment_id());

    // =====================================================================
    // Example 1: Simple inline offline query
    // =====================================================================
    println!("\n=== Example 1: Simple Offline Query ===");

    let params = OfflineQueryParams::new()
        .with_input("user.id", vec![
            serde_json::json!(1),
            serde_json::json!(2),
            serde_json::json!(3),
        ])
        .with_output("user.id")
        .with_output("user.full_name")
        .with_output("user.gender");

    let response = client.offline_query(params).await?;

    println!("Revisions: {}", response.revisions.len());
    if let Some(rev) = response.revisions.last() {
        println!("  revision_id: {:?}", rev.revision_id);
        println!("  status: {:?}", rev.status);
    }
    if !response.errors.is_empty() {
        eprintln!("Errors: {:?}", response.errors);
    }

    // Wait for the job to complete and get download URLs
    println!("\nWaiting for job to complete...");
    client
        .wait_for_offline_query(&response, Some(Duration::from_secs(300)))
        .await?;
    println!("Job completed!");

    let urls = client
        .get_offline_query_download_urls(&response, Some(Duration::from_secs(60)))
        .await?;
    println!("Download URLs ({}):", urls.len());
    for (i, url) in urls.iter().enumerate() {
        println!("  [{}] {}", i, url);
    }

    // =====================================================================
    // Example 2: Async sharded offline query with resources
    // =====================================================================
    println!("\n=== Example 2: Async Sharded Offline Query ===");

    let params2 = OfflineQueryParams::new()
        .with_output("user.id")
        .with_output("user.full_name")
        .with_output("user.gender")
        .with_run_asynchronously(true)
        .with_num_shards(4)
        .with_num_workers(4)
        .with_resources(ResourceRequests {
            memory: Some("8G".to_string()),
            ..Default::default()
        })
        .with_dataset_name("rust_example_dataset");

    let response2 = client.offline_query(params2).await?;

    println!("Revisions: {}", response2.revisions.len());
    if let Some(rev) = response2.revisions.last() {
        println!("  revision_id: {:?}", rev.revision_id);
    }

    if let Some(rev) = response2.revisions.last() {
        if let Some(ref rev_id) = rev.revision_id {
            let status = client.get_offline_query_status(rev_id).await?;
            println!("  status: {:?}", status.report.status);
        }
    }

    println!("\nWaiting for async job...");
    client
        .wait_for_offline_query(&response2, Some(Duration::from_secs(600)))
        .await?;
    println!("Async job completed!");

    // =====================================================================
    // Example 3: SQL spine query
    // =====================================================================
    println!("\n=== Example 3: SQL Spine Query ===");

    let params3 = OfflineQueryParams::from_sql("SELECT id AS \"user.id\" FROM users LIMIT 10")
        .with_output("user.id")
        .with_output("user.full_name");

    let response3 = client.offline_query(params3).await?;
    println!("Revisions: {}", response3.revisions.len());

    println!("\nOffline query example completed!");
    Ok(())
}
