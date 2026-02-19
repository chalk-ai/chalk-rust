//! Offline query example — submit job, wait for completion, get download URLs.
//!
//! Run with:
//! ```sh
//! cargo run --example offline_query
//! ```

use chalk_rs::ChalkClient;
use chalk_rs::offline::OfflineQueryParams;
use std::time::Duration;

#[tokio::main]
async fn main() -> chalk_rs::error::Result<()> {
    let client = ChalkClient::new().build().await?;

    println!("Environment: {}", client.environment_id());

    // --- Simple inline offline query ---
    println!("\n=== Offline Query ===");

    let params = OfflineQueryParams::new()
        .with_input("user.id", vec![
            serde_json::json!(1),
            serde_json::json!(2),
            serde_json::json!(3),
        ])
        .with_output("user.id")
        .with_output("user.name")
        .with_output("user.age");

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

    println!("\nOffline query example completed!");
    Ok(())
}
