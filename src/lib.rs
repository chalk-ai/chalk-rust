//! # chalk-rs
//!
//! A Rust SDK for the [Chalk](https://chalk.ai) feature store.
//!
//! This crate provides an HTTP/REST client for online queries, offline queries,
//! bulk queries, and feature uploads.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use chalk_rs::ChalkClient;
//! use chalk_rs::types::QueryOptions;
//! use std::collections::HashMap;
//!
//! #[tokio::main]
//! async fn main() -> chalk_rs::error::Result<()> {
//!     // Build the client (credentials can come from env vars or ~/.chalk.yml).
//!     let client = ChalkClient::new()
//!         .client_id("your-client-id")
//!         .client_secret("your-client-secret")
//!         .environment("production")
//!         .build()
//!         .await?;
//!
//!     // Query features for a single user.
//!     let inputs = HashMap::from([
//!         ("user.id".to_string(), serde_json::json!(42)),
//!     ]);
//!     let outputs = vec!["user.age".to_string(), "user.name".to_string()];
//!
//!     let response = client.query(inputs, outputs, QueryOptions::default()).await?;
//!
//!     for feature in &response.data {
//!         println!("{}: {:?}", feature.field, feature.value);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Configuration Resolution
//!
//! Credentials are resolved in this priority order:
//! 1. Explicit values passed to the builder.
//! 2. Environment variables (`CHALK_CLIENT_ID`, `CHALK_CLIENT_SECRET`, etc.).
//! 3. `~/.chalk.yml` file (created by `chalk login`).
//! 4. Defaults (e.g. API server = `https://api.chalk.ai`).

/// Error types and the crate-wide `Result` alias.
pub mod error;

/// JSON request/response structs for the REST API.
pub mod types;

/// Configuration resolution (env vars, YAML, defaults).
pub mod config;

/// Token management (OAuth2 credential exchange + caching).
pub mod auth;

/// The HTTP/REST client.
pub mod http_client;

/// Fluent builder for offline query parameters.
pub mod offline;

pub use http_client::{BulkQueryResult, ChalkClient};
pub use offline::OfflineQueryParams;
