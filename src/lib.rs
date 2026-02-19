//! # chalk-rs
//!
//! The official [Chalk](https://chalk.ai) client library for Rust.
//!
//! Provides both an HTTP/REST client ([`ChalkClient`]) and a gRPC client
//! ([`ChalkGrpcClient`]) for online queries, bulk queries, and feature uploads.
//! Use the gRPC client for latency-sensitive workloads; use the HTTP client for
//! offline queries and general use.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use chalk_rs::ChalkClient;
//! use chalk_rs::types::QueryOptions;
//! use std::collections::HashMap;
//!
//! # async fn example() -> chalk_rs::error::Result<()> {
//! let client = ChalkClient::new()
//!     .client_id("your-client-id")
//!     .client_secret("your-client-secret")
//!     .environment("production")
//!     .build()
//!     .await?;
//!
//! let inputs = HashMap::from([
//!     ("user.id".to_string(), serde_json::json!(42)),
//! ]);
//! let outputs = vec!["user.age".to_string(), "user.name".to_string()];
//!
//! let response = client.query(inputs, outputs, QueryOptions::default()).await?;
//! for feature in &response.data {
//!     println!("{}: {:?}", feature.field, feature.value);
//! }
//! # Ok(())
//! # }
//! ```

pub mod error;
pub mod types;
pub mod config;
pub mod auth;
pub mod http_client;
pub mod offline;
pub mod gen;
pub mod grpc_client;

pub use http_client::{BulkQueryResult, ChalkClient};
pub use grpc_client::ChalkGrpcClient;
pub use offline::OfflineQueryParams;
