//! Error types for the Chalk client SDK.
//!
//! This module defines a single error enum, [`ChalkClientError`](crate::error::ChalkClientError), that covers
//! everything that can go wrong when using the client — from configuration
//! mistakes to network failures to server-side errors.

use crate::types::ChalkError;

/// Everything that can go wrong when using the Chalk client.
#[derive(Debug, thiserror::Error)]
pub enum ChalkClientError {
    /// A configuration problem — missing credentials, bad YAML, etc.
    #[error("configuration error: {0}")]
    Config(String),

    /// An authentication failure — could not exchange credentials for a token.
    #[error("authentication error: {0}")]
    Auth(String),

    /// An HTTP-level error from the `reqwest` library (DNS failure, timeout,
    /// TLS handshake error, etc.).
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// The Chalk API returned an HTTP success status but the response body
    /// contained an error we couldn't map to `ServerErrors`. This is a
    /// catch-all for unexpected API-level failures.
    #[error("API error (status {status}): {message}")]
    Api {
        /// The HTTP status code returned by the server.
        status: u16,
        /// The error message from the response body.
        message: String,
    },

    /// Failed to serialize or deserialize JSON.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Failed to parse a YAML configuration file (e.g. `~/.chalk.yml`).
    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    /// An error from the Apache Arrow library (e.g. when encoding/decoding
    /// Arrow IPC for bulk queries).
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// A standard I/O error (file not found, permission denied, etc.).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// gRPC protocol error (e.g. deadline exceeded, unavailable).
    #[error("gRPC error: {0}")]
    Grpc(Box<tonic::Status>),

    /// gRPC transport/connection error.
    #[error("gRPC transport error: {0}")]
    GrpcTransport(#[from] tonic::transport::Error),

    /// The Chalk server returned one or more structured errors in its response.
    #[error("server returned {} error(s): {}", .0.len(), .0.first().map(|e| e.message.as_str()).unwrap_or("unknown"))]
    ServerErrors(Vec<ChalkError>),
}

impl From<tonic::Status> for ChalkClientError {
    fn from(status: tonic::Status) -> Self {
        ChalkClientError::Grpc(Box::new(status))
    }
}

/// A convenience type alias so we can write `Result<T>` instead of
/// `std::result::Result<T, ChalkClientError>` everywhere in this crate.
pub type Result<T> = std::result::Result<T, ChalkClientError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = ChalkClientError::Config("missing client_id".into());
        assert!(err.to_string().contains("missing client_id"));

        let err = ChalkClientError::Api {
            status: 401,
            message: "unauthorized".into(),
        };
        assert!(err.to_string().contains("401"));
        assert!(err.to_string().contains("unauthorized"));
    }

    #[test]
    fn test_server_errors_display() {
        let errors = vec![
            ChalkError {
                code: "RESOLVER_FAILED".into(),
                category: "FIELD".into(),
                message: "resolver timed out".into(),
                feature: Some("user.age".into()),
                resolver: Some("get_user_age".into()),
                exception: None,
            },
            ChalkError {
                code: "RESOLVER_FAILED".into(),
                category: "FIELD".into(),
                message: "another error".into(),
                feature: None,
                resolver: None,
                exception: None,
            },
        ];
        let err = ChalkClientError::ServerErrors(errors);
        let msg = err.to_string();
        assert!(msg.contains("2 error(s)"));
        assert!(msg.contains("resolver timed out"));
    }
}
