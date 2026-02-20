//! Token management — exchanging credentials for JWTs.
//!
//! The Chalk API uses OAuth2 client-credentials flow:
//!
//! 1. You POST your `client_id` + `client_secret` to `/v1/oauth/token`.
//! 2. The server returns a JWT (`access_token`) that expires after some time.
//! 3. You attach this JWT as a `Bearer` token on every subsequent request.
//!
//! This module handles the exchange *and* caches the token so we don't hit
//! the auth endpoint on every single query. The cache is thread-safe (using
//! `tokio::sync::RwLock`) so multiple async tasks can share a single
//! `TokenManager`.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::sync::RwLock;

use crate::config::ChalkClientConfig;
use crate::error::{ChalkClientError, Result};
use crate::types::{TokenExchangeRequest, TokenResponse};

/// How many seconds before actual expiry we consider a token "stale" and
/// refresh it proactively.
const TOKEN_REFRESH_BUFFER_SECS: i64 = 60;

/// A cached token plus the timestamp when it expires.
#[derive(Debug, Clone)]
struct CachedToken {
    response: TokenResponse,
    expires_at: DateTime<Utc>,
}

/// Manages authentication tokens for the Chalk client.
///
/// This struct is cheap to clone (the inner state is behind an `Arc`), so
/// it can be shared between clients.
#[derive(Clone)]
pub struct TokenManager {
    config: ChalkClientConfig,
    http_client: reqwest::Client,
    cache: Arc<RwLock<Option<CachedToken>>>,
}

impl TokenManager {
    /// Create a new `TokenManager` from the given config.
    pub fn new(config: ChalkClientConfig) -> Self {
        Self {
            config,
            http_client: reqwest::Client::new(),
            cache: Arc::new(RwLock::new(None)),
        }
    }

    /// Get a valid token, fetching or refreshing as needed.
    pub async fn get_token(&self) -> Result<TokenResponse> {
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.as_ref() {
                if is_token_valid(cached) {
                    return Ok(cached.response.clone());
                }
            }
        }
        let mut cache = self.cache.write().await;

        if let Some(cached) = cache.as_ref() {
            if is_token_valid(cached) {
                return Ok(cached.response.clone());
            }
        }

        let response = self.exchange_credentials().await?;
        let expires_at = parse_expiry(&response);

        *cache = Some(CachedToken {
            response: response.clone(),
            expires_at,
        });

        Ok(response)
    }

    /// POST to `/v1/oauth/token` to exchange client credentials for a JWT.
    async fn exchange_credentials(&self) -> Result<TokenResponse> {
        let url = format!("{}/v1/oauth/token", self.config.api_server);

        let body = TokenExchangeRequest {
            client_id: self.config.client_id.clone(),
            client_secret: self.config.client_secret.clone(),
            grant_type: "client_credentials".into(),
        };

        tracing::debug!("exchanging credentials at {}", url);

        let resp = self
            .http_client
            .post(&url)
            .json(&body)
            .header("Content-Type", "application/json")
            .header("User-Agent", "chalk-rust/0.1.0")
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            return Err(ChalkClientError::Auth(format!(
                "token exchange failed (HTTP {}): {}",
                status.as_u16(),
                body_text
            )));
        }

        let token: TokenResponse = resp.json().await?;
        tracing::debug!(
            "token exchanged successfully, primary_environment={:?}",
            token.primary_environment
        );

        Ok(token)
    }

    /// Returns a reference to the underlying config.
    pub fn config(&self) -> &ChalkClientConfig {
        &self.config
    }
}

fn is_token_valid(cached: &CachedToken) -> bool {
    let now = Utc::now();
    let remaining = cached.expires_at.signed_duration_since(now);
    remaining.num_seconds() > TOKEN_REFRESH_BUFFER_SECS
}

fn parse_expiry(response: &TokenResponse) -> DateTime<Utc> {
    if let Some(ref at) = response.expires_at {
        if let Ok(parsed) = at.parse::<DateTime<Utc>>() {
            return parsed;
        }
    }

    if let Some(seconds) = response.expires_in {
        return Utc::now() + chrono::Duration::seconds(seconds);
    }

    Utc::now() + chrono::Duration::hours(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ChalkClientConfigBuilder;
    use std::collections::HashMap;

    fn test_config(api_server: &str) -> ChalkClientConfig {
        ChalkClientConfigBuilder::new()
            .client_id("test-id")
            .client_secret("test-secret")
            .api_server(api_server)
            .build()
            .unwrap()
    }

    #[test]
    fn test_parse_expiry_from_expires_at() {
        let response = TokenResponse {
            access_token: "token".into(),
            expires_at: Some("2099-12-31T23:59:59Z".into()),
            expires_in: None,
            primary_environment: None,
            engines: HashMap::new(),
            grpc_engines: HashMap::new(),
            environment_id_to_name: HashMap::new(),
            api_server: None,
        };

        let expiry = parse_expiry(&response);
        assert!(expiry > Utc::now());
    }

    #[test]
    fn test_parse_expiry_from_expires_in() {
        let response = TokenResponse {
            access_token: "token".into(),
            expires_at: None,
            expires_in: Some(3600),
            primary_environment: None,
            engines: HashMap::new(),
            grpc_engines: HashMap::new(),
            environment_id_to_name: HashMap::new(),
            api_server: None,
        };

        let expiry = parse_expiry(&response);
        let now = Utc::now();
        let diff = expiry.signed_duration_since(now).num_seconds();
        assert!(diff > 3500 && diff <= 3600);
    }

    #[test]
    fn test_is_token_valid_expired() {
        let cached = CachedToken {
            response: TokenResponse {
                access_token: "token".into(),
                expires_at: None,
                expires_in: None,
                primary_environment: None,
                engines: HashMap::new(),
                grpc_engines: HashMap::new(),
                environment_id_to_name: HashMap::new(),
                api_server: None,
            },
            expires_at: Utc::now() - chrono::Duration::minutes(10),
        };

        assert!(!is_token_valid(&cached));
    }

    #[test]
    fn test_is_token_valid_fresh() {
        let cached = CachedToken {
            response: TokenResponse {
                access_token: "token".into(),
                expires_at: None,
                expires_in: None,
                primary_environment: None,
                engines: HashMap::new(),
                grpc_engines: HashMap::new(),
                environment_id_to_name: HashMap::new(),
                api_server: None,
            },
            expires_at: Utc::now() + chrono::Duration::minutes(30),
        };

        assert!(is_token_valid(&cached));
    }

    #[tokio::test]
    async fn test_token_exchange_success() {
        let mut server = mockito::Server::new_async().await;

        let mock = server
            .mock("POST", "/v1/oauth/token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "access_token": "mock-jwt-token",
                    "expires_in": 3600,
                    "primary_environment": "env-abc",
                    "engines": {"env-abc": "https://engine.chalk.ai"},
                    "grpc_engines": {"env-abc": "https://grpc.chalk.ai"},
                    "environment_id_to_name": {"env-abc": "production"}
                })
                .to_string(),
            )
            .create_async()
            .await;

        let config = test_config(&server.url());
        let manager = TokenManager::new(config);

        let token = manager.get_token().await.unwrap();
        assert_eq!(token.access_token, "mock-jwt-token");
        assert_eq!(token.primary_environment.as_deref(), Some("env-abc"));
        assert_eq!(
            token.engines.get("env-abc").map(|s| s.as_str()),
            Some("https://engine.chalk.ai")
        );

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_token_caching() {
        let mut server = mockito::Server::new_async().await;

        let mock = server
            .mock("POST", "/v1/oauth/token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "access_token": "cached-token",
                    "expires_in": 3600,
                    "engines": {},
                    "grpc_engines": {}
                })
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;

        let config = test_config(&server.url());
        let manager = TokenManager::new(config);

        let t1 = manager.get_token().await.unwrap();
        let t2 = manager.get_token().await.unwrap();

        assert_eq!(t1.access_token, t2.access_token);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_token_exchange_failure() {
        let mut server = mockito::Server::new_async().await;

        server
            .mock("POST", "/v1/oauth/token")
            .with_status(401)
            .with_body("invalid credentials")
            .create_async()
            .await;

        let config = test_config(&server.url());
        let manager = TokenManager::new(config);

        let result = manager.get_token().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("401"));
        assert!(err.contains("invalid credentials"));
    }
}
