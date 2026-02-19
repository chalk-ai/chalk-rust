//! gRPC client for the Chalk feature store.

use tonic::transport::{Channel, ClientTlsConfig};

use crate::auth::TokenManager;
use crate::config::{ChalkClientConfig, ChalkClientConfigBuilder, ensure_scheme};
use crate::error::{ChalkClientError, Result};
use crate::gen::chalk::common::v1::{
    OnlineQueryBulkRequest as ProtoOnlineQueryBulkRequest,
    OnlineQueryBulkResponse as ProtoOnlineQueryBulkResponse,
    OnlineQueryRequest as ProtoOnlineQueryRequest,
    OnlineQueryResponse as ProtoOnlineQueryResponse,
    UploadFeaturesBulkRequest as ProtoUploadFeaturesBulkRequest,
    UploadFeaturesBulkResponse as ProtoUploadFeaturesBulkResponse,
};
use crate::gen::chalk::engine::v1::query_service_client::QueryServiceClient;

const USER_AGENT: &str = "chalk-rust-grpc/0.1.0";

/// A gRPC client for the Chalk feature store.
///
/// [`ChalkGrpcClient`] is an alternative to [`ChalkClient`](crate::ChalkClient)
/// that uses gRPC (HTTP/2 + Protocol Buffers) instead of REST/JSON for lower
/// latency and higher throughput.
///
/// Supports [`query_proto`](Self::query_proto), [`query_bulk_proto`](Self::query_bulk_proto),
/// and [`upload_features_proto`](Self::upload_features_proto). These are
/// low-level methods that accept raw protobuf types. Offline queries are
/// only available via the REST client.
///
/// # Example
///
/// ```rust,no_run
/// use chalk_rs::ChalkGrpcClient;
/// use chalk_rs::gen::chalk::common::v1::{OnlineQueryRequest, OutputExpr};
/// use std::collections::HashMap;
///
/// # async fn example() -> chalk_rs::error::Result<()> {
/// let client = ChalkGrpcClient::new()
///     .client_id("your-client-id")
///     .client_secret("your-client-secret")
///     .environment("production")
///     .build()
///     .await?;
///
/// let request = OnlineQueryRequest {
///     inputs: HashMap::from([(
///         "user.id".to_string(),
///         prost_types::Value {
///             kind: Some(prost_types::value::Kind::NumberValue(42.0)),
///         },
///     )]),
///     outputs: vec![OutputExpr {
///         expr: Some(chalk_rs::gen::chalk::common::v1::output_expr::Expr::FeatureFqn(
///             "user.name".to_string(),
///         )),
///     }],
///     ..Default::default()
/// };
///
/// let response = client.query_proto(request).await?;
/// # Ok(())
/// # }
/// ```
pub struct ChalkGrpcClient {
    config: ChalkClientConfig,
    token_manager: TokenManager,
    grpc_client: QueryServiceClient<Channel>,
    environment_id: String,
}

/// Builder for [`ChalkGrpcClient`].
pub struct ChalkGrpcClientBuilder {
    config_builder: ChalkClientConfigBuilder,
}

#[allow(clippy::new_ret_no_self)]
impl ChalkGrpcClient {
    /// Creates a new [`ChalkGrpcClientBuilder`] with authentication settings configured.
    ///
    /// Configuration is resolved from the first available source:
    /// 1. Explicit values passed to the builder.
    /// 2. Environment variables: `CHALK_CLIENT_ID`, `CHALK_CLIENT_SECRET`,
    ///    `CHALK_API_SERVER`, `CHALK_ACTIVE_ENVIRONMENT`.
    /// 3. `~/.chalk.yml` file, created by running `chalk login`.
    pub fn new() -> ChalkGrpcClientBuilder {
        ChalkGrpcClientBuilder {
            config_builder: ChalkClientConfigBuilder::new(),
        }
    }
}

impl ChalkGrpcClientBuilder {
    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.config_builder = self.config_builder.client_id(id);
        self
    }

    pub fn client_secret(mut self, secret: impl Into<String>) -> Self {
        self.config_builder = self.config_builder.client_secret(secret);
        self
    }

    pub fn api_server(mut self, url: impl Into<String>) -> Self {
        self.config_builder = self.config_builder.api_server(url);
        self
    }

    pub fn environment(mut self, env: impl Into<String>) -> Self {
        self.config_builder = self.config_builder.environment(env);
        self
    }

    /// If specified, Chalk will route all requests from this client
    /// to the relevant branch.
    pub fn branch_id(mut self, id: impl Into<String>) -> Self {
        self.config_builder = self.config_builder.branch_id(id);
        self
    }

    /// Chalk can route queries to specific deployments using deployment tags.
    pub fn deployment_tag(mut self, tag: impl Into<String>) -> Self {
        self.config_builder = self.config_builder.deployment_tag(tag);
        self
    }

    /// Chalk routes performance-sensitive requests like online query directly
    /// to the query engine. Set this to override the automatically resolved
    /// query server URL.
    pub fn query_server(mut self, url: impl Into<String>) -> Self {
        self.config_builder = self.config_builder.query_server(url);
        self
    }

    /// Build the gRPC client, exchanging credentials for a token and
    /// establishing an HTTP/2 connection to the query engine.
    pub async fn build(self) -> Result<ChalkGrpcClient> {
        let config = self.config_builder.build()?;
        let token_manager = TokenManager::new(config.clone());
        let token = token_manager.get_token().await?;

        let environment_id = config
            .environment
            .clone()
            .or(token.primary_environment.clone())
            .ok_or_else(|| {
                ChalkClientError::Config(
                    "no environment specified and token has no primary_environment".into(),
                )
            })?;

        // Priority: explicit query_server > grpc_engines > engines > api_server
        let grpc_url = ensure_scheme(
            config
                .query_server
                .clone()
                .or_else(|| token.grpc_engines.get(&environment_id).cloned())
                .or_else(|| token.engines.get(&environment_id).cloned())
                .unwrap_or_else(|| config.api_server.clone()),
        );

        tracing::info!(
            environment = %environment_id,
            grpc_url = %grpc_url,
            "connecting gRPC channel"
        );

        let mut endpoint = Channel::from_shared(grpc_url.clone()).map_err(|e| {
            ChalkClientError::Config(format!("invalid gRPC URL '{}': {}", grpc_url, e))
        })?;

        if grpc_url.starts_with("https://") {
            endpoint = endpoint
                .tls_config(ClientTlsConfig::new().with_native_roots())
                .map_err(|e| {
                    ChalkClientError::Config(format!("TLS configuration error: {}", e))
                })?;
        }

        let channel = endpoint.connect().await?;

        let grpc_client = QueryServiceClient::new(channel);

        tracing::info!("ChalkGrpcClient connected to {}", grpc_url);

        Ok(ChalkGrpcClient {
            config,
            token_manager,
            grpc_client,
            environment_id,
        })
    }
}

impl ChalkGrpcClient {
    /// Low-level: computes feature values for a single entity using the raw
    /// protobuf request/response types.
    ///
    /// Prefer a higher-level wrapper (when available) over constructing proto
    /// messages by hand. See <https://docs.chalk.ai/docs/query-basics>.
    pub async fn query_proto(
        &self,
        request: ProtoOnlineQueryRequest,
    ) -> Result<ProtoOnlineQueryResponse> {
        let mut client = self.grpc_client.clone();
        let mut req = tonic::Request::new(request);
        self.inject_metadata(req.metadata_mut()).await?;
        let response = client.online_query(req).await?;
        Ok(response.into_inner())
    }

    /// Low-level: computes feature values for multiple entities at once using
    /// the raw protobuf request/response types.
    ///
    /// Inputs and outputs use Arrow IPC (Feather) encoding inside the proto
    /// messages.
    pub async fn query_bulk_proto(
        &self,
        request: ProtoOnlineQueryBulkRequest,
    ) -> Result<ProtoOnlineQueryBulkResponse> {
        let mut client = self.grpc_client.clone();
        let mut req = tonic::Request::new(request);
        self.inject_metadata(req.metadata_mut()).await?;
        let response = client.online_query_bulk(req).await?;
        Ok(response.into_inner())
    }

    /// Low-level: uploads pre-computed feature values using the raw protobuf
    /// request/response types.
    pub async fn upload_features_proto(
        &self,
        request: ProtoUploadFeaturesBulkRequest,
    ) -> Result<ProtoUploadFeaturesBulkResponse> {
        let mut client = self.grpc_client.clone();
        let mut req = tonic::Request::new(request);
        self.inject_metadata(req.metadata_mut()).await?;
        let response = client.upload_features_bulk(req).await?;
        Ok(response.into_inner())
    }

    /// Returns the resolved environment ID.
    pub fn environment_id(&self) -> &str {
        &self.environment_id
    }

    /// Returns the current client configuration.
    pub fn config(&self) -> &ChalkClientConfig {
        &self.config
    }

    async fn inject_metadata(&self, metadata: &mut tonic::metadata::MetadataMap) -> Result<()> {
        let token = self.token_manager.get_token().await?;

        metadata.insert(
            "authorization",
            format!("Bearer {}", token.access_token)
                .parse()
                .map_err(|e| {
                    ChalkClientError::Auth(format!("invalid token for metadata: {}", e))
                })?,
        );
        metadata.insert(
            "x-chalk-env-id",
            self.environment_id
                .parse()
                .map_err(|e| ChalkClientError::Config(format!("invalid env ID: {}", e)))?,
        );
        metadata.insert(
            "x-chalk-client-id",
            self.config
                .client_id
                .parse()
                .map_err(|e| ChalkClientError::Config(format!("invalid client ID: {}", e)))?,
        );
        metadata.insert(
            "user-agent",
            USER_AGENT
                .parse()
                .map_err(|e| ChalkClientError::Config(format!("invalid user-agent: {}", e)))?,
        );
        metadata.insert(
            "x-chalk-deployment-type",
            "engine-grpc".parse().unwrap(),
        );
        metadata.insert("x-chalk-server", "engine".parse().unwrap());

        if let Some(ref branch) = self.config.branch_id {
            metadata.insert(
                "x-chalk-branch-id",
                branch
                    .parse()
                    .map_err(|e| ChalkClientError::Config(format!("invalid branch ID: {}", e)))?,
            );
        }
        if let Some(ref tag) = self.config.deployment_tag {
            metadata.insert(
                "x-chalk-deployment-tag",
                tag.parse().map_err(|e| {
                    ChalkClientError::Config(format!("invalid deployment tag: {}", e))
                })?,
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metadata_injection() {
        let mut server = mockito::Server::new_async().await;

        server
            .mock("POST", "/v1/oauth/token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "access_token": "test-grpc-jwt",
                    "expires_in": 3600,
                    "primary_environment": "env-1",
                    "engines": {},
                    "grpc_engines": {}
                })
                .to_string(),
            )
            .create_async()
            .await;

        let config = ChalkClientConfigBuilder::new()
            .client_id("grpc-test-id")
            .client_secret("grpc-test-secret")
            .api_server(&server.url())
            .environment("env-1")
            .branch_id("branch-42")
            .deployment_tag("canary")
            .build()
            .unwrap();

        let token_manager = TokenManager::new(config.clone());
        let token = token_manager.get_token().await.unwrap();
        assert_eq!(token.access_token, "test-grpc-jwt");

        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert(
            "authorization",
            format!("Bearer {}", token.access_token).parse().unwrap(),
        );
        metadata.insert("x-chalk-env-id", "env-1".parse().unwrap());
        metadata.insert("x-chalk-client-id", "grpc-test-id".parse().unwrap());
        metadata.insert("user-agent", USER_AGENT.parse().unwrap());
        metadata.insert("x-chalk-branch-id", "branch-42".parse().unwrap());
        metadata.insert("x-chalk-deployment-tag", "canary".parse().unwrap());

        assert_eq!(
            metadata.get("authorization").unwrap().to_str().unwrap(),
            "Bearer test-grpc-jwt"
        );
        assert_eq!(
            metadata.get("x-chalk-env-id").unwrap().to_str().unwrap(),
            "env-1"
        );
        assert_eq!(
            metadata.get("x-chalk-branch-id").unwrap().to_str().unwrap(),
            "branch-42"
        );
        assert_eq!(
            metadata
                .get("x-chalk-deployment-tag")
                .unwrap()
                .to_str()
                .unwrap(),
            "canary"
        );
    }
}
