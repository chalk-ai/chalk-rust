//! HTTP/REST client for the Chalk feature store.
//!
//! [`ChalkClient`] is the main entry point for most users. It talks to the
//! Chalk API over HTTP/JSON (for `query`, `offline_query`) and HTTP with
//! Arrow IPC binary bodies (for `query_bulk`, `upload_features`).
//!
//! ## Builder pattern
//!
//! You construct a `ChalkClient` using the builder, which mirrors the config
//! builder but also performs the initial token exchange:
//!
//! ```rust,no_run
//! use chalk_rs::ChalkClient;
//!
//! # async fn example() -> chalk_rs::error::Result<()> {
//! let client = ChalkClient::new()
//!     .client_id("my-client-id")
//!     .client_secret("my-secret")
//!     .environment("production")
//!     .build()
//!     .await?;
//!
//! // Now use it:
//! // let response = client.query(inputs, outputs, options).await?;
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::time::Duration;

use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use serde::Serialize;
use serde_json::Value;

use crate::auth::TokenManager;
use crate::config::{ChalkClientConfig, ChalkClientConfigBuilder};
use crate::error::{ChalkClientError, Result};
use crate::offline::OfflineQueryParams;
use crate::types::{
    FeatureEncodingOptions, GetOfflineQueryJobResponse, GetOfflineQueryStatusResponse,
    OfflineQueryRequest, OfflineQueryResponse, OnlineQueryContext, OnlineQueryRequest,
    OnlineQueryResponse, QueryOptions, UploadFeaturesResult,
};

/// The User-Agent string we send with every request.
const USER_AGENT: &str = "chalk-rust/0.1.0";

/// Magic string that marks the start of a multi-query feather request.
const MULTI_QUERY_MAGIC_STR: &[u8] = b"chal1";

/// Magic string that marks the start of a ByteBaseModel response.
const BYTEMODEL_MAGIC_STR: &[u8] = b"CHALK_BYTE_TRANSMISSION";

// =========================================================================
// ChalkClient
// =========================================================================

/// An HTTP/REST client for the Chalk feature store.
pub struct ChalkClient {
    /// The resolved configuration.
    config: ChalkClientConfig,

    /// Manages JWT tokens (exchange + caching).
    token_manager: TokenManager,

    /// The underlying HTTP client (connection pooling, TLS, etc.).
    http_client: reqwest::Client,

    /// The resolved query server URL.
    query_server: String,

    /// The resolved environment ID.
    environment_id: String,
}

// =========================================================================
// Builder
// =========================================================================

/// Builder for [`ChalkClient`].
pub struct ChalkClientBuilder {
    config_builder: ChalkClientConfigBuilder,
}

impl ChalkClient {
    /// Start building a new `ChalkClient`.
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> ChalkClientBuilder {
        ChalkClientBuilder {
            config_builder: ChalkClientConfigBuilder::new(),
        }
    }
}

impl ChalkClientBuilder {
    /// Set the OAuth2 client ID.
    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.config_builder = self.config_builder.client_id(id);
        self
    }

    /// Set the OAuth2 client secret.
    pub fn client_secret(mut self, secret: impl Into<String>) -> Self {
        self.config_builder = self.config_builder.client_secret(secret);
        self
    }

    /// Set the API server URL.
    pub fn api_server(mut self, url: impl Into<String>) -> Self {
        self.config_builder = self.config_builder.api_server(url);
        self
    }

    /// Set the target environment.
    pub fn environment(mut self, env: impl Into<String>) -> Self {
        self.config_builder = self.config_builder.environment(env);
        self
    }

    /// Set the branch ID.
    pub fn branch_id(mut self, id: impl Into<String>) -> Self {
        self.config_builder = self.config_builder.branch_id(id);
        self
    }

    /// Set the deployment tag.
    pub fn deployment_tag(mut self, tag: impl Into<String>) -> Self {
        self.config_builder = self.config_builder.deployment_tag(tag);
        self
    }

    /// Set the query server URL directly.
    pub fn query_server(mut self, url: impl Into<String>) -> Self {
        self.config_builder = self.config_builder.query_server(url);
        self
    }

    /// Build the client.
    ///
    /// This is `async` because it performs the initial token exchange to
    /// discover the query engine URL and validate credentials.
    pub async fn build(self) -> Result<ChalkClient> {
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

        let query_server = config
            .query_server
            .clone()
            .or_else(|| token.engines.get(&environment_id).cloned())
            .unwrap_or_else(|| config.api_server.clone());

        tracing::info!(
            environment = %environment_id,
            query_server = %query_server,
            "ChalkClient initialized"
        );

        Ok(ChalkClient {
            config,
            token_manager,
            http_client: reqwest::Client::new(),
            query_server,
            environment_id,
        })
    }
}

// =========================================================================
// Query methods
// =========================================================================

impl ChalkClient {
    /// Query features online (single entity, JSON request/response).
    ///
    /// # Arguments
    ///
    /// * `inputs` — Known feature values, e.g. `{"user.id": 42}`.
    /// * `outputs` — Which features to compute, e.g. `["user.age", "user.name"]`.
    /// * `options` — Optional settings (staleness, tags, etc.).
    pub async fn query(
        &self,
        inputs: HashMap<String, Value>,
        outputs: Vec<String>,
        options: QueryOptions,
    ) -> Result<OnlineQueryResponse> {
        let url = format!("{}/v1/query/online", self.engine_url());

        let body = OnlineQueryRequest {
            inputs,
            outputs,
            context: options.context,
            staleness: options.staleness,
            include_meta: options.include_meta,
            query_name: options.query_name,
            correlation_id: options.correlation_id,
            query_context: options.query_context,
            meta: options.meta,
            query_name_version: options.query_name_version,
            now: options.now,
            explain: options.explain,
            store_plan_stages: options.store_plan_stages,
            encoding_options: options.encoding_options,
            branch_id: options.branch_id.or(self.config.branch_id.clone()),
        };

        let resp = self
            .send_json_request(reqwest::Method::POST, &url, &body)
            .await?;

        let status = resp.status();
        let body_text = resp.text().await?;

        if !status.is_success() {
            return Err(ChalkClientError::Api {
                status: status.as_u16(),
                message: body_text,
            });
        }

        let response: OnlineQueryResponse = serde_json::from_str(&body_text)?;

        if !response.errors.is_empty() {
            tracing::warn!(
                error_count = response.errors.len(),
                "query returned server errors"
            );
        }

        Ok(response)
    }

    /// Query features in bulk using the Chalk feather protocol.
    ///
    /// You provide inputs as an Arrow `RecordBatch` (one column per input
    /// feature, one row per entity) and get back a `BulkQueryResult` containing
    /// the output features as raw Feather bytes.
    pub async fn query_bulk(
        &self,
        inputs: &RecordBatch,
        outputs: Vec<String>,
        options: QueryOptions,
    ) -> Result<BulkQueryResult> {
        let url = format!("{}/v1/query/feather", self.engine_url());

        let header = FeatherRequestHeader {
            outputs: outputs.clone(),
            expression_outputs: vec![],
            now: None,
            staleness: options.staleness,
            context: options.context,
            include_meta: options.include_meta.unwrap_or(true),
            explain: options.explain.unwrap_or(false),
            correlation_id: options.correlation_id,
            query_name: options.query_name,
            query_name_version: options.query_name_version,
            deployment_id: None,
            branch_id: options.branch_id.or(self.config.branch_id.clone()),
            meta: options.meta,
            store_plan_stages: options.store_plan_stages.or(Some(false)),
            query_context: options.query_context,
            encoding_options: options
                .encoding_options
                .unwrap_or(FeatureEncodingOptions {
                    encode_structs_as_objects: None,
                }),
            planner_options: options.planner_options,
            value_metrics_tag_by_features: vec![],
            overlay_graph: None,
        };

        let feather_bytes = serialize_record_batch_to_feather(inputs)?;

        let request_body = build_feather_request_body(&header, &feather_bytes)?;

        let token = self.token_manager.get_token().await?;

        let deployment_type = if self.config.branch_id.is_some() {
            "branch"
        } else {
            "engine"
        };

        let mut request = self
            .http_client
            .post(&url)
            .header("Authorization", format!("Bearer {}", token.access_token))
            .header("User-Agent", USER_AGENT)
            .header("Content-Type", "application/octet-stream")
            .header("Accept", "application/octet-stream")
            .header("X-Chalk-Client-Id", &self.config.client_id)
            .header("X-Chalk-Env-Id", &self.environment_id)
            .header("X-Chalk-Deployment-Type", deployment_type)
            .header("X-Chalk-Features-Versioned", "true");

        if let Some(ref branch) = self.config.branch_id {
            request = request.header("X-Chalk-Branch-Id", branch.as_str());
        }
        if let Some(ref tag) = self.config.deployment_tag {
            request = request.header("X-Chalk-Deployment-Tag", tag);
        }

        let resp = request.body(request_body).send().await?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ChalkClientError::Api {
                status: status.as_u16(),
                message: body,
            });
        }

        let response_bytes = resp.bytes().await?;
        parse_bulk_query_response(&response_bytes)
    }

    /// Run an offline query using the builder pattern.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use chalk_rs::{ChalkClient, OfflineQueryParams};
    /// # async fn example(client: &ChalkClient) -> chalk_rs::error::Result<()> {
    /// let response = client.offline_query(
    ///     OfflineQueryParams::new()
    ///         .with_input("user.id", vec![serde_json::json!(1), serde_json::json!(2)])
    ///         .with_output("user.email")
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn offline_query(
        &self,
        params: OfflineQueryParams,
    ) -> Result<OfflineQueryResponse> {
        let request = params.build()?;
        self.offline_query_raw(request).await
    }

    /// Run an offline query with a raw [`OfflineQueryRequest`].
    pub async fn offline_query_raw(
        &self,
        request: OfflineQueryRequest,
    ) -> Result<OfflineQueryResponse> {
        let url = format!("{}/v4/offline_query", self.config.api_server);

        let resp = self
            .send_json_request(reqwest::Method::POST, &url, &request)
            .await?;

        let status = resp.status();
        let body_text = resp.text().await?;

        if !status.is_success() {
            return Err(ChalkClientError::Api {
                status: status.as_u16(),
                message: body_text,
            });
        }

        let response: OfflineQueryResponse = serde_json::from_str(&body_text)?;
        Ok(response)
    }

    /// Get the status of an offline query job.
    pub async fn get_offline_query_status(
        &self,
        job_id: &str,
    ) -> Result<GetOfflineQueryStatusResponse> {
        let url = format!(
            "{}/v4/offline_query/{}/status",
            self.config.api_server, job_id
        );

        let resp = self
            .send_get_request(&url)
            .await?;

        let status = resp.status();
        let body_text = resp.text().await?;

        if !status.is_success() {
            return Err(ChalkClientError::Api {
                status: status.as_u16(),
                message: body_text,
            });
        }

        let response: GetOfflineQueryStatusResponse = serde_json::from_str(&body_text)?;
        Ok(response)
    }

    /// Wait for an offline query job to complete.
    ///
    /// Polls [`get_offline_query_status`](Self::get_offline_query_status) every
    /// second until the job reaches `"COMPLETED"` or `"FAILED"` status.
    pub async fn wait_for_offline_query(
        &self,
        response: &OfflineQueryResponse,
        timeout: Option<Duration>,
    ) -> Result<()> {
        let revision = response
            .revisions
            .last()
            .and_then(|r| r.revision_id.as_deref())
            .ok_or_else(|| {
                ChalkClientError::Config("offline query response has no revision ID".into())
            })?;

        let poll_fut = async {
            loop {
                let status_resp = self.get_offline_query_status(revision).await?;
                let report = match status_resp.report {
                    Some(r) => r,
                    None => {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
                let status = report.status.as_deref().unwrap_or("UNKNOWN");

                match status {
                    "COMPLETED" => return Ok(()),
                    "FAILED" => {
                        let errors = report.all_errors;
                        if errors.is_empty() {
                            if let Some(err) = report.error {
                                return Err(ChalkClientError::ServerErrors(vec![err]));
                            }
                            return Err(ChalkClientError::Api {
                                status: 0,
                                message: "offline query failed with no error details".into(),
                            });
                        }
                        return Err(ChalkClientError::ServerErrors(errors));
                    }
                    _ => {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        };

        if let Some(timeout_dur) = timeout {
            tokio::time::timeout(timeout_dur, poll_fut)
                .await
                .map_err(|_| {
                    ChalkClientError::Api {
                        status: 0,
                        message: format!(
                            "timed out waiting for offline query after {:?}",
                            timeout_dur
                        ),
                    }
                })?
        } else {
            poll_fut.await
        }
    }

    /// Get download URLs for an offline query's result Parquet files.
    pub async fn get_offline_query_download_urls(
        &self,
        response: &OfflineQueryResponse,
        timeout: Option<Duration>,
    ) -> Result<Vec<String>> {
        let revision_id = response
            .revisions
            .last()
            .and_then(|r| r.revision_id.as_deref())
            .ok_or_else(|| {
                ChalkClientError::Config("offline query response has no revision ID".into())
            })?;

        let poll_fut = async {
            loop {
                let url = format!(
                    "{}/v2/offline_query/{}",
                    self.config.api_server, revision_id
                );

                let resp = self.send_get_request(&url).await?;
                let status = resp.status();
                let body_text = resp.text().await?;

                if !status.is_success() {
                    return Err(ChalkClientError::Api {
                        status: status.as_u16(),
                        message: body_text,
                    });
                }

                let job_resp: GetOfflineQueryJobResponse = serde_json::from_str(&body_text)?;

                if job_resp.is_finished {
                    if !job_resp.errors.is_empty() {
                        return Err(ChalkClientError::ServerErrors(job_resp.errors));
                    }
                    return Ok(job_resp.urls);
                }

                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        };

        if let Some(timeout_dur) = timeout {
            tokio::time::timeout(timeout_dur, poll_fut)
                .await
                .map_err(|_| {
                    ChalkClientError::Api {
                        status: 0,
                        message: format!(
                            "timed out waiting for download URLs after {:?}",
                            timeout_dur
                        ),
                    }
                })?
        } else {
            poll_fut.await
        }
    }

    /// Upload feature values to the Chalk feature store.
    ///
    /// # Arguments
    ///
    /// * `features` — An Arrow RecordBatch where each column is a feature
    ///   (column names are feature FQNs like `"user.age"`).
    pub async fn upload_features(
        &self,
        features: &RecordBatch,
    ) -> Result<UploadFeaturesResult> {
        let url = format!("{}/v1/upload_features/multi", self.engine_url());

        let feature_names: Vec<String> = features
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let feather_bytes = serialize_record_batch_to_feather(features)?;

        let json_attrs = serde_json::json!({
            "features": feature_names,
            "table_compression": "uncompressed",
        });
        let body = build_byte_base_model(&json_attrs, &[("table_bytes", &feather_bytes)])?;

        let token = self.token_manager.get_token().await?;

        let deployment_type = if self.config.branch_id.is_some() {
            "branch"
        } else {
            "engine"
        };

        let mut request = self
            .http_client
            .post(&url)
            .header("Authorization", format!("Bearer {}", token.access_token))
            .header("User-Agent", USER_AGENT)
            .header("Content-Type", "application/octet-stream")
            .header("Accept", "application/json")
            .header("X-Chalk-Client-Id", &self.config.client_id)
            .header("X-Chalk-Env-Id", &self.environment_id)
            .header("X-Chalk-Deployment-Type", deployment_type)
            .header("X-Chalk-Features-Versioned", "true");

        if let Some(ref branch) = self.config.branch_id {
            request = request.header("X-Chalk-Branch-Id", branch.as_str());
        }
        if let Some(ref tag) = self.config.deployment_tag {
            request = request.header("X-Chalk-Deployment-Tag", tag);
        }

        let resp = request.body(body).send().await?;

        let status = resp.status();
        let body_text = resp.text().await?;

        if !status.is_success() {
            return Err(ChalkClientError::Api {
                status: status.as_u16(),
                message: body_text,
            });
        }

        let result: UploadFeaturesResult = serde_json::from_str(&body_text)?;

        if !result.errors.is_empty() {
            tracing::warn!(
                error_count = result.errors.len(),
                "upload_features returned server errors"
            );
        }

        Ok(result)
    }

    /// Upload feature values from a map of feature names to value arrays.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use chalk_rs::ChalkClient;
    /// # use std::collections::HashMap;
    /// # async fn example(client: &ChalkClient) -> chalk_rs::error::Result<()> {
    /// let inputs = HashMap::from([
    ///     ("user.id".to_string(), vec![serde_json::json!(1), serde_json::json!(2)]),
    ///     ("user.name".to_string(), vec![serde_json::json!("Alice"), serde_json::json!("Bob")]),
    /// ]);
    /// let result = client.upload_features_map(inputs).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn upload_features_map(
        &self,
        inputs: HashMap<String, Vec<Value>>,
    ) -> Result<UploadFeaturesResult> {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        if inputs.is_empty() {
            return Err(ChalkClientError::Config(
                "upload_features_map requires at least one feature".into(),
            ));
        }

        let mut feature_names: Vec<String> = inputs.keys().cloned().collect();
        feature_names.sort();

        let num_rows = inputs[&feature_names[0]].len();

        let fields: Vec<Field> = feature_names
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        let columns: Vec<Arc<dyn arrow::array::Array>> = feature_names
            .iter()
            .map(|name| {
                let values = &inputs[name];
                let strings: Vec<Option<String>> = values
                    .iter()
                    .map(|v| match v {
                        Value::Null => None,
                        Value::String(s) => Some(s.clone()),
                        other => Some(other.to_string()),
                    })
                    .collect();
                Arc::new(StringArray::from(strings)) as Arc<dyn arrow::array::Array>
            })
            .collect();

        let batch = RecordBatch::try_new(schema, columns).map_err(|e| {
            ChalkClientError::Arrow(e)
        })?;

        if batch.num_rows() != num_rows {
            return Err(ChalkClientError::Config(
                "all input arrays must be the same length".into(),
            ));
        }

        self.upload_features(&batch).await
    }

    /// Returns the resolved environment ID.
    pub fn environment_id(&self) -> &str {
        &self.environment_id
    }

    /// Returns the resolved query server URL.
    pub fn query_server(&self) -> &str {
        &self.query_server
    }

    // =====================================================================
    // Internal helpers
    // =====================================================================

    fn engine_url(&self) -> &str {
        if self.config.branch_id.is_some() {
            &self.config.api_server
        } else {
            &self.query_server
        }
    }

    async fn send_json_request<T: serde::Serialize>(
        &self,
        method: reqwest::Method,
        url: &str,
        body: &T,
    ) -> Result<reqwest::Response> {
        let token = self.token_manager.get_token().await?;

        let deployment_type = if self.config.branch_id.is_some() {
            "branch"
        } else {
            "engine"
        };

        let mut request = self
            .http_client
            .request(method, url)
            .header("Authorization", format!("Bearer {}", token.access_token))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header("User-Agent", USER_AGENT)
            .header("X-Chalk-Client-Id", &self.config.client_id)
            .header("X-Chalk-Env-Id", &self.environment_id)
            .header("X-Chalk-Deployment-Type", deployment_type)
            .header("X-Chalk-Features-Versioned", "true");

        if let Some(ref branch) = self.config.branch_id {
            request = request.header("X-Chalk-Branch-Id", branch.as_str());
        }
        if let Some(ref tag) = self.config.deployment_tag {
            request = request.header("X-Chalk-Deployment-Tag", tag);
        }

        let resp = request.json(body).send().await?;
        Ok(resp)
    }

    async fn send_get_request(&self, url: &str) -> Result<reqwest::Response> {
        let token = self.token_manager.get_token().await?;

        let deployment_type = if self.config.branch_id.is_some() {
            "branch"
        } else {
            "engine"
        };

        let mut request = self
            .http_client
            .get(url)
            .header("Authorization", format!("Bearer {}", token.access_token))
            .header("Accept", "application/json")
            .header("User-Agent", USER_AGENT)
            .header("X-Chalk-Client-Id", &self.config.client_id)
            .header("X-Chalk-Env-Id", &self.environment_id)
            .header("X-Chalk-Deployment-Type", deployment_type)
            .header("X-Chalk-Features-Versioned", "true");

        if let Some(ref branch) = self.config.branch_id {
            request = request.header("X-Chalk-Branch-Id", branch.as_str());
        }
        if let Some(ref tag) = self.config.deployment_tag {
            request = request.header("X-Chalk-Deployment-Tag", tag);
        }

        let resp = request.send().await?;
        Ok(resp)
    }
}

// =========================================================================
// Feather request protocol types
// =========================================================================

#[derive(Debug, Serialize)]
struct FeatherRequestHeader {
    outputs: Vec<String>,
    #[serde(default)]
    expression_outputs: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    now: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    staleness: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    context: Option<OnlineQueryContext>,
    include_meta: bool,
    explain: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    correlation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    query_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    query_name_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deployment_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    branch_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    meta: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    store_plan_stages: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    query_context: Option<HashMap<String, Value>>,
    encoding_options: FeatureEncodingOptions,
    #[serde(skip_serializing_if = "Option::is_none")]
    planner_options: Option<HashMap<String, Value>>,
    #[serde(default)]
    value_metrics_tag_by_features: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    overlay_graph: Option<String>,
}

// =========================================================================
// Bulk query response types
// =========================================================================

/// The result of a bulk (feather) query.
#[derive(Debug)]
pub struct BulkQueryResult {
    /// The output features as raw Feather (Arrow IPC file) bytes.
    pub scalar_data: Vec<u8>,

    /// Whether the server indicated it has data.
    pub has_data: bool,

    /// JSON-stringified query metadata.
    pub meta: Option<String>,

    /// JSON-stringified error objects from the server.
    pub errors: Vec<String>,
}

// =========================================================================
// Feather request serialization
// =========================================================================

fn build_feather_request_body(header: &FeatherRequestHeader, feather_bytes: &[u8]) -> Result<Vec<u8>> {
    let header_json = serde_json::to_string(header)?;
    let header_bytes = header_json.as_bytes();

    let total_size = 5 + 8 + header_bytes.len() + 8 + feather_bytes.len();
    let mut buf = Vec::with_capacity(total_size);

    buf.extend_from_slice(MULTI_QUERY_MAGIC_STR);

    buf.extend_from_slice(&(header_bytes.len() as u64).to_be_bytes());
    buf.extend_from_slice(header_bytes);

    buf.extend_from_slice(&(feather_bytes.len() as u64).to_be_bytes());
    buf.extend_from_slice(feather_bytes);

    Ok(buf)
}

// =========================================================================
// ByteBaseModel response parsing
// =========================================================================

fn parse_bulk_query_response(data: &[u8]) -> Result<BulkQueryResult> {
    let mut pos: usize = 0;

    pos = consume_magic(data, pos)?;

    let (new_pos, _attrs_json) = read_length_prefixed_json(data, pos)?;
    pos = new_pos;

    let (new_pos, _pydantic_json) = read_length_prefixed_json(data, pos)?;
    pos = new_pos;

    let (new_pos, byte_offset_map) = read_length_prefixed_json(data, pos)?;
    pos = new_pos;
    pos = skip_byte_data(data, pos, &byte_offset_map)?;

    let (new_pos, serializable_offset_map) = read_length_prefixed_json(data, pos)?;
    pos = new_pos;

    let query_results_len = serializable_offset_map
        .get("query_results_bytes")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| ChalkClientError::Api {
            status: 0,
            message: format!(
                "missing query_results_bytes in serializable_attrs (got: {})",
                serializable_offset_map
            ),
        })? as usize;

    if pos + query_results_len > data.len() {
        return Err(ChalkClientError::Api {
            status: 0,
            message: "response truncated: query_results_bytes extends beyond data".into(),
        });
    }
    let query_results_bytes = &data[pos..pos + query_results_len];

    parse_query_result_feather(query_results_bytes)
}

fn parse_query_result_feather(data: &[u8]) -> Result<BulkQueryResult> {
    let mut pos: usize = 0;

    pos = consume_magic(data, pos)?;

    let (new_pos, _) = read_length_prefixed_json(data, pos)?;
    pos = new_pos;

    let (new_pos, _) = read_length_prefixed_json(data, pos)?;
    pos = new_pos;

    let (new_pos, byte_offset_map) = read_length_prefixed_json(data, pos)?;
    pos = new_pos;

    let (_query_key, result_len) = byte_offset_map
        .as_object()
        .and_then(|m| m.iter().next())
        .and_then(|(k, v)| v.as_u64().map(|len| (k.clone(), len as usize)))
        .ok_or_else(|| ChalkClientError::Api {
            status: 0,
            message: "empty byte_attrs in query results ByteDict".into(),
        })?;

    if pos + result_len > data.len() {
        return Err(ChalkClientError::Api {
            status: 0,
            message: "response truncated: result bytes extend beyond data".into(),
        });
    }
    let result_bytes = &data[pos..pos + result_len];

    parse_online_query_result_feather(result_bytes)
}

fn parse_online_query_result_feather(data: &[u8]) -> Result<BulkQueryResult> {
    let mut pos: usize = 0;

    pos = consume_magic(data, pos)?;

    let (new_pos, json_attrs) = read_length_prefixed_json(data, pos)?;
    pos = new_pos;

    let has_data = json_attrs
        .get("has_data")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let meta = json_attrs
        .get("meta")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let errors: Vec<String> = json_attrs
        .get("errors")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    let (new_pos, _) = read_length_prefixed_json(data, pos)?;
    pos = new_pos;

    let (new_pos, byte_offset_map) = read_length_prefixed_json(data, pos)?;
    pos = new_pos;

    let scalar_data_len = byte_offset_map
        .get("scalar_data")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;

    let scalar_data = if scalar_data_len > 0 && pos + scalar_data_len <= data.len() {
        data[pos..pos + scalar_data_len].to_vec()
    } else {
        vec![]
    };

    Ok(BulkQueryResult {
        scalar_data,
        has_data,
        meta,
        errors,
    })
}

fn consume_magic(data: &[u8], pos: usize) -> Result<usize> {
    if pos + BYTEMODEL_MAGIC_STR.len() > data.len() {
        return Err(ChalkClientError::Api {
            status: 0,
            message: format!(
                "response too short for magic string at position {} ({} bytes available)",
                pos,
                data.len() - pos
            ),
        });
    }
    if &data[pos..pos + BYTEMODEL_MAGIC_STR.len()] != BYTEMODEL_MAGIC_STR {
        return Err(ChalkClientError::Api {
            status: 0,
            message: format!(
                "invalid ByteBaseModel magic at position {} (got {:?})",
                pos,
                &data[pos..std::cmp::min(pos + BYTEMODEL_MAGIC_STR.len(), data.len())]
            ),
        });
    }
    Ok(pos + BYTEMODEL_MAGIC_STR.len())
}

fn skip_byte_data(data: &[u8], pos: usize, offset_map: &Value) -> Result<usize> {
    let total_bytes: usize = offset_map
        .as_object()
        .map(|m| {
            m.values()
                .filter_map(|v| v.as_u64())
                .map(|v| v as usize)
                .sum()
        })
        .unwrap_or(0);

    if pos + total_bytes > data.len() {
        return Err(ChalkClientError::Api {
            status: 0,
            message: format!(
                "response truncated: byte data of {} bytes at position {} extends beyond data (total {})",
                total_bytes, pos, data.len()
            ),
        });
    }

    Ok(pos + total_bytes)
}

fn read_length_prefixed_json(data: &[u8], pos: usize) -> Result<(usize, Value)> {
    if pos + 8 > data.len() {
        return Err(ChalkClientError::Api {
            status: 0,
            message: format!(
                "response truncated: expected 8-byte length at position {}, but only {} bytes remain",
                pos,
                data.len() - pos
            ),
        });
    }

    let len = u64::from_be_bytes(data[pos..pos + 8].try_into().unwrap()) as usize;
    let json_start = pos + 8;

    if json_start + len > data.len() {
        return Err(ChalkClientError::Api {
            status: 0,
            message: format!(
                "response truncated: JSON payload of {} bytes at position {} extends beyond data (total {})",
                len, json_start, data.len()
            ),
        });
    }

    let json_str = std::str::from_utf8(&data[json_start..json_start + len]).map_err(|e| {
        ChalkClientError::Api {
            status: 0,
            message: format!("invalid UTF-8 in response JSON: {}", e),
        }
    })?;

    let value: Value = serde_json::from_str(json_str)?;
    Ok((json_start + len, value))
}

// =========================================================================
// ByteBaseModel serialization (request direction)
// =========================================================================

fn build_byte_base_model(
    json_attrs: &Value,
    byte_attrs: &[(&str, &[u8])],
) -> Result<Vec<u8>> {
    let json_attrs_bytes = serde_json::to_vec(json_attrs)?;
    let empty_json = b"{}";

    let byte_offset_map = {
        let mut map = serde_json::Map::new();
        for (key, data) in byte_attrs {
            map.insert((*key).to_string(), Value::Number((data.len() as u64).into()));
        }
        serde_json::to_vec(&Value::Object(map))?
    };

    let total_byte_data: usize = byte_attrs.iter().map(|(_, d)| d.len()).sum();

    let total_size = BYTEMODEL_MAGIC_STR.len()
        + 4 * 8
        + json_attrs_bytes.len()
        + empty_json.len()
        + byte_offset_map.len()
        + total_byte_data
        + empty_json.len();
    let mut buf = Vec::with_capacity(total_size);

    buf.extend_from_slice(BYTEMODEL_MAGIC_STR);

    buf.extend_from_slice(&(json_attrs_bytes.len() as u64).to_be_bytes());
    buf.extend_from_slice(&json_attrs_bytes);

    buf.extend_from_slice(&(empty_json.len() as u64).to_be_bytes());
    buf.extend_from_slice(empty_json);

    buf.extend_from_slice(&(byte_offset_map.len() as u64).to_be_bytes());
    buf.extend_from_slice(&byte_offset_map);
    for (_, data) in byte_attrs {
        buf.extend_from_slice(data);
    }

    buf.extend_from_slice(&(empty_json.len() as u64).to_be_bytes());
    buf.extend_from_slice(empty_json);

    Ok(buf)
}

// =========================================================================
// Arrow serialization helpers
// =========================================================================

fn serialize_record_batch_to_feather(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buf = Vec::new();

    {
        let mut writer = FileWriter::try_new(&mut buf, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }

    Ok(buf)
}

// =========================================================================
// Unit tests
// =========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_serialize_record_batch_to_feather() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "user.id",
            DataType::Int32,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();

        let feather_bytes = serialize_record_batch_to_feather(&batch).unwrap();
        assert!(!feather_bytes.is_empty());
        assert_eq!(&feather_bytes[..6], b"ARROW1");
    }

    #[test]
    fn test_build_feather_request_body() {
        let header = FeatherRequestHeader {
            outputs: vec!["user.id".into()],
            expression_outputs: vec![],
            now: None,
            staleness: None,
            context: None,
            include_meta: true,
            explain: false,
            correlation_id: None,
            query_name: None,
            query_name_version: None,
            deployment_id: None,
            branch_id: None,
            meta: None,
            store_plan_stages: Some(false),
            query_context: None,
            encoding_options: FeatureEncodingOptions {
                encode_structs_as_objects: None,
            },
            planner_options: None,
            value_metrics_tag_by_features: vec![],
            overlay_graph: None,
        };

        let fake_feather = b"ARROW1fake_feather_data";
        let body = build_feather_request_body(&header, fake_feather).unwrap();

        assert_eq!(&body[..5], b"chal1");

        let header_len = u64::from_be_bytes(body[5..13].try_into().unwrap()) as usize;
        assert!(header_len > 0);

        let header_json_str = std::str::from_utf8(&body[13..13 + header_len]).unwrap();
        let parsed: Value = serde_json::from_str(header_json_str).unwrap();
        assert_eq!(parsed["outputs"][0], "user.id");
        assert_eq!(parsed["include_meta"], true);

        let body_len_start = 13 + header_len;
        let body_len =
            u64::from_be_bytes(body[body_len_start..body_len_start + 8].try_into().unwrap())
                as usize;
        assert_eq!(body_len, fake_feather.len());

        let body_start = body_len_start + 8;
        assert_eq!(&body[body_start..body_start + body_len], fake_feather);
    }

    #[tokio::test]
    async fn test_client_builder() {
        let mut server = mockito::Server::new_async().await;

        let mock = server
            .mock("POST", "/v1/oauth/token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "access_token": "test-jwt",
                    "expires_in": 3600,
                    "primary_environment": "env-123",
                    "engines": {"env-123": server.url()},
                    "grpc_engines": {},
                    "environment_id_to_name": {"env-123": "production"}
                })
                .to_string(),
            )
            .create_async()
            .await;

        let client = ChalkClient::new()
            .client_id("test-id")
            .client_secret("test-secret")
            .api_server(&server.url())
            .environment("env-123")
            .build()
            .await
            .unwrap();

        assert_eq!(client.environment_id(), "env-123");
        assert_eq!(client.query_server(), &server.url());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_query() {
        let mut server = mockito::Server::new_async().await;

        server
            .mock("POST", "/v1/oauth/token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "access_token": "test-jwt",
                    "expires_in": 3600,
                    "primary_environment": "env-1",
                    "engines": {"env-1": server.url()},
                    "grpc_engines": {}
                })
                .to_string(),
            )
            .create_async()
            .await;

        let query_mock = server
            .mock("POST", "/v1/query/online")
            .match_header("Authorization", "Bearer test-jwt")
            .match_header("X-Chalk-Env-Id", "env-1")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "data": [
                        {"field": "user.age", "value": 25},
                        {"field": "user.name", "value": "Alice"}
                    ],
                    "errors": []
                })
                .to_string(),
            )
            .create_async()
            .await;

        let client = ChalkClient::new()
            .client_id("test-id")
            .client_secret("test-secret")
            .api_server(&server.url())
            .environment("env-1")
            .build()
            .await
            .unwrap();

        let inputs = HashMap::from([("user.id".into(), serde_json::json!(42))]);
        let outputs = vec!["user.age".into(), "user.name".into()];

        let response = client
            .query(inputs, outputs, QueryOptions::default())
            .await
            .unwrap();

        assert_eq!(response.data.len(), 2);
        assert_eq!(response.data[0].field, "user.age");
        assert_eq!(response.data[0].value, serde_json::json!(25));
        assert_eq!(response.data[1].field, "user.name");
        assert_eq!(response.data[1].value, serde_json::json!("Alice"));

        query_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_query_api_error() {
        let mut server = mockito::Server::new_async().await;

        server
            .mock("POST", "/v1/oauth/token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "access_token": "jwt",
                    "expires_in": 3600,
                    "primary_environment": "env-1",
                    "engines": {"env-1": server.url()},
                    "grpc_engines": {}
                })
                .to_string(),
            )
            .create_async()
            .await;

        server
            .mock("POST", "/v1/query/online")
            .with_status(500)
            .with_body("internal server error")
            .create_async()
            .await;

        let client = ChalkClient::new()
            .client_id("id")
            .client_secret("secret")
            .api_server(&server.url())
            .environment("env-1")
            .build()
            .await
            .unwrap();

        let result = client
            .query(HashMap::new(), vec![], QueryOptions::default())
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            ChalkClientError::Api { status, message } => {
                assert_eq!(status, 500);
                assert!(message.contains("internal server error"));
            }
            other => panic!("expected Api error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_offline_query() {
        let mut server = mockito::Server::new_async().await;

        server
            .mock("POST", "/v1/oauth/token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "access_token": "jwt",
                    "expires_in": 3600,
                    "primary_environment": "env-1",
                    "engines": {"env-1": server.url()},
                    "grpc_engines": {}
                })
                .to_string(),
            )
            .create_async()
            .await;

        let offline_mock = server
            .mock("POST", "/v4/offline_query")
            .match_header("Authorization", "Bearer jwt")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "is_finished": false,
                    "dataset_id": "ds-123",
                    "revisions": [{
                        "revision_id": "rev-1",
                        "status": "pending"
                    }],
                    "errors": []
                })
                .to_string(),
            )
            .create_async()
            .await;

        let client = ChalkClient::new()
            .client_id("id")
            .client_secret("secret")
            .api_server(&server.url())
            .environment("env-1")
            .build()
            .await
            .unwrap();

        let request = OfflineQueryRequest {
            input: None,
            output: vec!["user.ltv".into()],
            destination_format: Some("PARQUET".into()),
            job_id: None,
            max_samples: None,
            max_cache_age_secs: None,
            observed_at_lower_bound: None,
            observed_at_upper_bound: None,
            dataset_name: None,
            branch: None,
            recompute_features: None,
            tags: None,
            required_resolver_tags: None,
            correlation_id: None,
            store_online: None,
            store_offline: None,
            required_output: None,
            run_asynchronously: None,
            num_shards: None,
            num_workers: None,
            resources: None,
            completion_deadline: None,
            max_retries: None,
            store_plan_stages: None,
            explain: None,
            planner_options: None,
            query_context: None,
            use_multiple_computers: None,
            spine_sql_query: None,
            query_name: None,
            query_name_version: None,
        };

        let response = client.offline_query_raw(request).await.unwrap();
        assert!(!response.is_finished);
        assert_eq!(response.dataset_id.as_deref(), Some("ds-123"));
        assert_eq!(response.revisions.len(), 1);

        offline_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_offline_query_with_builder() {
        let mut server = mockito::Server::new_async().await;

        server
            .mock("POST", "/v1/oauth/token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "access_token": "jwt",
                    "expires_in": 3600,
                    "primary_environment": "env-1",
                    "engines": {"env-1": server.url()},
                    "grpc_engines": {}
                })
                .to_string(),
            )
            .create_async()
            .await;

        let offline_mock = server
            .mock("POST", "/v4/offline_query")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "is_finished": false,
                    "dataset_id": "ds-456",
                    "revisions": [{
                        "revision_id": "rev-2",
                        "status": "pending"
                    }],
                    "errors": []
                })
                .to_string(),
            )
            .create_async()
            .await;

        let client = ChalkClient::new()
            .client_id("id")
            .client_secret("secret")
            .api_server(&server.url())
            .environment("env-1")
            .build()
            .await
            .unwrap();

        use crate::offline::OfflineQueryParams;

        let response = client
            .offline_query(
                OfflineQueryParams::new()
                    .with_input("user.id", vec![serde_json::json!(1), serde_json::json!(2)])
                    .with_output("user.email")
                    .with_output("user.ltv")
                    .with_num_shards(4),
            )
            .await
            .unwrap();

        assert!(!response.is_finished);
        assert_eq!(response.dataset_id.as_deref(), Some("ds-456"));
        offline_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_wait_for_offline_query_success() {
        let mut server = mockito::Server::new_async().await;

        server
            .mock("POST", "/v1/oauth/token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "access_token": "jwt",
                    "expires_in": 3600,
                    "primary_environment": "env-1",
                    "engines": {"env-1": server.url()},
                    "grpc_engines": {}
                })
                .to_string(),
            )
            .create_async()
            .await;

        server
            .mock("GET", "/v4/offline_query/rev-1/status")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "report": {
                        "status": "RUNNING"
                    }
                })
                .to_string(),
            )
            .create_async()
            .await;

        server
            .mock("GET", "/v4/offline_query/rev-1/status")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "report": {
                        "status": "COMPLETED"
                    }
                })
                .to_string(),
            )
            .create_async()
            .await;

        let client = ChalkClient::new()
            .client_id("id")
            .client_secret("secret")
            .api_server(&server.url())
            .environment("env-1")
            .build()
            .await
            .unwrap();

        let response = OfflineQueryResponse {
            is_finished: false,
            version: None,
            dataset_id: Some("ds-123".into()),
            dataset_name: None,
            environment_id: None,
            revisions: vec![crate::types::DatasetRevision {
                revision_id: Some("rev-1".into()),
                creator_id: None,
                environment_id: None,
                outputs: vec![],
                status: Some("pending".into()),
                num_partitions: None,
                output_uris: None,
                created_at: None,
                started_at: None,
                terminated_at: None,
                dashboard_url: None,
                dataset_name: None,
                dataset_id: None,
                branch: None,
            }],
            errors: vec![],
        };

        let result = client
            .wait_for_offline_query(&response, Some(Duration::from_secs(5)))
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_wait_for_offline_query_failure() {
        let mut server = mockito::Server::new_async().await;

        server
            .mock("POST", "/v1/oauth/token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "access_token": "jwt",
                    "expires_in": 3600,
                    "primary_environment": "env-1",
                    "engines": {"env-1": server.url()},
                    "grpc_engines": {}
                })
                .to_string(),
            )
            .create_async()
            .await;

        server
            .mock("GET", "/v4/offline_query/rev-1/status")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "report": {
                        "status": "FAILED",
                        "all_errors": [{
                            "code": "INTERNAL_ERROR",
                            "category": "REQUEST",
                            "message": "job failed due to OOM"
                        }]
                    }
                })
                .to_string(),
            )
            .create_async()
            .await;

        let client = ChalkClient::new()
            .client_id("id")
            .client_secret("secret")
            .api_server(&server.url())
            .environment("env-1")
            .build()
            .await
            .unwrap();

        let response = OfflineQueryResponse {
            is_finished: false,
            version: None,
            dataset_id: None,
            dataset_name: None,
            environment_id: None,
            revisions: vec![crate::types::DatasetRevision {
                revision_id: Some("rev-1".into()),
                creator_id: None,
                environment_id: None,
                outputs: vec![],
                status: None,
                num_partitions: None,
                output_uris: None,
                created_at: None,
                started_at: None,
                terminated_at: None,
                dashboard_url: None,
                dataset_name: None,
                dataset_id: None,
                branch: None,
            }],
            errors: vec![],
        };

        let result = client
            .wait_for_offline_query(&response, Some(Duration::from_secs(5)))
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("OOM"));
    }

    #[tokio::test]
    async fn test_get_offline_query_download_urls() {
        let mut server = mockito::Server::new_async().await;

        server
            .mock("POST", "/v1/oauth/token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "access_token": "jwt",
                    "expires_in": 3600,
                    "primary_environment": "env-1",
                    "engines": {"env-1": server.url()},
                    "grpc_engines": {}
                })
                .to_string(),
            )
            .create_async()
            .await;

        server
            .mock("GET", "/v2/offline_query/rev-1")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "is_finished": false,
                    "urls": [],
                    "errors": []
                })
                .to_string(),
            )
            .create_async()
            .await;

        server
            .mock("GET", "/v2/offline_query/rev-1")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "is_finished": true,
                    "urls": [
                        "https://storage.example.com/results/part-0.parquet",
                        "https://storage.example.com/results/part-1.parquet"
                    ],
                    "errors": []
                })
                .to_string(),
            )
            .create_async()
            .await;

        let client = ChalkClient::new()
            .client_id("id")
            .client_secret("secret")
            .api_server(&server.url())
            .environment("env-1")
            .build()
            .await
            .unwrap();

        let response = OfflineQueryResponse {
            is_finished: false,
            version: None,
            dataset_id: None,
            dataset_name: None,
            environment_id: None,
            revisions: vec![crate::types::DatasetRevision {
                revision_id: Some("rev-1".into()),
                creator_id: None,
                environment_id: None,
                outputs: vec![],
                status: None,
                num_partitions: None,
                output_uris: None,
                created_at: None,
                started_at: None,
                terminated_at: None,
                dashboard_url: None,
                dataset_name: None,
                dataset_id: None,
                branch: None,
            }],
            errors: vec![],
        };

        let urls = client
            .get_offline_query_download_urls(&response, Some(Duration::from_secs(5)))
            .await
            .unwrap();

        assert_eq!(urls.len(), 2);
        assert!(urls[0].contains("part-0.parquet"));
        assert!(urls[1].contains("part-1.parquet"));
    }

    #[tokio::test]
    async fn test_wait_for_offline_query_timeout() {
        let mut server = mockito::Server::new_async().await;

        server
            .mock("POST", "/v1/oauth/token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "access_token": "jwt",
                    "expires_in": 3600,
                    "primary_environment": "env-1",
                    "engines": {"env-1": server.url()},
                    "grpc_engines": {}
                })
                .to_string(),
            )
            .create_async()
            .await;

        server
            .mock("GET", "/v4/offline_query/rev-1/status")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "report": {
                        "status": "RUNNING"
                    }
                })
                .to_string(),
            )
            .expect_at_least(1)
            .create_async()
            .await;

        let client = ChalkClient::new()
            .client_id("id")
            .client_secret("secret")
            .api_server(&server.url())
            .environment("env-1")
            .build()
            .await
            .unwrap();

        let response = OfflineQueryResponse {
            is_finished: false,
            version: None,
            dataset_id: None,
            dataset_name: None,
            environment_id: None,
            revisions: vec![crate::types::DatasetRevision {
                revision_id: Some("rev-1".into()),
                creator_id: None,
                environment_id: None,
                outputs: vec![],
                status: None,
                num_partitions: None,
                output_uris: None,
                created_at: None,
                started_at: None,
                terminated_at: None,
                dashboard_url: None,
                dataset_name: None,
                dataset_id: None,
                branch: None,
            }],
            errors: vec![],
        };

        let result = client
            .wait_for_offline_query(&response, Some(Duration::from_millis(500)))
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("timed out"));
    }

    #[test]
    fn test_build_byte_base_model() {
        let json_attrs = serde_json::json!({
            "features": ["user.id", "user.age"],
            "table_compression": "uncompressed",
        });
        let fake_arrow = b"ARROW1fake_data_here";

        let body = build_byte_base_model(&json_attrs, &[("table_bytes", fake_arrow.as_slice())])
            .unwrap();

        let mut pos = 0;

        assert_eq!(
            &body[pos..pos + BYTEMODEL_MAGIC_STR.len()],
            BYTEMODEL_MAGIC_STR
        );
        pos += BYTEMODEL_MAGIC_STR.len();

        let json_attrs_len =
            u64::from_be_bytes(body[pos..pos + 8].try_into().unwrap()) as usize;
        pos += 8;
        let json_attrs_parsed: Value =
            serde_json::from_slice(&body[pos..pos + json_attrs_len]).unwrap();
        assert_eq!(json_attrs_parsed["features"][0], "user.id");
        assert_eq!(json_attrs_parsed["table_compression"], "uncompressed");
        pos += json_attrs_len;

        let pydantic_len =
            u64::from_be_bytes(body[pos..pos + 8].try_into().unwrap()) as usize;
        pos += 8;
        let pydantic: Value =
            serde_json::from_slice(&body[pos..pos + pydantic_len]).unwrap();
        assert_eq!(pydantic, serde_json::json!({}));
        pos += pydantic_len;

        let byte_map_len =
            u64::from_be_bytes(body[pos..pos + 8].try_into().unwrap()) as usize;
        pos += 8;
        let byte_map: Value =
            serde_json::from_slice(&body[pos..pos + byte_map_len]).unwrap();
        assert_eq!(byte_map["table_bytes"], fake_arrow.len() as u64);
        pos += byte_map_len;

        assert_eq!(&body[pos..pos + fake_arrow.len()], fake_arrow);
        pos += fake_arrow.len();

        let ser_len =
            u64::from_be_bytes(body[pos..pos + 8].try_into().unwrap()) as usize;
        pos += 8;
        let ser: Value = serde_json::from_slice(&body[pos..pos + ser_len]).unwrap();
        assert_eq!(ser, serde_json::json!({}));
        pos += ser_len;

        assert_eq!(pos, body.len());
    }

    #[tokio::test]
    async fn test_upload_features() {
        let mut server = mockito::Server::new_async().await;

        server
            .mock("POST", "/v1/oauth/token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "access_token": "jwt",
                    "expires_in": 3600,
                    "primary_environment": "env-1",
                    "engines": {"env-1": server.url()},
                    "grpc_engines": {}
                })
                .to_string(),
            )
            .create_async()
            .await;

        let upload_mock = server
            .mock("POST", "/v1/upload_features/multi")
            .match_header("Authorization", "Bearer jwt")
            .match_header("Content-Type", "application/octet-stream")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "operation_id": "op-abc-123",
                    "errors": []
                })
                .to_string(),
            )
            .create_async()
            .await;

        let client = ChalkClient::new()
            .client_id("id")
            .client_secret("secret")
            .api_server(&server.url())
            .environment("env-1")
            .build()
            .await
            .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("user.id", DataType::Int32, false),
            Field::new("user.age", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![25, 30, 22])),
            ],
        )
        .unwrap();

        let result = client.upload_features(&batch).await.unwrap();
        assert_eq!(result.operation_id.as_deref(), Some("op-abc-123"));
        assert!(result.errors.is_empty());

        upload_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_upload_features_map() {
        let mut server = mockito::Server::new_async().await;

        server
            .mock("POST", "/v1/oauth/token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "access_token": "jwt",
                    "expires_in": 3600,
                    "primary_environment": "env-1",
                    "engines": {"env-1": server.url()},
                    "grpc_engines": {}
                })
                .to_string(),
            )
            .create_async()
            .await;

        let upload_mock = server
            .mock("POST", "/v1/upload_features/multi")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "operation_id": "op-map-456",
                    "errors": []
                })
                .to_string(),
            )
            .create_async()
            .await;

        let client = ChalkClient::new()
            .client_id("id")
            .client_secret("secret")
            .api_server(&server.url())
            .environment("env-1")
            .build()
            .await
            .unwrap();

        let inputs = HashMap::from([
            (
                "user.id".to_string(),
                vec![serde_json::json!(1), serde_json::json!(2)],
            ),
            (
                "user.name".to_string(),
                vec![serde_json::json!("Alice"), serde_json::json!("Bob")],
            ),
        ]);

        let result = client.upload_features_map(inputs).await.unwrap();
        assert_eq!(result.operation_id.as_deref(), Some("op-map-456"));

        upload_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_upload_features_map_empty_inputs() {
        let mut server = mockito::Server::new_async().await;

        server
            .mock("POST", "/v1/oauth/token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "access_token": "jwt",
                    "expires_in": 3600,
                    "primary_environment": "env-1",
                    "engines": {"env-1": server.url()},
                    "grpc_engines": {}
                })
                .to_string(),
            )
            .create_async()
            .await;

        let client = ChalkClient::new()
            .client_id("id")
            .client_secret("secret")
            .api_server(&server.url())
            .environment("env-1")
            .build()
            .await
            .unwrap();

        let result = client.upload_features_map(HashMap::new()).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("at least one feature"));
    }
}
