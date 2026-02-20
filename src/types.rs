//! JSON request and response types for the Chalk REST API.
//!
//! These structs mirror the shapes the Chalk HTTP API expects and returns.
//! We use `serde` to automatically convert between Rust structs and JSON.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

/// Deserialize a Vec field that may be `null` in the JSON (treat null as empty vec).
fn deserialize_null_as_empty_vec<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    let opt = Option::<Vec<T>>::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

/// Deserialize a status field that may arrive as a string or an integer.
fn deserialize_status_flexible<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let v = Option::<Value>::deserialize(deserializer)?;
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s)),
        Some(Value::Number(n)) => {
            // Maps the QueryStatus IntEnum used by the HTTP/JSON API.
            let label = match n.as_i64() {
                Some(1) => "pending_submission",
                Some(2) => "submitted",
                Some(3) => "running",
                Some(4) => "error",
                Some(5) => "expired",
                Some(6) => "cancelled",
                Some(7) => "successful",
                _ => "unknown",
            };
            Ok(Some(label.to_string()))
        }
        Some(other) => Ok(Some(other.to_string())),
    }
}

// =========================================================================
// Online Query — Request types
// =========================================================================

/// The body we POST to `/v1/query/online`.
#[derive(Debug, Clone, Serialize)]
pub struct OnlineQueryRequest {
    /// Feature inputs — the "known" values you're providing.
    pub inputs: HashMap<String, Value>,

    /// Which features you want back, e.g. `["user.age", "user.fico_score"]`.
    pub outputs: Vec<String>,

    /// Contextual metadata (tags, required resolver tags).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<OnlineQueryContext>,

    /// Per-feature staleness tolerances.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub staleness: Option<HashMap<String, String>>,

    /// Whether to include metadata (resolver FQN, cache hit, etc.) in results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_meta: Option<bool>,

    /// A named query registered in the Chalk dashboard.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_name: Option<String>,

    /// A caller-provided correlation ID for tracing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,

    /// Arbitrary key-value context passed through to resolvers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_context: Option<HashMap<String, Value>>,

    /// Arbitrary metadata tags attached to the query.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<HashMap<String, String>>,

    /// Version of the named query to use.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_name_version: Option<String>,

    /// Override the "current time" for the query (RFC 3339 string).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub now: Option<String>,

    /// Whether to return a query execution plan (for debugging).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explain: Option<bool>,

    /// Whether to store intermediate plan stages.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store_plan_stages: Option<bool>,

    /// Controls how structured types (like dataclass features) are encoded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding_options: Option<FeatureEncodingOptions>,

    /// Branch ID to target (for branch deployments).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch_id: Option<String>,
}

/// Tags and resolver constraints for a query.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OnlineQueryContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub required_resolver_tags: Option<Vec<String>>,
}

/// Controls how structured feature types are encoded in the response.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FeatureEncodingOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encode_structs_as_objects: Option<bool>,
}

// =========================================================================
// Online Query — Response types
// =========================================================================

/// The response from `/v1/query/online`.
#[derive(Debug, Clone, Deserialize)]
pub struct OnlineQueryResponse {
    pub data: Vec<FeatureResult>,

    #[serde(default, deserialize_with = "deserialize_null_as_empty_vec")]
    pub errors: Vec<ChalkError>,

    #[serde(default)]
    pub meta: Option<QueryMeta>,
}

/// A single feature value in the query response.
#[derive(Debug, Clone, Deserialize)]
pub struct FeatureResult {
    pub field: String,
    pub value: Value,

    #[serde(default)]
    pub pkey: Option<Value>,

    #[serde(default)]
    pub ts: Option<String>,

    #[serde(default)]
    pub meta: Option<FeatureMeta>,

    #[serde(default)]
    pub error: Option<ChalkError>,
}

/// Metadata about how a single feature was resolved.
#[derive(Debug, Clone, Deserialize)]
pub struct FeatureMeta {
    #[serde(default)]
    pub chosen_resolver_fqn: Option<String>,

    #[serde(default)]
    pub cache_hit: Option<bool>,

    #[serde(default)]
    pub primitive_type: Option<String>,

    #[serde(default)]
    pub version: Option<i64>,
}

/// Metadata about the overall query execution.
#[derive(Debug, Clone, Deserialize)]
pub struct QueryMeta {
    #[serde(default)]
    pub execution_duration_s: Option<f64>,

    #[serde(default)]
    pub deployment_id: Option<String>,

    #[serde(default)]
    pub environment_id: Option<String>,

    #[serde(default)]
    pub environment_name: Option<String>,

    #[serde(default)]
    pub query_id: Option<String>,

    #[serde(default)]
    pub query_timestamp: Option<DateTime<Utc>>,

    #[serde(default)]
    pub query_hash: Option<String>,
}

// =========================================================================
// Offline Query — Request types
// =========================================================================

/// The body we POST to `/v4/offline_query`.
#[derive(Debug, Clone, Serialize)]
pub struct OfflineQueryRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<OfflineQueryInputType>,

    pub output: Vec<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination_format: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_samples: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_cache_age_secs: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_at_lower_bound: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_at_upper_bound: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset_name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub recompute_features: Option<Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub required_resolver_tags: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub store_online: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub store_offline: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub required_output: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_asynchronously: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_shards: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_workers: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequests>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_deadline: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub store_plan_stages: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub explain: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub planner_options: Option<HashMap<String, Value>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_context: Option<HashMap<String, Value>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_multiple_computers: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub spine_sql_query: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_name_version: Option<String>,
}

/// Inline input data for an offline query — a columnar table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OfflineQueryInput {
    pub columns: Vec<String>,
    pub values: Vec<Vec<Value>>,
}

/// Input for an offline query — inline data, a Parquet URI, or a SQL query.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OfflineQueryInputType {
    Inline(OfflineQueryInput),
    Uri(OfflineQueryInputUri),
    Sql(OfflineQueryInputSql),
}

/// Point to an existing Parquet file on S3/GCS.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OfflineQueryInputUri {
    pub parquet_uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_row: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_row: Option<i64>,
}

/// Use a SQL query to generate input data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OfflineQueryInputSql {
    pub input_sql: String,
}

/// Resource requests for an offline query job.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceRequests {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ephemeral_storage: Option<String>,
}

// =========================================================================
// Offline Query — Response types
// =========================================================================

/// The response from `/v4/offline_query`.
#[derive(Debug, Clone, Deserialize)]
pub struct OfflineQueryResponse {
    #[serde(default)]
    pub is_finished: bool,

    #[serde(default)]
    pub version: Option<i64>,

    #[serde(default)]
    pub dataset_id: Option<String>,

    #[serde(default)]
    pub dataset_name: Option<String>,

    #[serde(default)]
    pub environment_id: Option<String>,

    #[serde(default, deserialize_with = "deserialize_null_as_empty_vec")]
    pub revisions: Vec<DatasetRevision>,

    #[serde(default, deserialize_with = "deserialize_null_as_empty_vec")]
    pub errors: Vec<ChalkError>,
}

/// A single revision (run) of an offline query dataset.
#[derive(Debug, Clone, Deserialize)]
pub struct DatasetRevision {
    #[serde(default)]
    pub revision_id: Option<String>,

    #[serde(default)]
    pub creator_id: Option<String>,

    #[serde(default)]
    pub environment_id: Option<String>,

    #[serde(default)]
    pub outputs: Vec<String>,

    #[serde(default, deserialize_with = "deserialize_status_flexible")]
    pub status: Option<String>,

    #[serde(default)]
    pub num_partitions: Option<i64>,

    #[serde(default)]
    pub output_uris: Option<String>,

    #[serde(default)]
    pub created_at: Option<DateTime<Utc>>,

    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,

    #[serde(default)]
    pub terminated_at: Option<DateTime<Utc>>,

    #[serde(default)]
    pub dashboard_url: Option<String>,

    #[serde(default)]
    pub dataset_name: Option<String>,

    #[serde(default)]
    pub dataset_id: Option<String>,

    #[serde(default)]
    pub branch: Option<String>,
}

// =========================================================================
// Offline Query — Polling response types
// =========================================================================

/// Response from `GET /v4/offline_query/{job_id}/status`.
#[derive(Debug, Clone, Deserialize)]
pub struct GetOfflineQueryStatusResponse {
    pub report: Option<BatchReport>,
}

/// Status report for an offline query batch job.
#[derive(Debug, Clone, Deserialize)]
pub struct BatchReport {
    #[serde(default)]
    pub operation_id: Option<String>,

    #[serde(default)]
    pub status: Option<String>,

    #[serde(default)]
    pub environment_id: Option<String>,

    #[serde(default)]
    pub error: Option<ChalkError>,

    #[serde(default, deserialize_with = "deserialize_null_as_empty_vec")]
    pub all_errors: Vec<ChalkError>,
}

/// Response from `GET /v2/offline_query/{revision_id}`.
#[derive(Debug, Clone, Deserialize)]
pub struct GetOfflineQueryJobResponse {
    pub is_finished: bool,

    #[serde(default)]
    pub version: Option<i64>,

    #[serde(default, deserialize_with = "deserialize_null_as_empty_vec")]
    pub urls: Vec<String>,

    #[serde(default, deserialize_with = "deserialize_null_as_empty_vec")]
    pub errors: Vec<ChalkError>,
}

// =========================================================================
// Upload Features — Response types
// =========================================================================

/// The response from `POST /v1/upload_features/multi`.
#[derive(Debug, Clone, Deserialize)]
pub struct UploadFeaturesResult {
    #[serde(default)]
    pub operation_id: Option<String>,

    #[serde(default, deserialize_with = "deserialize_null_as_empty_vec")]
    pub errors: Vec<ChalkError>,
}

// =========================================================================
// Shared types
// =========================================================================

/// A structured error returned by the Chalk server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChalkError {
    pub code: String,
    pub category: String,
    pub message: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub feature: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolver: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub exception: Option<ResolverException>,
}

/// Details about a Python exception that occurred inside a resolver.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolverException {
    #[serde(default)]
    pub kind: Option<String>,

    #[serde(default)]
    pub message: Option<String>,

    #[serde(default)]
    pub stacktrace: Option<String>,
}

/// Options that control query behavior (used by both `query` and `query_bulk`).
#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    pub context: Option<OnlineQueryContext>,
    pub staleness: Option<HashMap<String, String>>,
    pub include_meta: Option<bool>,
    pub query_name: Option<String>,
    pub query_name_version: Option<String>,
    pub correlation_id: Option<String>,
    pub query_context: Option<HashMap<String, Value>>,
    pub meta: Option<HashMap<String, String>>,
    pub now: Option<String>,
    pub explain: Option<bool>,
    pub store_plan_stages: Option<bool>,
    pub planner_options: Option<HashMap<String, Value>>,
    pub branch_id: Option<String>,
    pub encoding_options: Option<FeatureEncodingOptions>,
}

// =========================================================================
// Auth types
// =========================================================================

/// The request body for the token exchange endpoint (`/v1/oauth/token`).
#[derive(Debug, Serialize)]
pub struct TokenExchangeRequest {
    pub client_id: String,
    pub client_secret: String,
    pub grant_type: String,
}

/// The response from the token exchange endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,

    #[serde(default)]
    pub expires_at: Option<String>,

    #[serde(default)]
    pub expires_in: Option<i64>,

    #[serde(default)]
    pub primary_environment: Option<String>,

    #[serde(default)]
    pub engines: HashMap<String, String>,

    #[serde(default)]
    pub grpc_engines: HashMap<String, String>,

    #[serde(default)]
    pub environment_id_to_name: HashMap<String, String>,

    #[serde(default)]
    pub api_server: Option<String>,
}

// =========================================================================
// Unit tests
// =========================================================================
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_online_query_request_serialization() {
        let req = OnlineQueryRequest {
            inputs: HashMap::from([("user.id".into(), serde_json::json!(1))]),
            outputs: vec!["user.age".into(), "user.name".into()],
            context: None,
            staleness: None,
            include_meta: Some(true),
            query_name: None,
            correlation_id: None,
            query_context: None,
            meta: None,
            query_name_version: None,
            now: None,
            explain: None,
            store_plan_stages: None,
            encoding_options: None,
            branch_id: None,
        };

        let json = serde_json::to_value(&req).unwrap();

        assert_eq!(json["inputs"]["user.id"], 1);
        assert_eq!(json["outputs"][0], "user.age");
        assert_eq!(json["include_meta"], true);
        assert!(json.get("context").is_none());
        assert!(json.get("staleness").is_none());
        assert!(json.get("query_name").is_none());
    }

    #[test]
    fn test_online_query_response_deserialization() {
        let json = r#"{
            "data": [
                {
                    "field": "user.age",
                    "value": 25,
                    "ts": "2024-01-15T10:30:00Z"
                }
            ],
            "errors": [],
            "meta": {
                "execution_duration_s": 0.042,
                "query_id": "q-123"
            }
        }"#;

        let resp: OnlineQueryResponse = serde_json::from_str(json).unwrap();

        assert_eq!(resp.data.len(), 1);
        assert_eq!(resp.data[0].field, "user.age");
        assert_eq!(resp.data[0].value, serde_json::json!(25));
        assert_eq!(resp.data[0].ts.as_deref(), Some("2024-01-15T10:30:00Z"));
        assert!(resp.data[0].meta.is_none());
        assert!(resp.errors.is_empty());

        let meta = resp.meta.unwrap();
        assert_eq!(meta.execution_duration_s, Some(0.042));
        assert_eq!(meta.query_id.as_deref(), Some("q-123"));
    }

    #[test]
    fn test_chalk_error_round_trip() {
        let err = ChalkError {
            code: "RESOLVER_FAILED".into(),
            category: "FIELD".into(),
            message: "timeout after 30s".into(),
            feature: Some("user.credit_score".into()),
            resolver: Some("get_credit_score".into()),
            exception: Some(ResolverException {
                kind: Some("TimeoutError".into()),
                message: Some("deadline exceeded".into()),
                stacktrace: None,
            }),
        };

        let json = serde_json::to_string(&err).unwrap();
        let parsed: ChalkError = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.code, "RESOLVER_FAILED");
        assert_eq!(parsed.feature.as_deref(), Some("user.credit_score"));
        assert!(parsed.exception.is_some());
        assert_eq!(
            parsed.exception.unwrap().kind.as_deref(),
            Some("TimeoutError")
        );
    }

    #[test]
    fn test_token_response_deserialization() {
        let json = r#"{
            "access_token": "eyJhbGci...",
            "expires_in": 3600,
            "primary_environment": "env-123",
            "engines": {
                "env-123": "https://engine1.chalk.ai"
            },
            "grpc_engines": {
                "env-123": "https://grpc1.chalk.ai"
            }
        }"#;

        let resp: TokenResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.access_token, "eyJhbGci...");
        assert_eq!(resp.primary_environment.as_deref(), Some("env-123"));
        assert_eq!(
            resp.engines.get("env-123").map(|s| s.as_str()),
            Some("https://engine1.chalk.ai")
        );
    }

    #[test]
    fn test_offline_query_request_serialization() {
        let req = OfflineQueryRequest {
            input: Some(OfflineQueryInputType::Inline(OfflineQueryInput {
                columns: vec!["user.id".into(), "user.signup_date".into()],
                values: vec![
                    vec![serde_json::json!(1), serde_json::json!(2)],
                    vec![serde_json::json!("2024-01-01"), serde_json::json!("2024-02-01")],
                ],
            })),
            output: vec!["user.ltv".into()],
            destination_format: Some("PARQUET".into()),
            job_id: None,
            max_samples: None,
            max_cache_age_secs: None,
            observed_at_lower_bound: None,
            observed_at_upper_bound: None,
            dataset_name: Some("training_data_v2".into()),
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

        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["output"][0], "user.ltv");
        assert_eq!(json["input"]["columns"][0], "user.id");
        assert_eq!(json["dataset_name"], "training_data_v2");
        assert!(json.get("branch").is_none());
        assert!(json.get("use_multiple_computers").is_none());
    }

    #[test]
    fn test_offline_query_request_with_uri_input() {
        let req = OfflineQueryRequest {
            input: Some(OfflineQueryInputType::Uri(OfflineQueryInputUri {
                parquet_uri: "s3://bucket/inputs.parquet".into(),
                start_row: None,
                end_row: None,
            })),
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

        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["input"]["parquet_uri"], "s3://bucket/inputs.parquet");
        assert!(json["input"].get("columns").is_none());
    }
}
