//! Fluent builder for offline query parameters.
//!
//! [`OfflineQueryParams`] provides a chainable API for constructing offline
//! queries. It supports three input modes:
//!
//! - **Inline data**: `OfflineQueryParams::new()` with `.with_input()` calls
//! - **Parquet URI**: `OfflineQueryParams::from_uri("s3://...")`
//! - **SQL query**: `OfflineQueryParams::from_sql("SELECT ...")`

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::error::{ChalkClientError, Result};
use crate::types::{
    OfflineQueryInput, OfflineQueryInputSql, OfflineQueryInputType, OfflineQueryInputUri,
    OfflineQueryRequest, ResourceRequests,
};

/// The special column name Chalk uses for per-row observation timestamps.
const CHALK_TS_COLUMN: &str = "__chalk__.CHALK_TS";

/// Builder for offline query parameters.
///
/// # Examples
///
/// ```
/// use chalk_client::OfflineQueryParams;
/// use serde_json::json;
///
/// let params = OfflineQueryParams::new()
///     .with_input("user.id", vec![json!(1), json!(2), json!(3)])
///     .with_output("user.email")
///     .with_output("user.ltv")
///     .with_num_shards(4);
/// ```
#[derive(Debug, Clone)]
pub struct OfflineQueryParams {
    inputs: HashMap<String, Vec<Value>>,
    input_times: Vec<DateTime<Utc>>,
    input_type: Option<OfflineQueryInputType>,
    output: Vec<String>,
    required_output: Vec<String>,

    destination_format: Option<String>,
    job_id: Option<String>,
    max_samples: Option<i64>,
    max_cache_age_secs: Option<i64>,
    observed_at_lower_bound: Option<String>,
    observed_at_upper_bound: Option<String>,
    dataset_name: Option<String>,
    branch: Option<String>,
    recompute_features: Option<Value>,
    tags: Option<Vec<String>>,
    required_resolver_tags: Option<Vec<String>>,
    correlation_id: Option<String>,
    store_online: Option<bool>,
    store_offline: Option<bool>,
    run_asynchronously: Option<bool>,
    num_shards: Option<i64>,
    num_workers: Option<i64>,
    resources: Option<ResourceRequests>,
    completion_deadline: Option<String>,
    max_retries: Option<i64>,
    store_plan_stages: Option<bool>,
    explain: Option<bool>,
    planner_options: Option<HashMap<String, Value>>,
    query_context: Option<HashMap<String, Value>>,
    spine_sql_query: Option<String>,
    query_name: Option<String>,
    query_name_version: Option<String>,
}

impl OfflineQueryParams {
    /// Create a new builder for inline input data.
    pub fn new() -> Self {
        Self {
            inputs: HashMap::new(),
            input_times: Vec::new(),
            input_type: None,
            output: Vec::new(),
            required_output: Vec::new(),
            destination_format: None,
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
            spine_sql_query: None,
            query_name: None,
            query_name_version: None,
        }
    }

    /// Create a builder that reads input from a Parquet file at the given URI.
    pub fn from_uri(parquet_uri: impl Into<String>) -> Self {
        let mut params = Self::new();
        params.input_type = Some(OfflineQueryInputType::Uri(OfflineQueryInputUri {
            parquet_uri: parquet_uri.into(),
            start_row: None,
            end_row: None,
        }));
        params
    }

    /// Create a builder that reads input from a Parquet URI with row range.
    pub fn from_uri_with_range(
        parquet_uri: impl Into<String>,
        start_row: Option<i64>,
        end_row: Option<i64>,
    ) -> Self {
        let mut params = Self::new();
        params.input_type = Some(OfflineQueryInputType::Uri(OfflineQueryInputUri {
            parquet_uri: parquet_uri.into(),
            start_row,
            end_row,
        }));
        params
    }

    /// Create a builder that generates input data from a SQL query.
    pub fn from_sql(input_sql: impl Into<String>) -> Self {
        let mut params = Self::new();
        params.input_type = Some(OfflineQueryInputType::Sql(OfflineQueryInputSql {
            input_sql: input_sql.into(),
        }));
        params
    }

    /// Add an input column with values.
    pub fn with_input(mut self, feature: impl Into<String>, values: Vec<Value>) -> Self {
        self.inputs.insert(feature.into(), values);
        self
    }

    /// Set per-row observation timestamps.
    pub fn with_input_times(mut self, times: Vec<DateTime<Utc>>) -> Self {
        self.input_times = times;
        self
    }

    /// Add a feature to the output list.
    pub fn with_output(mut self, feature: impl Into<String>) -> Self {
        self.output.push(feature.into());
        self
    }

    /// Add a feature to the required output list.
    pub fn with_required_output(mut self, feature: impl Into<String>) -> Self {
        self.required_output.push(feature.into());
        self
    }

    pub fn with_destination_format(mut self, format: impl Into<String>) -> Self {
        self.destination_format = Some(format.into());
        self
    }

    pub fn with_job_id(mut self, id: impl Into<String>) -> Self {
        self.job_id = Some(id.into());
        self
    }

    pub fn with_max_samples(mut self, n: i64) -> Self {
        self.max_samples = Some(n);
        self
    }

    pub fn with_max_cache_age_secs(mut self, secs: i64) -> Self {
        self.max_cache_age_secs = Some(secs);
        self
    }

    pub fn with_observed_at_lower_bound(mut self, bound: impl Into<String>) -> Self {
        self.observed_at_lower_bound = Some(bound.into());
        self
    }

    pub fn with_observed_at_upper_bound(mut self, bound: impl Into<String>) -> Self {
        self.observed_at_upper_bound = Some(bound.into());
        self
    }

    pub fn with_dataset_name(mut self, name: impl Into<String>) -> Self {
        self.dataset_name = Some(name.into());
        self
    }

    pub fn with_branch(mut self, branch: impl Into<String>) -> Self {
        self.branch = Some(branch.into());
        self
    }

    pub fn with_recompute_features(mut self, recompute: Value) -> Self {
        self.recompute_features = Some(recompute);
        self
    }

    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = Some(tags);
        self
    }

    pub fn with_required_resolver_tags(mut self, tags: Vec<String>) -> Self {
        self.required_resolver_tags = Some(tags);
        self
    }

    pub fn with_correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    pub fn with_store_online(mut self, store: bool) -> Self {
        self.store_online = Some(store);
        self
    }

    pub fn with_store_offline(mut self, store: bool) -> Self {
        self.store_offline = Some(store);
        self
    }

    pub fn with_run_asynchronously(mut self, async_: bool) -> Self {
        self.run_asynchronously = Some(async_);
        self
    }

    pub fn with_num_shards(mut self, n: i64) -> Self {
        self.num_shards = Some(n);
        self
    }

    pub fn with_num_workers(mut self, n: i64) -> Self {
        self.num_workers = Some(n);
        self
    }

    pub fn with_resources(mut self, resources: ResourceRequests) -> Self {
        self.resources = Some(resources);
        self
    }

    pub fn with_completion_deadline(mut self, deadline: impl Into<String>) -> Self {
        self.completion_deadline = Some(deadline.into());
        self
    }

    pub fn with_max_retries(mut self, n: i64) -> Self {
        self.max_retries = Some(n);
        self
    }

    pub fn with_store_plan_stages(mut self, store: bool) -> Self {
        self.store_plan_stages = Some(store);
        self
    }

    pub fn with_explain(mut self, explain: bool) -> Self {
        self.explain = Some(explain);
        self
    }

    pub fn with_planner_options(mut self, options: HashMap<String, Value>) -> Self {
        self.planner_options = Some(options);
        self
    }

    pub fn with_query_context(mut self, context: HashMap<String, Value>) -> Self {
        self.query_context = Some(context);
        self
    }

    pub fn with_spine_sql_query(mut self, sql: impl Into<String>) -> Self {
        self.spine_sql_query = Some(sql.into());
        self
    }

    pub fn with_query_name(mut self, name: impl Into<String>) -> Self {
        self.query_name = Some(name.into());
        self
    }

    pub fn with_query_name_version(mut self, version: impl Into<String>) -> Self {
        self.query_name_version = Some(version.into());
        self
    }

    /// Build the [`OfflineQueryRequest`].
    pub fn build(self) -> Result<OfflineQueryRequest> {
        if self.output.is_empty() && self.required_output.is_empty() {
            return Err(ChalkClientError::Config(
                "offline query requires at least one output or required_output".into(),
            ));
        }

        let spine_sql_query = match &self.input_type {
            Some(OfflineQueryInputType::Sql(sql)) => Some(sql.input_sql.clone()),
            _ => self.spine_sql_query,
        };

        let input = if let Some(input_type) = self.input_type {
            match input_type {
                OfflineQueryInputType::Inline(_)
                | OfflineQueryInputType::Uri(_) => Some(input_type),
                OfflineQueryInputType::Sql(_) => None,
            }
        } else if self.inputs.is_empty() {
            None
        } else {
            let mut columns: Vec<String> = self.inputs.keys().cloned().collect();
            columns.sort();

            let mut values: Vec<Vec<Value>> = Vec::with_capacity(columns.len());
            for col in &columns {
                values.push(self.inputs[col].clone());
            }

            if !self.input_times.is_empty() {
                columns.push(CHALK_TS_COLUMN.to_string());
                let ts_values: Vec<Value> = self
                    .input_times
                    .iter()
                    .map(|ts| Value::String(ts.to_rfc3339()))
                    .collect();
                values.push(ts_values);
            }

            Some(OfflineQueryInputType::Inline(OfflineQueryInput { columns, values }))
        };

        let required_output = if self.required_output.is_empty() {
            None
        } else {
            Some(self.required_output)
        };

        let use_multiple_computers = if self.num_shards.is_some()
            || self.num_workers.is_some()
            || self.run_asynchronously == Some(true)
        {
            Some(true)
        } else {
            None
        };

        Ok(OfflineQueryRequest {
            input,
            output: self.output,
            destination_format: self.destination_format,
            job_id: self.job_id,
            max_samples: self.max_samples,
            max_cache_age_secs: self.max_cache_age_secs,
            observed_at_lower_bound: self.observed_at_lower_bound,
            observed_at_upper_bound: self.observed_at_upper_bound,
            dataset_name: self.dataset_name,
            branch: self.branch,
            recompute_features: self.recompute_features,
            tags: self.tags,
            required_resolver_tags: self.required_resolver_tags,
            correlation_id: self.correlation_id,
            store_online: self.store_online,
            store_offline: self.store_offline,
            required_output,
            run_asynchronously: self.run_asynchronously,
            num_shards: self.num_shards,
            num_workers: self.num_workers,
            resources: self.resources,
            completion_deadline: self.completion_deadline,
            max_retries: self.max_retries,
            store_plan_stages: self.store_plan_stages,
            explain: self.explain,
            planner_options: self.planner_options,
            query_context: self.query_context,
            use_multiple_computers,
            spine_sql_query,
            query_name: self.query_name,
            query_name_version: self.query_name_version,
        })
    }
}

impl Default for OfflineQueryParams {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    #[test]
    fn test_builder_inline_input_serialization() {
        let params = OfflineQueryParams::new()
            .with_input("user.id", vec![json!(1), json!(2), json!(3)])
            .with_output("user.email")
            .with_output("user.ltv");

        let req = params.build().unwrap();
        let json = serde_json::to_value(&req).unwrap();

        let input = &json["input"];
        assert_eq!(input["columns"][0], "user.id");
        assert_eq!(input["values"][0][0], 1);
        assert_eq!(input["values"][0][1], 2);
        assert_eq!(input["values"][0][2], 3);
        assert_eq!(json["output"][0], "user.email");
        assert_eq!(json["output"][1], "user.ltv");
    }

    #[test]
    fn test_builder_with_timestamps() {
        let ts1 = Utc.with_ymd_and_hms(2024, 1, 15, 10, 0, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2024, 2, 15, 10, 0, 0).unwrap();

        let params = OfflineQueryParams::new()
            .with_input("user.id", vec![json!(1), json!(2)])
            .with_input_times(vec![ts1, ts2])
            .with_output("user.email");

        let req = params.build().unwrap();
        let input_type = req.input.unwrap();
        let input = match input_type {
            OfflineQueryInputType::Inline(inline) => inline,
            _ => panic!("expected Inline input"),
        };

        assert!(input.columns.contains(&CHALK_TS_COLUMN.to_string()));
        let ts_col_idx = input
            .columns
            .iter()
            .position(|c| c == CHALK_TS_COLUMN)
            .unwrap();
        let ts_val = input.values[ts_col_idx][0].as_str().unwrap();
        assert!(ts_val.contains("2024-01-15"));
    }

    #[test]
    fn test_builder_validation_no_outputs() {
        let params = OfflineQueryParams::new().with_input("user.id", vec![json!(1)]);

        let result = params.build();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("at least one output"));
    }

    #[test]
    fn test_builder_required_output_only() {
        let params = OfflineQueryParams::new()
            .with_input("user.id", vec![json!(1)])
            .with_required_output("user.email");

        let req = params.build().unwrap();
        assert!(req.output.is_empty());
        assert_eq!(req.required_output.as_ref().unwrap()[0], "user.email");
    }

    #[test]
    fn test_from_uri() {
        let params = OfflineQueryParams::from_uri("s3://bucket/inputs.parquet")
            .with_output("user.email");

        let req = params.build().unwrap();
        let input = req.input.unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["parquet_uri"], "s3://bucket/inputs.parquet");
    }

    #[test]
    fn test_from_uri_serialization() {
        let input = OfflineQueryInputUri {
            parquet_uri: "s3://bucket/inputs.parquet".into(),
            start_row: Some(0),
            end_row: Some(1000),
        };

        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["parquet_uri"], "s3://bucket/inputs.parquet");
        assert_eq!(json["start_row"], 0);
        assert_eq!(json["end_row"], 1000);
    }

    #[test]
    fn test_from_sql() {
        let params =
            OfflineQueryParams::from_sql("SELECT user_id FROM events").with_output("user.email");

        let req = params.build().unwrap();
        assert!(req.input.is_none());
        assert_eq!(
            req.spine_sql_query.as_deref(),
            Some("SELECT user_id FROM events")
        );
    }

    #[test]
    fn test_from_sql_input_serialization() {
        let input = OfflineQueryInputSql {
            input_sql: "SELECT user_id FROM events".into(),
        };

        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["input_sql"], "SELECT user_id FROM events");
    }

    #[test]
    fn test_builder_all_options() {
        let params = OfflineQueryParams::new()
            .with_input("user.id", vec![json!(1)])
            .with_output("user.email")
            .with_num_shards(4)
            .with_num_workers(2)
            .with_run_asynchronously(true)
            .with_dataset_name("my_dataset")
            .with_max_retries(3)
            .with_completion_deadline("3600s");

        let req = params.build().unwrap();
        assert_eq!(req.num_shards, Some(4));
        assert_eq!(req.num_workers, Some(2));
        assert_eq!(req.run_asynchronously, Some(true));
        assert_eq!(req.dataset_name.as_deref(), Some("my_dataset"));
        assert_eq!(req.max_retries, Some(3));
        assert_eq!(req.completion_deadline.as_deref(), Some("3600s"));
        assert_eq!(req.use_multiple_computers, Some(true));
    }

    #[test]
    fn test_use_multiple_computers_not_set_by_default() {
        let params = OfflineQueryParams::new()
            .with_input("user.id", vec![json!(1)])
            .with_output("user.email");

        let req = params.build().unwrap();
        assert!(req.use_multiple_computers.is_none());
    }
}
