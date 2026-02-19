//! Configuration resolution for the Chalk client.
//!
//! Chalk credentials and endpoints can come from three places (checked in
//! this order):
//!
//! 1. **Explicit values** passed to the builder (highest priority).
//! 2. **Environment variables** like `CHALK_CLIENT_ID` and `_CHALK_CLIENT_ID`.
//! 3. **`~/.chalk.yml`** — a YAML file written by `chalk login`.
//! 4. **Defaults** — e.g. the API server defaults to `https://api.chalk.ai`.
//!
//! This module implements the builder pattern — you create a
//! [`ChalkClientConfigBuilder`](crate::config::ChalkClientConfigBuilder), set the fields you know, call `.build()`,
//! and the builder fills in the rest from env/file/defaults.

use std::collections::HashMap;
use std::path::PathBuf;

use serde::Deserialize;

use crate::error::{ChalkClientError, Result};

/// The default Chalk API server URL.
const DEFAULT_API_SERVER: &str = "https://api.chalk.ai";

/// Holds all resolved configuration needed to connect to Chalk.
///
/// You don't construct this directly — use [`ChalkClientConfigBuilder`].
#[derive(Debug, Clone)]
pub struct ChalkClientConfig {
    /// OAuth2 client ID (required).
    pub client_id: String,

    /// OAuth2 client secret (required).
    pub client_secret: String,

    /// The API server URL (e.g. `https://api.chalk.ai`).
    pub api_server: String,

    /// The target environment (e.g. `"production"` or an environment ID).
    pub environment: Option<String>,

    /// A branch ID for branch deployments.
    pub branch_id: Option<String>,

    /// A deployment tag for routing to specific deployments.
    pub deployment_tag: Option<String>,

    /// Override for the query server URL.
    pub query_server: Option<String>,
}

/// A builder for [`ChalkClientConfig`].
///
/// ## Example
///
/// ```rust,no_run
/// use chalk_rs::config::ChalkClientConfigBuilder;
///
/// let config = ChalkClientConfigBuilder::new()
///     .client_id("my-client-id")
///     .client_secret("my-secret")
///     .environment("production")
///     .build()
///     .expect("failed to build config");
/// ```
#[derive(Debug, Default)]
pub struct ChalkClientConfigBuilder {
    client_id: Option<String>,
    client_secret: Option<String>,
    api_server: Option<String>,
    environment: Option<String>,
    branch_id: Option<String>,
    deployment_tag: Option<String>,
    query_server: Option<String>,
}

impl ChalkClientConfigBuilder {
    /// Create a new, empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the OAuth2 client ID.
    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = Some(id.into());
        self
    }

    /// Set the OAuth2 client secret.
    pub fn client_secret(mut self, secret: impl Into<String>) -> Self {
        self.client_secret = Some(secret.into());
        self
    }

    /// Set the API server URL.
    pub fn api_server(mut self, url: impl Into<String>) -> Self {
        self.api_server = Some(url.into());
        self
    }

    /// Set the target environment.
    pub fn environment(mut self, env: impl Into<String>) -> Self {
        self.environment = Some(env.into());
        self
    }

    /// Set the branch ID.
    pub fn branch_id(mut self, id: impl Into<String>) -> Self {
        self.branch_id = Some(id.into());
        self
    }

    /// Set the deployment tag.
    pub fn deployment_tag(mut self, tag: impl Into<String>) -> Self {
        self.deployment_tag = Some(tag.into());
        self
    }

    /// Set the query server URL directly (skips engine-map resolution).
    pub fn query_server(mut self, url: impl Into<String>) -> Self {
        self.query_server = Some(url.into());
        self
    }

    /// Resolve all configuration and produce a [`ChalkClientConfig`].
    ///
    /// Returns an error if `client_id` or `client_secret` cannot be found
    /// from any source.
    pub fn build(self) -> Result<ChalkClientConfig> {
        let yaml_config = load_yaml_config();

        let client_id = self
            .client_id
            .or_else(|| get_env("CHALK_CLIENT_ID"))
            .or_else(|| get_env("_CHALK_CLIENT_ID"))
            .or_else(|| yaml_config.as_ref().map(|c| c.client_id.clone()))
            .ok_or_else(|| {
                ChalkClientError::Config(
                    "client_id is required — set it explicitly, via CHALK_CLIENT_ID env var, \
                     or by running `chalk login`"
                        .into(),
                )
            })?;

        let client_secret = self
            .client_secret
            .or_else(|| get_env("CHALK_CLIENT_SECRET"))
            .or_else(|| get_env("_CHALK_CLIENT_SECRET"))
            .or_else(|| yaml_config.as_ref().map(|c| c.client_secret.clone()))
            .ok_or_else(|| {
                ChalkClientError::Config(
                    "client_secret is required — set it explicitly, via CHALK_CLIENT_SECRET \
                     env var, or by running `chalk login`"
                        .into(),
                )
            })?;

        let api_server = self
            .api_server
            .or_else(|| get_env("CHALK_API_SERVER"))
            .or_else(|| get_env("_CHALK_API_SERVER"))
            .or_else(|| yaml_config.as_ref().and_then(|c| c.api_server.clone()))
            .unwrap_or_else(|| DEFAULT_API_SERVER.to_string());

        let environment = self
            .environment
            .or_else(|| get_env("CHALK_ACTIVE_ENVIRONMENT"))
            .or_else(|| get_env("_CHALK_ACTIVE_ENVIRONMENT"))
            .or_else(|| {
                yaml_config
                    .as_ref()
                    .and_then(|c| c.active_environment.clone())
            });

        let branch_id = self
            .branch_id
            .or_else(|| get_env("CHALK_BRANCH_ID"))
            .or_else(|| get_env("_CHALK_BRANCH_ID"));

        let deployment_tag = self
            .deployment_tag
            .or_else(|| get_env("CHALK_DEPLOYMENT_TAG"))
            .or_else(|| get_env("_CHALK_DEPLOYMENT_TAG"));

        let query_server = self
            .query_server
            .or_else(|| get_env("CHALK_QUERY_SERVER"))
            .or_else(|| get_env("_CHALK_QUERY_SERVER"));

        Ok(ChalkClientConfig {
            client_id,
            client_secret,
            api_server,
            environment,
            branch_id,
            deployment_tag,
            query_server,
        })
    }
}

/// Read an environment variable, returning `None` if it's unset or empty.
fn get_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

/// The top-level structure of `~/.chalk.yml`.
#[derive(Debug, Deserialize)]
struct YamlConfig {
    #[serde(default)]
    tokens: HashMap<String, YamlProjectToken>,
}

/// Credentials for a single project in the YAML config.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct YamlProjectToken {
    client_id: String,
    client_secret: String,
    #[serde(default)]
    api_server: Option<String>,
    #[serde(default)]
    active_environment: Option<String>,
}

/// Try to find and parse `~/.chalk.yml` (or `~/.chalk.yaml`).
fn load_yaml_config() -> Option<YamlProjectToken> {
    let home = dirs::home_dir()?;
    let path = find_config_file(&home)?;
    let contents = std::fs::read_to_string(&path).ok()?;
    let config: YamlConfig = serde_yaml::from_str(&contents).ok()?;

    let project_root = find_project_root();

    let token = project_root
        .as_deref()
        .and_then(|root| config.tokens.get(root))
        .or_else(|| config.tokens.get("default"));

    token.cloned()
}

/// Look for `.chalk.yml` or `.chalk.yaml` in the home directory.
fn find_config_file(home: &std::path::Path) -> Option<PathBuf> {
    let yml = home.join(".chalk.yml");
    if yml.exists() {
        return Some(yml);
    }

    let yaml = home.join(".chalk.yaml");
    if yaml.exists() {
        return Some(yaml);
    }

    if let Some(xdg) = get_env("XDG_CONFIG_HOME") {
        let xdg_path = PathBuf::from(xdg).join(".chalk.yml");
        if xdg_path.exists() {
            return Some(xdg_path);
        }
    }

    None
}

/// Walk up from the current directory looking for `chalk.yml` or `chalk.yaml`.
fn find_project_root() -> Option<String> {
    let mut dir = std::env::current_dir().ok()?;
    loop {
        if dir.join("chalk.yml").exists() || dir.join("chalk.yaml").exists() {
            return Some(dir.to_string_lossy().into_owned());
        }
        if !dir.pop() {
            break;
        }
    }
    None
}

impl Clone for YamlProjectToken {
    fn clone(&self) -> Self {
        Self {
            client_id: self.client_id.clone(),
            client_secret: self.client_secret.clone(),
            api_server: self.api_server.clone(),
            active_environment: self.active_environment.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Tests in this module mutate process-global env vars (`CHALK_*`, `HOME`).
    /// Acquiring this lock before touching env state prevents races when cargo
    /// runs tests in parallel.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    const CHALK_ENV_VARS: &[&str] = &[
        "CHALK_CLIENT_ID",
        "_CHALK_CLIENT_ID",
        "CHALK_CLIENT_SECRET",
        "_CHALK_CLIENT_SECRET",
        "CHALK_API_SERVER",
        "_CHALK_API_SERVER",
        "CHALK_ACTIVE_ENVIRONMENT",
        "_CHALK_ACTIVE_ENVIRONMENT",
        "CHALK_BRANCH_ID",
        "_CHALK_BRANCH_ID",
        "CHALK_DEPLOYMENT_TAG",
        "_CHALK_DEPLOYMENT_TAG",
        "CHALK_QUERY_SERVER",
        "_CHALK_QUERY_SERVER",
    ];

    fn clear_chalk_env() {
        for var in CHALK_ENV_VARS {
            std::env::remove_var(var);
        }
    }

    #[test]
    fn test_builder_explicit_values() {
        let config = ChalkClientConfigBuilder::new()
            .client_id("test-id")
            .client_secret("test-secret")
            .api_server("https://custom.chalk.ai")
            .environment("staging")
            .branch_id("branch-1")
            .deployment_tag("canary")
            .query_server("https://query.chalk.ai")
            .build()
            .unwrap();

        assert_eq!(config.client_id, "test-id");
        assert_eq!(config.client_secret, "test-secret");
        assert_eq!(config.api_server, "https://custom.chalk.ai");
        assert_eq!(config.environment.as_deref(), Some("staging"));
        assert_eq!(config.branch_id.as_deref(), Some("branch-1"));
        assert_eq!(config.deployment_tag.as_deref(), Some("canary"));
        assert_eq!(
            config.query_server.as_deref(),
            Some("https://query.chalk.ai")
        );
    }

    #[test]
    fn test_builder_default_api_server() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_chalk_env();

        let config = ChalkClientConfigBuilder::new()
            .client_id("id")
            .client_secret("secret")
            .api_server(DEFAULT_API_SERVER)
            .build()
            .unwrap();

        assert_eq!(config.api_server, DEFAULT_API_SERVER);
    }

    #[test]
    fn test_builder_missing_credentials() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_chalk_env();
        // Use a real but empty temp dir so dirs::home_dir() resolves it
        // but no .chalk.yml exists inside it.
        let tmp = std::env::temp_dir().join("chalk_test_no_yaml_creds");
        let _ = std::fs::create_dir_all(&tmp);
        let original_home = std::env::var("HOME").ok();
        std::env::set_var("HOME", &tmp);

        // Missing client_id
        let result = ChalkClientConfigBuilder::new()
            .client_secret("secret")
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("client_id"));

        // Missing client_secret
        let result = ChalkClientConfigBuilder::new()
            .client_id("id")
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("client_secret"));

        if let Some(h) = original_home {
            std::env::set_var("HOME", h);
        }
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_get_env_helper() {
        let var = "_CHALK_TEST_GET_ENV_HELPER";

        std::env::remove_var(var);
        assert_eq!(get_env(var), None);

        std::env::set_var(var, "");
        assert_eq!(get_env(var), None);

        std::env::set_var(var, "hello");
        assert_eq!(get_env(var), Some("hello".to_string()));

        std::env::remove_var(var);
    }

    #[test]
    fn test_builder_explicit_overrides_env() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_chalk_env();

        std::env::set_var("CHALK_CLIENT_ID", "env-id");
        std::env::set_var("CHALK_CLIENT_SECRET", "env-secret");

        let config = ChalkClientConfigBuilder::new()
            .client_id("explicit-id")
            .client_secret("explicit-secret")
            .build()
            .unwrap();

        assert_eq!(config.client_id, "explicit-id");
        assert_eq!(config.client_secret, "explicit-secret");

        clear_chalk_env();
    }

    #[test]
    fn test_yaml_config_parsing() {
        let yaml = r#"
tokens:
  default:
    clientId: "yaml-id"
    clientSecret: "yaml-secret"
    apiServer: "https://yaml.chalk.ai"
    activeEnvironment: "yaml-env"
"#;

        let config: YamlConfig = serde_yaml::from_str(yaml).unwrap();
        let token = config.tokens.get("default").unwrap();

        assert_eq!(token.client_id, "yaml-id");
        assert_eq!(token.client_secret, "yaml-secret");
        assert_eq!(token.api_server.as_deref(), Some("https://yaml.chalk.ai"));
        assert_eq!(token.active_environment.as_deref(), Some("yaml-env"));
    }
}
