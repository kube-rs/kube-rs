use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

use chrono::{DateTime, Duration, Utc};
use futures::future::BoxFuture;
use http::{
    header::{InvalidHeaderValue, AUTHORIZATION},
    HeaderValue, Request,
};
use jsonpath_rust::JsonPath;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};
use tower::{filter::AsyncPredicate, BoxError};

use crate::config::{AuthInfo, AuthProviderConfig, ExecAuthCluster, ExecConfig, ExecInteractiveMode};

#[cfg(feature = "oauth")] mod oauth;
#[cfg(feature = "oauth")] pub use oauth::Error as OAuthError;
#[cfg(feature = "oidc")] mod oidc;
#[cfg(feature = "oidc")] pub use oidc::errors as oidc_errors;
#[cfg(target_os = "windows")] use std::os::windows::process::CommandExt;

#[derive(Error, Debug)]
/// Client auth errors
pub enum Error {
    /// Invalid basic auth
    #[error("invalid basic auth: {0}")]
    InvalidBasicAuth(#[source] InvalidHeaderValue),

    /// Invalid bearer token
    #[error("invalid bearer token: {0}")]
    InvalidBearerToken(#[source] InvalidHeaderValue),

    /// Tried to refresh a token and got a non-refreshable token response
    #[error("tried to refresh a token and got a non-refreshable token response")]
    UnrefreshableTokenResponse,

    /// Exec plugin response did not contain a status
    #[error("exec-plugin response did not contain a status")]
    ExecPluginFailed,

    /// Malformed token expiration date
    #[error("malformed token expiration date: {0}")]
    MalformedTokenExpirationDate(#[source] chrono::ParseError),

    /// Failed to start auth exec
    #[error("unable to run auth exec: {0}")]
    AuthExecStart(#[source] std::io::Error),

    /// Failed to run auth exec command
    #[error("auth exec command '{cmd}' failed with status {status}: {out:?}")]
    AuthExecRun {
        /// The failed command
        cmd: String,
        /// The exit status or exit code of the failed command
        status: std::process::ExitStatus,
        /// Stdout/Stderr of the failed command
        out: std::process::Output,
    },

    /// Failed to parse auth exec output
    #[error("failed to parse auth exec output: {0}")]
    AuthExecParse(#[source] serde_json::Error),

    /// Fail to serialize input
    #[error("failed to serialize input: {0}")]
    AuthExecSerialize(#[source] serde_json::Error),

    /// Failed to exec auth
    #[error("failed exec auth: {0}")]
    AuthExec(String),

    /// Failed to read token file
    #[error("failed to read token file '{1:?}': {0}")]
    ReadTokenFile(#[source] std::io::Error, PathBuf),

    /// Failed to parse token-key
    #[error("failed to parse token-key")]
    ParseTokenKey(#[source] serde_json::Error),

    /// command was missing from exec config
    #[error("command must be specified to use exec authentication plugin")]
    MissingCommand,

    /// OAuth error
    #[cfg(feature = "oauth")]
    #[cfg_attr(docsrs, doc(cfg(feature = "oauth")))]
    #[error("failed OAuth: {0}")]
    OAuth(#[source] OAuthError),

    /// OIDC error
    #[cfg(feature = "oidc")]
    #[cfg_attr(docsrs, doc(cfg(feature = "oidc")))]
    #[error("failed OIDC: {0}")]
    Oidc(#[source] oidc_errors::Error),

    /// cluster spec missing while `provideClusterInfo` is true
    #[error("Cluster spec must be populated when `provideClusterInfo` is true")]
    ExecMissingClusterInfo,

    /// No valid native root CA certificates found
    #[error("No valid native root CA certificates found")]
    NoValidNativeRootCA(#[source] std::io::Error),
}

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum Auth {
    None,
    Basic(String, SecretString),
    Bearer(SecretString),
    RefreshableToken(RefreshableToken),
    Certificate(String, SecretString, Option<DateTime<Utc>>),
}

// Token file reference. Reloads at least once per minute.
#[derive(Debug)]
pub struct TokenFile {
    path: PathBuf,
    token: SecretString,
    expires_at: DateTime<Utc>,
}

impl TokenFile {
    fn new<P: AsRef<Path>>(path: P) -> Result<TokenFile, Error> {
        let token = std::fs::read_to_string(&path)
            .map_err(|source| Error::ReadTokenFile(source, path.as_ref().to_owned()))?;
        Ok(Self {
            path: path.as_ref().to_owned(),
            token: SecretString::from(token),
            // Try to reload at least once a minute
            expires_at: Utc::now() + SIXTY_SEC,
        })
    }

    fn is_expiring(&self) -> bool {
        Utc::now() + TEN_SEC > self.expires_at
    }

    /// Get the cached token. Returns `None` if it's expiring.
    fn cached_token(&self) -> Option<&str> {
        (!self.is_expiring()).then(|| self.token.expose_secret())
    }

    /// Get a token. Reloads from file if the cached token is expiring.
    fn token(&mut self) -> &str {
        if self.is_expiring() {
            // > If reload from file fails, the last-read token should be used to avoid breaking
            // > clients that make token files available on process start and then remove them to
            // > limit credential exposure.
            // > https://github.com/kubernetes/kubernetes/issues/68164
            if let Ok(token) = std::fs::read_to_string(&self.path) {
                self.token = SecretString::from(token);
            }
            self.expires_at = Utc::now() + SIXTY_SEC;
        }
        self.token.expose_secret()
    }
}

// Questionable decisions by chrono: https://github.com/chronotope/chrono/issues/1491
macro_rules! const_unwrap {
    ($e:expr) => {
        match $e {
            Some(v) => v,
            None => panic!(),
        }
    };
}

/// Common constant for checking if an auth token is close to expiring
pub const TEN_SEC: chrono::TimeDelta = const_unwrap!(Duration::try_seconds(10));
/// Common duration for time between reloads
const SIXTY_SEC: chrono::TimeDelta = const_unwrap!(Duration::try_seconds(60));

// See https://github.com/kubernetes/kubernetes/tree/master/staging/src/k8s.io/client-go/plugin/pkg/client/auth
// for the list of auth-plugins supported by client-go.
// We currently support the following:
// - exec
// - token-file refreshed at least once per minute
// - gcp: command based token source (exec)
// - gcp: application credential based token source (requires `oauth` feature)
//
// Note that the visibility must be `pub` for `impl Layer for AuthLayer`, but this is not exported from the crate.
// It's not accessible from outside and not shown on docs.
#[derive(Debug, Clone)]
pub enum RefreshableToken {
    Exec(Arc<Mutex<(SecretString, DateTime<Utc>, AuthInfo)>>),
    File(Arc<RwLock<TokenFile>>),
    #[cfg(feature = "oauth")]
    GcpOauth(Arc<Mutex<oauth::Gcp>>),
    #[cfg(feature = "oidc")]
    Oidc(Arc<Mutex<oidc::Oidc>>),
}

// For use with `AsyncFilterLayer` to add `Authorization` header with a refreshed token.
impl<B> AsyncPredicate<Request<B>> for RefreshableToken
where
    B: http_body::Body + Send + 'static,
{
    type Future = BoxFuture<'static, Result<Request<B>, BoxError>>;
    type Request = Request<B>;

    fn check(&mut self, mut request: Self::Request) -> Self::Future {
        let refreshable = self.clone();
        Box::pin(async move {
            refreshable.to_header().await.map_err(Into::into).map(|value| {
                request.headers_mut().insert(AUTHORIZATION, value);
                request
            })
        })
    }
}

impl RefreshableToken {
    async fn to_header(&self) -> Result<HeaderValue, Error> {
        match self {
            RefreshableToken::Exec(data) => {
                let mut locked_data = data.lock().await;
                // Add some wiggle room onto the current timestamp so we don't get any race
                // conditions where the token expires while we are refreshing
                if Utc::now() + SIXTY_SEC >= locked_data.1 {
                    // TODO Improve refreshing exec to avoid `Auth::try_from`
                    match Auth::try_from(&locked_data.2)? {
                        Auth::None | Auth::Basic(_, _) | Auth::Bearer(_) | Auth::Certificate(_, _, _) => {
                            return Err(Error::UnrefreshableTokenResponse);
                        }

                        Auth::RefreshableToken(RefreshableToken::Exec(d)) => {
                            let (new_token, new_expire, new_info) = Arc::try_unwrap(d)
                                .expect("Unable to unwrap Arc, this is likely a programming error")
                                .into_inner();
                            locked_data.0 = new_token;
                            locked_data.1 = new_expire;
                            locked_data.2 = new_info;
                        }

                        // Unreachable because the token source does not change
                        Auth::RefreshableToken(RefreshableToken::File(_)) => unreachable!(),
                        #[cfg(feature = "oauth")]
                        Auth::RefreshableToken(RefreshableToken::GcpOauth(_)) => unreachable!(),
                        #[cfg(feature = "oidc")]
                        Auth::RefreshableToken(RefreshableToken::Oidc(_)) => unreachable!(),
                    }
                }

                bearer_header(locked_data.0.expose_secret())
            }

            RefreshableToken::File(token_file) => {
                let guard = token_file.read().await;
                if let Some(header) = guard.cached_token().map(bearer_header) {
                    return header;
                }
                // Drop the read guard before a write lock attempt to prevent deadlock.
                drop(guard);
                // Note that `token()` only reloads if the cached token is expiring.
                // A separate method to conditionally reload minimizes the need for an exclusive access.
                bearer_header(token_file.write().await.token())
            }

            #[cfg(feature = "oauth")]
            RefreshableToken::GcpOauth(data) => {
                let gcp_oauth = data.lock().await;
                let token = (*gcp_oauth).token().await.map_err(Error::OAuth)?;
                bearer_header(&token.access_token)
            }

            #[cfg(feature = "oidc")]
            RefreshableToken::Oidc(oidc) => {
                let token = oidc.lock().await.id_token().await.map_err(Error::Oidc)?;
                bearer_header(&token)
            }
        }
    }
}

fn bearer_header(token: &str) -> Result<HeaderValue, Error> {
    let mut value = HeaderValue::try_from(format!("Bearer {token}")).map_err(Error::InvalidBearerToken)?;
    value.set_sensitive(true);
    Ok(value)
}

impl TryFrom<&AuthInfo> for Auth {
    type Error = Error;

    /// Loads the authentication header from the credentials available in the kubeconfig. This supports
    /// exec plugins as well as specified in
    /// https://kubernetes.io/docs/reference/access-authn-authz/authentication/#client-go-credential-plugins
    fn try_from(auth_info: &AuthInfo) -> Result<Self, Self::Error> {
        if let Some(provider) = &auth_info.auth_provider {
            match token_from_provider(provider)? {
                #[cfg(feature = "oidc")]
                ProviderToken::Oidc(oidc) => {
                    return Ok(Self::RefreshableToken(RefreshableToken::Oidc(Arc::new(
                        Mutex::new(oidc),
                    ))));
                }

                #[cfg(not(feature = "oidc"))]
                ProviderToken::Oidc(token) => {
                    return Ok(Self::Bearer(SecretString::from(token)));
                }

                ProviderToken::GcpCommand(token, Some(expiry)) => {
                    let mut info = auth_info.clone();
                    let mut provider = provider.clone();
                    provider.config.insert("access-token".into(), token.clone());
                    provider.config.insert("expiry".into(), expiry.to_rfc3339());
                    info.auth_provider = Some(provider);
                    return Ok(Self::RefreshableToken(RefreshableToken::Exec(Arc::new(
                        Mutex::new((SecretString::from(token), expiry, info)),
                    ))));
                }

                ProviderToken::GcpCommand(token, None) => {
                    return Ok(Self::Bearer(SecretString::from(token)));
                }

                #[cfg(feature = "oauth")]
                ProviderToken::GcpOauth(gcp) => {
                    return Ok(Self::RefreshableToken(RefreshableToken::GcpOauth(Arc::new(
                        Mutex::new(gcp),
                    ))));
                }
            }
        }

        if let (Some(u), Some(p)) = (&auth_info.username, &auth_info.password) {
            return Ok(Self::Basic(u.to_owned(), p.to_owned()));
        }

        // Inline token. Has precedence over `token_file`.
        if let Some(token) = &auth_info.token {
            return Ok(Self::Bearer(token.clone()));
        }

        // Token file reference. Must be reloaded at least once a minute.
        if let Some(file) = &auth_info.token_file {
            return Ok(Self::RefreshableToken(RefreshableToken::File(Arc::new(
                RwLock::new(TokenFile::new(file)?),
            ))));
        }

        if let Some(exec) = &auth_info.exec {
            let creds = auth_exec(exec)?;
            let status = creds.status.ok_or(Error::ExecPluginFailed)?;
            let expiration = status
                .expiration_timestamp
                .map(|ts| ts.parse())
                .transpose()
                .map_err(Error::MalformedTokenExpirationDate)?;

            if let (Some(client_certificate_data), Some(client_key_data)) =
                (status.client_certificate_data, status.client_key_data)
            {
                return Ok(Self::Certificate(
                    client_certificate_data,
                    client_key_data.into(),
                    expiration,
                ));
            }
            match (status.token.map(SecretString::from), expiration) {
                (Some(token), Some(expire)) => Ok(Self::RefreshableToken(RefreshableToken::Exec(Arc::new(
                    Mutex::new((token, expire, auth_info.clone())),
                )))),
                (Some(token), None) => Ok(Self::Bearer(token)),
                _ => Ok(Self::None),
            }
        } else {
            Ok(Self::None)
        }
    }
}

// We need to differentiate providers because the keys/formats to store token expiration differs.
enum ProviderToken {
    #[cfg(feature = "oidc")]
    Oidc(oidc::Oidc),
    #[cfg(not(feature = "oidc"))]
    Oidc(String),
    // "access-token", "expiry" (RFC3339)
    GcpCommand(String, Option<DateTime<Utc>>),
    #[cfg(feature = "oauth")]
    GcpOauth(oauth::Gcp),
    // "access-token", "expires-on" (timestamp)
    // Azure(String, Option<DateTime<Utc>>),
}

fn token_from_provider(provider: &AuthProviderConfig) -> Result<ProviderToken, Error> {
    match provider.name.as_ref() {
        "oidc" => token_from_oidc_provider(provider),
        "gcp" => token_from_gcp_provider(provider),
        "azure" => Err(Error::AuthExec(
            "The azure auth plugin is not supported; use https://github.com/Azure/kubelogin instead".into(),
        )),
        _ => Err(Error::AuthExec(format!(
            "Authentication with provider {:} not supported",
            provider.name
        ))),
    }
}

#[cfg(feature = "oidc")]
fn token_from_oidc_provider(provider: &AuthProviderConfig) -> Result<ProviderToken, Error> {
    oidc::Oidc::from_config(&provider.config)
        .map_err(Error::Oidc)
        .map(ProviderToken::Oidc)
}

#[cfg(not(feature = "oidc"))]
fn token_from_oidc_provider(provider: &AuthProviderConfig) -> Result<ProviderToken, Error> {
    match provider.config.get("id-token") {
        Some(id_token) => Ok(ProviderToken::Oidc(id_token.clone())),
        None => Err(Error::AuthExec(
            "No id-token for oidc Authentication provider".into(),
        )),
    }
}

fn token_from_gcp_provider(provider: &AuthProviderConfig) -> Result<ProviderToken, Error> {
    if let Some(id_token) = provider.config.get("id-token") {
        return Ok(ProviderToken::GcpCommand(id_token.clone(), None));
    }

    // Return cached access token if it's still valid
    if let Some(access_token) = provider.config.get("access-token") {
        if let Some(expiry) = provider.config.get("expiry") {
            let expiry_date = expiry
                .parse::<DateTime<Utc>>()
                .map_err(Error::MalformedTokenExpirationDate)?;
            if Utc::now() + SIXTY_SEC < expiry_date {
                return Ok(ProviderToken::GcpCommand(access_token.clone(), Some(expiry_date)));
            }
        }
    }

    // Command-based token source
    if let Some(cmd) = provider.config.get("cmd-path") {
        let params = provider.config.get("cmd-args").cloned().unwrap_or_default();
        // NB: This property does currently not exist upstream in client-go
        // See https://github.com/kube-rs/kube/issues/1060
        let drop_env = provider.config.get("cmd-drop-env").cloned().unwrap_or_default();
        // TODO splitting args by space is not safe
        let mut command = Command::new(cmd);
        // Do not pass the following env vars to the command
        for env in drop_env.trim().split(' ') {
            command.env_remove(env);
        }
        let output = command
            .args(params.trim().split(' '))
            .output()
            .map_err(|e| Error::AuthExec(format!("Executing {cmd:} failed: {e:?}")))?;

        if !output.status.success() {
            return Err(Error::AuthExecRun {
                cmd: format!("{cmd} {params}"),
                status: output.status,
                out: output,
            });
        }

        if let Some(field) = provider.config.get("token-key") {
            let json_output: serde_json::Value =
                serde_json::from_slice(&output.stdout).map_err(Error::ParseTokenKey)?;
            let token = extract_value(&json_output, "token-key", field)?;
            if let Some(field) = provider.config.get("expiry-key") {
                let expiry = extract_value(&json_output, "expiry-key", field)?;
                let expiry = expiry
                    .parse::<DateTime<Utc>>()
                    .map_err(Error::MalformedTokenExpirationDate)?;
                return Ok(ProviderToken::GcpCommand(token, Some(expiry)));
            } else {
                return Ok(ProviderToken::GcpCommand(token, None));
            }
        } else {
            let token = std::str::from_utf8(&output.stdout)
                .map_err(|e| Error::AuthExec(format!("Result is not a string {e:?} ")))?
                .to_owned();
            return Ok(ProviderToken::GcpCommand(token, None));
        }
    }

    // Google Application Credentials-based token source
    #[cfg(feature = "oauth")]
    {
        Ok(ProviderToken::GcpOauth(
            oauth::Gcp::default_credentials_with_scopes(provider.config.get("scopes"))
                .map_err(Error::OAuth)?,
        ))
    }
    #[cfg(not(feature = "oauth"))]
    {
        Err(Error::AuthExec(
            "Enable oauth feature to use Google Application Credentials-based token source".into(),
        ))
    }
}

fn extract_value(json: &serde_json::Value, context: &str, path: &str) -> Result<String, Error> {
    let parsed_path = path
        .trim_matches(|c| c == '"' || c == '{' || c == '}')
        .parse::<JsonPath>()
        .map_err(|err| {
            Error::AuthExec(format!(
                "Failed to parse {context:?} as a JsonPath: {path}\n
                 Error: {err}"
            ))
        })?;

    let res = parsed_path.find_slice(json);

    let Some(res) = res.into_iter().next() else {
        return Err(Error::AuthExec(format!(
            "Target {context:?} value {path:?} not found"
        )));
    };

    let jval = res.to_data();
    let val = jval.as_str().ok_or(Error::AuthExec(format!(
        "Target {context:?} value {path:?} is not a string"
    )))?;

    Ok(val.to_string())
}

/// ExecCredentials is used by exec-based plugins to communicate credentials to
/// HTTP transports.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecCredential {
    pub kind: Option<String>,
    #[serde(rename = "apiVersion")]
    pub api_version: Option<String>,
    pub spec: Option<ExecCredentialSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ExecCredentialStatus>,
}

/// ExecCredenitalSpec holds request and runtime specific information provided
/// by transport.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecCredentialSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    interactive: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    cluster: Option<ExecAuthCluster>,
}

/// ExecCredentialStatus holds credentials for the transport to use.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecCredentialStatus {
    #[serde(rename = "expirationTimestamp")]
    pub expiration_timestamp: Option<String>,
    pub token: Option<String>,
    #[serde(rename = "clientCertificateData")]
    pub client_certificate_data: Option<String>,
    #[serde(rename = "clientKeyData")]
    pub client_key_data: Option<String>,
}

fn auth_exec(auth: &ExecConfig) -> Result<ExecCredential, Error> {
    let mut cmd = match &auth.command {
        Some(cmd) => Command::new(cmd),
        None => return Err(Error::MissingCommand),
    };

    if let Some(args) = &auth.args {
        cmd.args(args);
    }
    if let Some(env) = &auth.env {
        let envs = env
            .iter()
            .flat_map(|env| match (env.get("name"), env.get("value")) {
                (Some(name), Some(value)) => Some((name, value)),
                _ => None,
            });
        cmd.envs(envs);
    }

    let interactive = auth.interactive_mode != Some(ExecInteractiveMode::Never);
    if interactive {
        cmd.stdin(std::process::Stdio::inherit());
    } else {
        cmd.stdin(std::process::Stdio::piped());
    }

    let mut exec_credential_spec = ExecCredentialSpec {
        interactive: Some(interactive),
        cluster: None,
    };

    if auth.provide_cluster_info {
        exec_credential_spec.cluster = Some(auth.cluster.clone().ok_or(Error::ExecMissingClusterInfo)?);
    }

    // Provide exec info to child process
    let exec_info = serde_json::to_string(&ExecCredential {
        api_version: auth.api_version.clone(),
        kind: "ExecCredential".to_string().into(),
        spec: Some(exec_credential_spec),
        status: None,
    })
    .map_err(Error::AuthExecSerialize)?;
    cmd.env("KUBERNETES_EXEC_INFO", exec_info);

    if let Some(envs) = &auth.drop_env {
        for env in envs {
            cmd.env_remove(env);
        }
    }

    #[cfg(target_os = "windows")]
    {
        const CREATE_NO_WINDOW: u32 = 0x08000000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }

    let out = cmd.output().map_err(Error::AuthExecStart)?;
    if !out.status.success() {
        return Err(Error::AuthExecRun {
            cmd: format!("{cmd:?}"),
            status: out.status,
            out,
        });
    }
    let creds = serde_json::from_slice(&out.stdout).map_err(Error::AuthExecParse)?;

    Ok(creds)
}

#[cfg(test)]
mod test {
    use crate::config::Kubeconfig;

    use super::*;
    #[tokio::test]
    #[ignore = "fails on windows mysteriously"]
    async fn exec_auth_command() -> Result<(), Error> {
        let expiry = (Utc::now() + SIXTY_SEC).to_rfc3339();
        let test_file = format!(
            r#"
        apiVersion: v1
        clusters:
        - cluster:
            certificate-authority-data: XXXXXXX
            server: https://36.XXX.XXX.XX
          name: generic-name
        contexts:
        - context:
            cluster: generic-name
            user: generic-name
          name: generic-name
        current-context: generic-name
        kind: Config
        preferences: {{}}
        users:
        - name: generic-name
          user:
            auth-provider:
              config:
                cmd-args: '{{"something": "else", "credential": {{"access_token": "my_token", "token_expiry": "{expiry}"}}}}'
                cmd-path: echo
                expiry-key: '{{.credential.token_expiry}}'
                token-key: '{{.credential.access_token}}'
              name: gcp
        "#
        );

        let config: Kubeconfig = serde_yaml::from_str(&test_file).unwrap();
        let auth_info = config.auth_infos[0].auth_info.as_ref().unwrap();
        match Auth::try_from(auth_info).unwrap() {
            Auth::RefreshableToken(RefreshableToken::Exec(refreshable)) => {
                let (token, _expire, info) = Arc::try_unwrap(refreshable).unwrap().into_inner();
                assert_eq!(token.expose_secret(), &"my_token".to_owned());
                let config = info.auth_provider.unwrap().config;
                assert_eq!(config.get("access-token"), Some(&"my_token".to_owned()));
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    #[test]
    fn token_file() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), "token1").unwrap();
        let mut token_file = TokenFile::new(file.path()).unwrap();
        assert_eq!(token_file.cached_token().unwrap(), "token1");
        assert!(!token_file.is_expiring());
        assert_eq!(token_file.token(), "token1");
        // Doesn't reload unless expiring
        std::fs::write(file.path(), "token2").unwrap();
        assert_eq!(token_file.token(), "token1");

        token_file.expires_at = Utc::now();
        assert!(token_file.is_expiring());
        assert_eq!(token_file.cached_token(), None);
        assert_eq!(token_file.token(), "token2");
        assert!(!token_file.is_expiring());
        assert_eq!(token_file.cached_token().unwrap(), "token2");
    }
}
