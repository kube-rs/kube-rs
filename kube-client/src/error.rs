//! Error handling and error types
use http::Uri;
use thiserror::Error;

pub use kube_core::ErrorResponse;

/// Possible errors from the [`Client`](crate::Client)
#[cfg_attr(docsrs, doc(cfg(any(feature = "config", feature = "client"))))]
#[derive(Error, Debug)]
pub enum Error {
    /// ApiError for when things fail
    ///
    /// This can be parsed into as an error handling fallback.
    /// It's also used in `WatchEvent` from watch calls.
    ///
    /// It's quite common to get a `410 Gone` when the `resourceVersion` is too old.
    #[error("ApiError: {0} ({0:?})")]
    Api(#[source] ErrorResponse),

    /// Hyper error
    #[cfg(feature = "client")]
    #[error("HyperError: {0}")]
    HyperError(#[source] hyper::Error),
    /// Service error
    #[cfg(feature = "client")]
    #[error("ServiceError: {0}")]
    Service(#[source] tower::BoxError),

    /// Returned when the configured proxy uses an unsupported protocol.
    #[error("configured proxy {proxy_url:?} uses an unsupported protocol")]
    ProxyProtocolUnsupported {
        /// The URL of the proxy.
        proxy_url: Uri,
    },
    /// Returned when the configured proxy uses a protocol that requires a Cargo feature that is currently disabled
    #[error("configured proxy {proxy_url:?} requires the disabled feature {protocol_feature:?}")]
    ProxyProtocolDisabled {
        /// The URL of the proxy.
        proxy_url: Uri,
        /// The Cargo feature that the proxy protocol requires.
        protocol_feature: &'static str,
    },

    /// UTF-8 Error
    #[error("UTF-8 Error: {0}")]
    FromUtf8(#[source] std::string::FromUtf8Error),

    /// Returned when failed to find a newline character within max length.
    /// Only returned by `Client::request_events` and this should never happen as
    /// the max is `usize::MAX`.
    #[error("Error finding newline character")]
    LinesCodecMaxLineLengthExceeded,

    /// Returned on `std::io::Error` when reading event stream.
    #[error("Error reading events stream: {0}")]
    ReadEvents(#[source] std::io::Error),

    /// Http based error
    #[error("HttpError: {0}")]
    HttpError(#[source] http::Error),

    /// Common error case when requesting parsing into own structs
    #[error("Error deserializing response: {0}")]
    SerdeError(#[source] serde_json::Error),

    /// Failed to build request
    #[error("Failed to build request: {0}")]
    BuildRequest(#[source] kube_core::request::Error),

    /// Failed to infer config
    #[error("Failed to infer configuration: {0}")]
    InferConfig(#[source] crate::config::InferConfigError),

    /// Discovery errors
    #[error("Error from discovery: {0}")]
    Discovery(#[source] DiscoveryError),

    /// Errors from OpenSSL TLS
    #[cfg(feature = "openssl-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "openssl-tls")))]
    #[error("openssl tls error: {0}")]
    OpensslTls(#[source] crate::client::OpensslTlsError),

    /// Errors from Rustls TLS
    #[cfg(feature = "rustls-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "rustls-tls")))]
    #[error("rustls tls error: {0}")]
    RustlsTls(#[source] crate::client::RustlsTlsError),

    /// Missing TLS stacks when TLS is required
    #[error("TLS required but no TLS stack selected")]
    TlsRequired,

    /// Failed to upgrade to a WebSocket connection
    #[cfg(feature = "ws")]
    #[cfg_attr(docsrs, doc(cfg(feature = "ws")))]
    #[error("failed to upgrade to a WebSocket connection: {0}")]
    UpgradeConnection(#[source] crate::client::UpgradeConnectionError),

    /// Errors related to client auth
    #[cfg(feature = "client")]
    #[cfg_attr(docsrs, doc(cfg(feature = "client")))]
    #[error("auth error: {0}")]
    Auth(#[source] crate::client::AuthError),

    /// Error resolving resource reference
    #[cfg(feature = "unstable-client")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable-client")))]
    #[error("Reference resolve error: {0}")]
    RefResolve(String),

    /// Error resolving resource name
    #[cfg(feature = "unstable-client")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable-client")))]
    #[error("Resource has no name")]
    NameResolve,
}

#[derive(Error, Debug)]
/// Possible errors when using API [discovery](crate::discovery)
pub enum DiscoveryError {
    /// Invalid GroupVersion
    #[error("Invalid GroupVersion: {0}")]
    InvalidGroupVersion(String),

    /// Missing Kind
    #[error("Missing Kind: {0}")]
    MissingKind(String),

    /// Missing ApiGroup
    #[error("Missing Api Group: {0}")]
    MissingApiGroup(String),

    /// MissingResource
    #[error("Missing Resource: {0}")]
    MissingResource(String),

    /// Empty ApiGroup
    #[error("Empty Api Group: {0}")]
    EmptyApiGroup(String),
}
