use bytes::Bytes;
use chrono::{DateTime, Utc};
use http::{header::HeaderMap, Request, Response};
use hyper::{
    body::Incoming,
    rt::{Read, Write},
};
use hyper_timeout::TimeoutConnector;

use hyper_util::{
    client::legacy::connect::{Connection, HttpConnector},
    rt::TokioExecutor,
};

use std::time::Duration;
use tower::{util::BoxService, BoxError, Layer, Service, ServiceBuilder};
use tower_http::{
    classify::ServerErrorsFailureClass, map_response_body::MapResponseBodyLayer, trace::TraceLayer,
};
use tracing::Span;

use super::body::Body;
use crate::{client::ConfigExt, Client, Config, Error, Result};

/// HTTP body of a dynamic backing type.
///
/// The suggested implementation type is [`crate::client::Body`].
pub type DynBody = dyn http_body::Body<Data = Bytes, Error = BoxError> + Send + Unpin;

/// Builder for [`Client`] instances with customized [tower](`Service`) middleware.
pub struct ClientBuilder<Svc> {
    service: Svc,
    default_ns: String,
    valid_until: Option<DateTime<Utc>>,
}

impl<Svc> ClientBuilder<Svc> {
    /// Construct a [`ClientBuilder`] from scratch with a fully custom [`Service`] stack.
    ///
    /// This method is only intended for advanced use cases, most users will want to use [`ClientBuilder::try_from`] instead,
    /// which provides a default stack as a starting point.
    pub fn new(service: Svc, default_namespace: impl Into<String>, valid_until: Option<DateTime<Utc>>) -> Self
    where
        Svc: Service<Request<Body>>,
    {
        Self {
            service,
            default_ns: default_namespace.into(),
            valid_until,
        }
    }

    /// Add a [`Layer`] to the current [`Service`] stack.
    pub fn with_layer<L: Layer<Svc>>(self, layer: &L) -> ClientBuilder<L::Service> {
        let Self {
            service: stack,
            default_ns,
            valid_until,
        } = self;
        ClientBuilder {
            service: layer.layer(stack),
            default_ns,
            valid_until,
        }
    }

    /// Build a [`Client`] instance with the current [`Service`] stack.
    pub fn build<B>(self) -> Client
    where
        Svc: Service<Request<Body>, Response = Response<B>> + Send + 'static,
        Svc::Future: Send + 'static,
        Svc::Error: Into<BoxError>,
        B: http_body::Body<Data = bytes::Bytes> + Send + 'static,
        B::Error: Into<BoxError>,
    {
        Client::new(self.service, self.default_ns, self.valid_until)
    }
}

pub type GenericService = BoxService<Request<Body>, Response<Box<DynBody>>, BoxError>;

impl TryFrom<Config> for ClientBuilder<GenericService> {
    type Error = Error;

    /// Builds a default [`ClientBuilder`] stack from a given configuration
    fn try_from(config: Config) -> Result<Self> {
        let mut connector = HttpConnector::new();
        connector.enforce_http(false);

        #[cfg(all(feature = "aws-lc-rs", feature = "rustls-tls"))]
        {
            if rustls::crypto::CryptoProvider::get_default().is_none() {
                // the only error here is if it's been initialized in between: we can ignore it
                // since our semantic is only to set the default value if it does not exist.
                let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
            }
        }

        match config.proxy_url.as_ref() {
            Some(proxy_url) if proxy_url.scheme_str() == Some("socks5") => {
                #[cfg(feature = "socks5")]
                {
                    let connector = hyper_socks2::SocksConnector {
                        proxy_addr: proxy_url.clone(),
                        auth: None,
                        connector,
                    };
                    make_generic_builder(connector, config)
                }

                #[cfg(not(feature = "socks5"))]
                Err(Error::ProxyProtocolDisabled {
                    proxy_url: proxy_url.clone(),
                    protocol_feature: "kube/socks5",
                })
            }

            Some(proxy_url) if proxy_url.scheme_str() == Some("http") => {
                #[cfg(feature = "http-proxy")]
                {
                    let proxy =
                        hyper_http_proxy::Proxy::new(hyper_http_proxy::Intercept::All, proxy_url.clone());
                    let connector = hyper_http_proxy::ProxyConnector::from_proxy_unsecured(connector, proxy);

                    make_generic_builder(connector, config)
                }

                #[cfg(not(feature = "http-proxy"))]
                Err(Error::ProxyProtocolDisabled {
                    proxy_url: proxy_url.clone(),
                    protocol_feature: "kube/http-proxy",
                })
            }

            Some(proxy_url) => Err(Error::ProxyProtocolUnsupported {
                proxy_url: proxy_url.clone(),
            }),

            None => make_generic_builder(connector, config),
        }
    }
}

/// Helper function for implementation of [`TryFrom<Config>`] for [`ClientBuilder`].
/// Ignores [`Config::proxy_url`], which at this point is already handled.
fn make_generic_builder<H>(connector: H, config: Config) -> Result<ClientBuilder<GenericService>, Error>
where
    H: 'static + Clone + Send + Sync + Service<http::Uri>,
    H::Response: 'static + Connection + Read + Write + Send + Unpin,
    H::Future: 'static + Send,
    H::Error: 'static + Send + Sync + std::error::Error,
{
    let default_ns = config.default_namespace.clone();
    let auth_layer = config.auth_layer()?;

    let (exec_identity, expiration) = config.exec_identity_pem();
    let identity = exec_identity.or_else(|| config.identity_pem());

    let client: hyper_util::client::legacy::Client<_, Body> = {
        // Current TLS feature precedence when more than one are set:
        // 1. rustls-tls
        // 2. openssl-tls
        // Create a custom client to use something else.
        // If TLS features are not enabled, http connector will be used.
        #[cfg(feature = "rustls-tls")]
        let connector = config.rustls_https_connector_with_connector(connector, identity)?;
        #[cfg(all(not(feature = "rustls-tls"), feature = "openssl-tls"))]
        let connector = config.openssl_https_connector_with_connector(connector, identity)?;
        #[cfg(all(not(feature = "rustls-tls"), not(feature = "openssl-tls")))]
        if config.cluster_url.scheme() == Some(&http::uri::Scheme::HTTPS) {
            // no tls stack situation only works with http scheme
            return Err(Error::TlsRequired);
        }

        let mut connector = TimeoutConnector::new(connector);

        // Set the timeouts for the client
        connector.set_connect_timeout(config.connect_timeout);
        connector.set_read_timeout(config.read_timeout);
        connector.set_write_timeout(config.write_timeout);

        hyper_util::client::legacy::Builder::new(TokioExecutor::new()).build(connector)
    };

    let stack = ServiceBuilder::new().layer(config.base_uri_layer()).into_inner();
    #[cfg(feature = "gzip")]
    let stack = ServiceBuilder::new()
        .layer(stack)
        .layer(
            tower_http::decompression::DecompressionLayer::new()
                .no_br()
                .no_deflate()
                .no_zstd()
                .gzip(!config.disable_compression),
        )
        .into_inner();

    let service = ServiceBuilder::new()
        .layer(stack)
        .option_layer(auth_layer)
        .layer(config.extra_headers_layer()?)
        .layer(
            // Attribute names follow [Semantic Conventions].
            // [Semantic Conventions]: https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/http.md
            TraceLayer::new_for_http()
                .make_span_with(|req: &Request<Body>| {
                    tracing::debug_span!(
                        "HTTP",
                         http.method = %req.method(),
                         http.url = %req.uri(),
                         http.status_code = tracing::field::Empty,
                         otel.name = req.extensions().get::<&'static str>().unwrap_or(&"HTTP"),
                         otel.kind = "client",
                         otel.status_code = tracing::field::Empty,
                    )
                })
                .on_request(|_req: &Request<Body>, _span: &Span| {
                    tracing::debug!("requesting");
                })
                .on_response(|res: &Response<Incoming>, _latency: Duration, span: &Span| {
                    let status = res.status();
                    span.record("http.status_code", status.as_u16());
                    if status.is_client_error() || status.is_server_error() {
                        span.record("otel.status_code", "ERROR");
                    }
                })
                // Explicitly disable `on_body_chunk`. The default does nothing.
                .on_body_chunk(())
                .on_eos(|_: Option<&HeaderMap>, _duration: Duration, _span: &Span| {
                    tracing::debug!("stream closed");
                })
                .on_failure(|ec: ServerErrorsFailureClass, _latency: Duration, span: &Span| {
                    // Called when
                    // - Calling the inner service errored
                    // - Polling `Body` errored
                    // - the response was classified as failure (5xx)
                    // - End of stream was classified as failure
                    span.record("otel.status_code", "ERROR");
                    match ec {
                        ServerErrorsFailureClass::StatusCode(status) => {
                            span.record("http.status_code", status.as_u16());
                            tracing::error!("failed with status {}", status)
                        }
                        ServerErrorsFailureClass::Error(err) => {
                            tracing::error!("failed with error {}", err)
                        }
                    }
                }),
        )
        .map_err(BoxError::from)
        .service(client);

    Ok(ClientBuilder::new(
        BoxService::new(
            MapResponseBodyLayer::new(|body| {
                Box::new(http_body_util::BodyExt::map_err(body, BoxError::from)) as Box<DynBody>
            })
            .layer(service),
        ),
        default_ns,
        expiration,
    ))
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "gzip")] use super::*;

    #[cfg(feature = "gzip")]
    #[tokio::test]
    async fn test_no_accept_encoding_header_sent_when_compression_disabled(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use http::Uri;
        use std::net::SocketAddr;
        use tokio::net::{TcpListener, TcpStream};

        // setup a server that echoes back any encoding header value
        let addr: SocketAddr = ([127, 0, 0, 1], 0).into();
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let uri: Uri = format!("http://{}", local_addr).parse()?;

        tokio::spawn(async move {
            use http_body_util::Full;
            use hyper::{server::conn::http1, service::service_fn};
            use hyper_util::rt::{TokioIo, TokioTimer};
            use std::convert::Infallible;

            loop {
                let (tcp, _) = listener.accept().await.unwrap();
                let io: TokioIo<TcpStream> = TokioIo::new(tcp);

                tokio::spawn(async move {
                    let _ = http1::Builder::new()
                        .timer(TokioTimer::new())
                        .serve_connection(
                            io,
                            service_fn(|req| async move {
                                let response = req
                                    .headers()
                                    .get(http::header::ACCEPT_ENCODING)
                                    .map(|b| Bytes::copy_from_slice(b.as_bytes()))
                                    .unwrap_or_default();
                                Ok::<_, Infallible>(Response::new(Full::new(response)))
                            }),
                        )
                        .await
                        .unwrap();
                });
            }
        });

        // confirm gzip echoed back with default config
        let config = Config { ..Config::new(uri) };
        let client = make_generic_builder(HttpConnector::new(), config.clone())?.build();
        let response = client.request_text(http::Request::default()).await?;
        assert_eq!(&response, "gzip");

        // now disable and check empty string echoed back
        let config = Config {
            disable_compression: true,
            ..config
        };
        let client = make_generic_builder(HttpConnector::new(), config)?.build();
        let response = client.request_text(http::Request::default()).await?;
        assert_eq!(&response, "");

        Ok(())
    }
}
