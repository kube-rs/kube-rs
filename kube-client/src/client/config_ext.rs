use std::sync::Arc;

use chrono::{DateTime, Utc};
use http::{header::HeaderName, HeaderValue};
#[cfg(feature = "openssl-tls")] use hyper::rt::{Read, Write};
use hyper_util::client::legacy::connect::HttpConnector;
use secrecy::ExposeSecret;
use tower::{filter::AsyncFilterLayer, util::Either};

#[cfg(any(feature = "rustls-tls", feature = "openssl-tls"))] use super::tls;
use super::{
    auth::Auth,
    middleware::{AddAuthorizationLayer, AuthLayer, BaseUriLayer, ExtraHeadersLayer},
};
use crate::{Config, Error, Result};

/// Extensions to [`Config`](crate::Config) for custom [`Client`](crate::Client).
///
/// See [`Client::new`](crate::Client::new) for an example.
///
/// This trait is sealed and cannot be implemented.
pub trait ConfigExt: private::Sealed {
    /// Layer to set the base URI of requests to the configured server.
    fn base_uri_layer(&self) -> BaseUriLayer;

    /// Optional layer to set up `Authorization` header depending on the config.
    fn auth_layer(&self) -> Result<Option<AuthLayer>>;

    /// Layer to add non-authn HTTP headers depending on the config.
    fn extra_headers_layer(&self) -> Result<ExtraHeadersLayer>;

    /// Create [`hyper_rustls::HttpsConnector`] based on config.
    ///
    /// # Example
    ///
    /// ```rust
    /// # async fn doc() -> Result<(), Box<dyn std::error::Error>> {
    /// # use kube::{client::{Body, ConfigExt}, Config};
    /// # use hyper_util::rt::TokioExecutor;
    /// let config = Config::infer().await?;
    /// let https = config.rustls_https_connector()?;
    /// let hyper_client: hyper_util::client::legacy::Client<_, Body> = hyper_util::client::legacy::Client::builder(TokioExecutor::new()).build(https);
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(docsrs, doc(cfg(feature = "rustls-tls")))]
    #[cfg(feature = "rustls-tls")]
    fn rustls_https_connector(
        &self,
        identity: Option<Vec<u8>>,
    ) -> Result<hyper_rustls::HttpsConnector<HttpConnector>>;

    /// Create [`hyper_rustls::HttpsConnector`] based on config and `connector`.
    ///
    /// # Example
    ///
    /// ```rust
    /// # async fn doc() -> Result<(), Box<dyn std::error::Error>> {
    /// # use kube::{client::{Body, ConfigExt}, Config};
    /// # use hyper_util::{client::legacy::connect::HttpConnector, rt::TokioExecutor};
    /// let config = Config::infer().await?;
    /// let mut connector = HttpConnector::new();
    /// connector.enforce_http(false);
    /// let https = config.rustls_https_connector_with_connector(connector)?;
    /// let hyper_client: hyper_util::client::legacy::Client<_, Body> = hyper_util::client::legacy::Client::builder(TokioExecutor::new()).build(https);
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(docsrs, doc(cfg(feature = "rustls-tls")))]
    #[cfg(feature = "rustls-tls")]
    fn rustls_https_connector_with_connector<H>(
        &self,
        connector: H,
        identity: Option<Vec<u8>>,
    ) -> Result<hyper_rustls::HttpsConnector<H>>;

    /// Create [`hyper_openssl::HttpsConnector`] based on config.
    /// # Example
    ///
    /// ```rust
    /// # async fn doc() -> Result<(), Box<dyn std::error::Error>> {
    /// # use kube::{client::ConfigExt, Config};
    /// let config = Config::infer().await?;
    /// let https = config.openssl_https_connector()?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(docsrs, doc(cfg(feature = "openssl-tls")))]
    #[cfg(feature = "openssl-tls")]
    fn openssl_https_connector(
        &self,
        identity: Option<Vec<u8>>,
    ) -> Result<hyper_openssl::client::legacy::HttpsConnector<HttpConnector>>;

    /// Create [`hyper_openssl::HttpsConnector`] based on config and `connector`.
    /// # Example
    ///
    /// ```rust
    /// # async fn doc() -> Result<(), Box<dyn std::error::Error>> {
    /// # use hyper_util::client::legacy::connect::HttpConnector;
    /// # use kube::{client::ConfigExt, Config};
    /// let mut http = HttpConnector::new();
    /// http.enforce_http(false);
    /// let config = Config::infer().await?;
    /// let https = config.openssl_https_connector_with_connector(http)?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(docsrs, doc(cfg(feature = "openssl-tls")))]
    #[cfg(feature = "openssl-tls")]
    fn openssl_https_connector_with_connector<H>(
        &self,
        connector: H,
        identity: Option<Vec<u8>>,
    ) -> Result<hyper_openssl::client::legacy::HttpsConnector<H>>
    where
        H: tower::Service<http::Uri> + Send,
        H::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        H::Future: Send + 'static,
        H::Response: Read + Write + hyper_util::client::legacy::connect::Connection + Unpin;

    /// Create [`openssl::ssl::SslConnectorBuilder`] based on config.
    /// # Example
    ///
    /// ```rust
    /// # async fn doc() -> Result<(), Box<dyn std::error::Error>> {
    /// # use hyper_util::client::legacy::connect::HttpConnector;
    /// # use kube::{client::ConfigExt, Client, Config};
    /// let config = Config::infer().await?;
    /// let https = {
    ///     let mut http = HttpConnector::new();
    ///     http.enforce_http(false);
    ///     let ssl = config.openssl_ssl_connector_builder()?;
    ///     hyper_openssl::client::legacy::HttpsConnector::with_connector(http, ssl)?
    /// };
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(docsrs, doc(cfg(feature = "openssl-tls")))]
    #[cfg(feature = "openssl-tls")]
    fn openssl_ssl_connector_builder(
        &self,
        identity: Option<Vec<u8>>,
    ) -> Result<openssl::ssl::SslConnectorBuilder>;
}

mod private {
    pub trait Sealed {}
    impl Sealed for super::Config {}
}

impl ConfigExt for Config {
    fn base_uri_layer(&self) -> BaseUriLayer {
        BaseUriLayer::new(self.cluster_url.clone())
    }

    fn auth_layer(&self) -> Result<Option<AuthLayer>> {
        Ok(match Auth::try_from(&self.auth_info).map_err(Error::Auth)? {
            Auth::None => None,
            Auth::Basic(user, pass) => Some(AuthLayer(Either::Left(
                AddAuthorizationLayer::basic(&user, pass.expose_secret()).as_sensitive(true),
            ))),
            Auth::Bearer(token) => Some(AuthLayer(Either::Left(
                AddAuthorizationLayer::bearer(token.expose_secret()).as_sensitive(true),
            ))),
            Auth::RefreshableToken(refreshable) => {
                Some(AuthLayer(Either::Right(AsyncFilterLayer::new(refreshable))))
            }
            Auth::Certificate(_client_certificate_data, _client_key_data, _) => None,
        })
    }

    fn extra_headers_layer(&self) -> Result<ExtraHeadersLayer> {
        let mut headers = self.headers.clone();
        if let Some(impersonate_user) = &self.auth_info.impersonate {
            headers.push((
                HeaderName::from_static("impersonate-user"),
                HeaderValue::from_str(impersonate_user)
                    .map_err(http::Error::from)
                    .map_err(Error::HttpError)?,
            ));
        }
        if let Some(impersonate_groups) = &self.auth_info.impersonate_groups {
            for group in impersonate_groups {
                headers.push((
                    HeaderName::from_static("impersonate-group"),
                    HeaderValue::from_str(group)
                        .map_err(http::Error::from)
                        .map_err(Error::HttpError)?,
                ));
            }
        }
        Ok(ExtraHeadersLayer {
            headers: Arc::new(headers),
        })
    }

    #[cfg(feature = "rustls-tls")]
    fn rustls_https_connector(
        &self,
        identity: Option<Vec<u8>>,
    ) -> Result<hyper_rustls::HttpsConnector<HttpConnector>> {
        let mut connector = HttpConnector::new();
        connector.enforce_http(false);
        self.rustls_https_connector_with_connector(connector, identity)
    }

    #[cfg(feature = "rustls-tls")]
    fn rustls_https_connector_with_connector<H>(
        &self,
        connector: H,
        identity: Option<Vec<u8>>,
    ) -> Result<hyper_rustls::HttpsConnector<H>> {
        use hyper_rustls::FixedServerNameResolver;

        use crate::client::tls::rustls_tls;

        let rustls_config = tls::rustls_tls::rustls_client_config(
            identity.as_deref(),
            self.root_cert.as_deref(),
            self.accept_invalid_certs,
        )
        .map_err(Error::RustlsTls)?;

        let mut builder = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(rustls_config)
            .https_or_http();
        if let Some(tsn) = self.tls_server_name.as_ref() {
            builder = builder.with_server_name_resolver(FixedServerNameResolver::new(
                tsn.clone()
                    .try_into()
                    .map_err(rustls_tls::Error::InvalidServerName)
                    .map_err(Error::RustlsTls)?,
            ));
        }
        Ok(builder.enable_http1().wrap_connector(connector))
    }

    #[cfg(feature = "openssl-tls")]
    fn openssl_ssl_connector_builder(
        &self,
        identity: Option<Vec<u8>>,
    ) -> Result<openssl::ssl::SslConnectorBuilder> {
        // TODO: pass self.tls_server_name for openssl
        tls::openssl_tls::ssl_connector_builder(identity.as_ref(), self.root_cert.as_ref())
            .map_err(|e| Error::OpensslTls(tls::openssl_tls::Error::CreateSslConnector(e)))
    }

    #[cfg(feature = "openssl-tls")]
    fn openssl_https_connector(
        &self,
        identity: Option<Vec<u8>>,
    ) -> Result<hyper_openssl::client::legacy::HttpsConnector<HttpConnector>> {
        let mut connector = HttpConnector::new();
        connector.enforce_http(false);
        self.openssl_https_connector_with_connector(connector, identity)
    }

    #[cfg(feature = "openssl-tls")]
    fn openssl_https_connector_with_connector<H>(
        &self,
        connector: H,
        identity: Option<Vec<u8>>,
    ) -> Result<hyper_openssl::client::legacy::HttpsConnector<H>>
    where
        H: tower::Service<http::Uri> + Send,
        H::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        H::Future: Send + 'static,
        H::Response: Read + Write + hyper_util::client::legacy::connect::Connection + Unpin,
    {
        let mut https = hyper_openssl::client::legacy::HttpsConnector::with_connector(
            connector,
            self.openssl_ssl_connector_builder(identity)?,
        )
        .map_err(|e| Error::OpensslTls(tls::openssl_tls::Error::CreateHttpsConnector(e)))?;
        if self.accept_invalid_certs {
            https.set_callback(|ssl, _uri| {
                ssl.set_verify(openssl::ssl::SslVerifyMode::NONE);
                Ok(())
            });
        }
        Ok(https)
    }
}

impl Config {
    /// Retrieves an identity when an exec plugin returns a client certificate and key instead of a token.
    ///
    /// This is necessary to check on TLS configuration vs tokens which can be added in as an AuthLayer.
    ///
    /// # Returns
    ///
    /// A tuple containing an optional vector of bytes representing the identity and an optional expiration date.

    // This is necessary to retrieve an identity when an exec plugin
    // returns a client certificate and key instead of a token.
    // This has be to be checked on TLS configuration vs tokens
    // which can be added in as an AuthLayer.
    pub fn exec_identity_pem(&self) -> (Option<Vec<u8>>, Option<DateTime<Utc>>) {
        match Auth::try_from(&self.auth_info) {
            Ok(Auth::Certificate(client_certificate_data, client_key_data, expiratiom)) => {
                const NEW_LINE: u8 = b'\n';

                let mut buffer = client_key_data.expose_secret().as_bytes().to_vec();
                buffer.push(NEW_LINE);
                buffer.extend_from_slice(client_certificate_data.as_bytes());
                buffer.push(NEW_LINE);
                (Some(buffer), expiratiom)
            }
            _ => (None, None),
        }
    }
}
