// Custom client example with TraceLayer.
use http::{Request, Response};
use hyper::body::Incoming;
use hyper_util::rt::TokioExecutor;
use k8s_openapi::api::core::v1::Pod;
use std::time::Duration;
use tower::{BoxError, ServiceBuilder};
use tower_http::{decompression::DecompressionLayer, trace::TraceLayer};
use tracing::{Span, *};

use kube::{
    client::{Body, ConfigExt},
    Api, Client, Config, ResourceExt,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = Config::infer().await?;
    let https = config.rustls_https_connector(None)?;
    let service = ServiceBuilder::new()
        .layer(config.base_uri_layer())
        // showcase rate limiting; max 10rps, and 4 concurrent
        .layer(tower::limit::RateLimitLayer::new(10, Duration::from_secs(1)))
        .layer(tower::limit::ConcurrencyLimitLayer::new(4))
        // Add `DecompressionLayer` to make request headers interesting.
        .layer(DecompressionLayer::new())
        .option_layer(config.auth_layer()?)
        .layer(
            // Attribute names follow [Semantic Conventions].
            // [Semantic Conventions]: https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/http.md#http-client
            TraceLayer::new_for_http()
                .make_span_with(|request: &Request<Body>| {
                    tracing::debug_span!(
                        "HTTP",
                        http.method = %request.method(),
                        http.url = %request.uri(),
                        http.status_code = tracing::field::Empty,
                        otel.name = %format!("HTTP {}", request.method()),
                        otel.kind = "client",
                        otel.status_code = tracing::field::Empty,
                    )
                })
                .on_request(|request: &Request<Body>, _span: &Span| {
                    tracing::debug!("payload: {:?} headers: {:?}", request.body(), request.headers())
                })
                .on_response(|response: &Response<Incoming>, latency: Duration, span: &Span| {
                    let status = response.status();
                    span.record("http.status_code", status.as_u16());
                    if status.is_client_error() || status.is_server_error() {
                        span.record("otel.status_code", "ERROR");
                    }
                    tracing::debug!("finished in {}ms", latency.as_millis())
                }),
        )
        .map_err(BoxError::from)
        .service(hyper_util::client::legacy::Client::builder(TokioExecutor::new()).build(https));

    let client = Client::new(service, config.default_namespace, None);

    let pods: Api<Pod> = Api::default_namespaced(client);
    for p in pods.list(&Default::default()).await? {
        info!("{}", p.name_any());
    }

    Ok(())
}
