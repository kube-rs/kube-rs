use hyper_util::rt::TokioExecutor;
// Minimal custom client example.
use k8s_openapi::api::core::v1::Pod;
use tower::BoxError;
use tracing::*;

use kube::{client::ConfigExt, Api, Client, Config, ResourceExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = Config::infer().await?;

    let https = config.rustls_https_connector(None)?;
    let service = tower::ServiceBuilder::new()
        .layer(config.base_uri_layer())
        .option_layer(config.auth_layer()?)
        .map_err(BoxError::from)
        .service(hyper_util::client::legacy::Client::builder(TokioExecutor::new()).build(https));
    let client = Client::new(service, config.default_namespace, None);

    let pods: Api<Pod> = Api::default_namespaced(client);
    for p in pods.list(&Default::default()).await? {
        info!("{}", p.name_any());
    }

    Ok(())
}
