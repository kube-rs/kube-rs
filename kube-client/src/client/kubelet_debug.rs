use crate::{
    api::{AttachParams, AttachedProcess, LogParams, Portforwarder},
    client::AsyncBufRead,
    Client, Error, Result,
};
use kube_core::{kubelet_debug::KubeletDebugParams, Request};
use std::fmt::Debug;

/// Methods to access debug endpoints directly on `kubelet`
///
/// These provide analogous methods to the `Pod` api methods for [`Execute`](crate::api::Exec), [`Attach`](crate::api::Attach), and [`Portforward`](crate::api::Portforward).
/// Service account must have `nodes/proxy` access, and
/// the debug handlers must be enabled either via `--enable-debugging-handlers ` or in the [kubelet config](https://kubernetes.io/docs/reference/config-api/kubelet-config.v1beta1/#kubelet-config-k8s-io-v1beta1-KubeletConfiguration).
/// See the [kubelet source](https://github.com/kubernetes/kubernetes/blob/b3926d137cd2964cd3a04088ded30845910547b1/pkg/kubelet/server/server.go#L454), and [kubelet reference](https://kubernetes.io/docs/reference/command-line-tools-reference/kubelet/) for more info.
///
/// ## Warning
/// These methods require direct, and **insecure access** to `kubelet` and is only available under the `kubelet_debug` feature.
/// End-to-end usage is explored in the [pod_log_kubelet_debug](https://github.com/kube-rs/kube/blob/main/examples/pod_log_kubelet_debug.rs) example.
#[cfg(feature = "kubelet-debug")]
impl Client {
    /// Attach to pod directly from the node
    ///
    /// ## Warning
    /// This method uses the insecure `kubelet_debug` interface. See [`Api::attach`](crate::Api::attach) for the normal interface.
    pub async fn kubelet_node_attach(
        &self,
        kubelet_params: &KubeletDebugParams<'_>,
        container: &str,
        ap: &AttachParams,
    ) -> Result<AttachedProcess> {
        let mut req =
            Request::kubelet_node_attach(kubelet_params, container, ap).map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("kubelet_node_attach");
        let stream = self.connect_exec(req).await?;
        Ok(AttachedProcess::new(stream, ap))
    }

    /// Execute a command in a pod directly from the node
    ///
    /// ## Warning
    /// This method uses the insecure `kubelet_debug` interface. See [`Api::exec`](crate::Api::exec) for the normal interface.
    pub async fn kubelet_node_exec<I, T>(
        &self,
        kubelet_params: &KubeletDebugParams<'_>,
        container: &str,
        command: I,
        ap: &AttachParams,
    ) -> Result<AttachedProcess>
    where
        I: IntoIterator<Item = T> + Debug,
        T: Into<String>,
    {
        let mut req = Request::kubelet_node_exec(kubelet_params, container, command, ap)
            .map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("kubelet_node_exec");
        let stream = self.connect_exec(req).await?;
        Ok(AttachedProcess::new(stream, ap))
    }

    /// Forward ports of a pod directly from the node
    ///
    /// ## Warning
    /// This method uses the insecure `kubelet_debug` interface. See [`Api::portforward`](crate::Api::portforward) for the normal interface.
    pub async fn kubelet_node_portforward(
        &self,
        kubelet_params: &KubeletDebugParams<'_>,
        ports: &[u16],
    ) -> Result<Portforwarder> {
        let mut req =
            Request::kubelet_node_portforward(kubelet_params, ports).map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("kubelet_node_portforward");
        let connection = self.connect_portforward(req).await?;
        Ok(Portforwarder::new(connection.into_stream(), ports))
    }

    /// Stream logs directly from node
    ///
    /// ## Warning
    /// This method uses the insecure `kubelet_debug` interface. See [`Api::log_stream`](crate::Api::log_stream) for the normal interface.
    pub async fn kubelet_node_logs(
        &self,
        kubelet_params: &KubeletDebugParams<'_>,
        container: &str,
        lp: &LogParams,
    ) -> Result<impl AsyncBufRead> {
        let mut req =
            Request::kubelet_node_logs(kubelet_params, container, lp).map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("kubelet_node_log");
        self.request_stream(req).await
    }
}
