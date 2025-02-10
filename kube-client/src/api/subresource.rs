use futures::AsyncBufRead;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;

use crate::{
    api::{Api, Patch, PatchParams, PostParams},
    Error, Result,
};

use kube_core::response::Status;
pub use kube_core::subresource::{EvictParams, LogParams};

#[cfg(feature = "ws")]
#[cfg_attr(docsrs, doc(cfg(feature = "ws")))]
pub use kube_core::subresource::AttachParams;

pub use k8s_openapi::api::autoscaling::v1::{Scale, ScaleSpec, ScaleStatus};

#[cfg(feature = "ws")] use crate::api::portforward::Portforwarder;
#[cfg(feature = "ws")] use crate::api::remote_command::AttachedProcess;

/// Methods for [scale subresource](https://kubernetes.io/docs/tasks/access-kubernetes-api/custom-resources/custom-resource-definitions/#scale-subresource).
impl<K> Api<K>
where
    K: Clone + DeserializeOwned,
{
    /// Fetch the scale subresource
    pub async fn get_scale(&self, name: &str) -> Result<Scale> {
        let mut req = self
            .request
            .get_subresource("scale", name)
            .map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("get_scale");
        self.client.request::<Scale>(req).await
    }

    /// Update the scale subresource
    pub async fn patch_scale<P: serde::Serialize + Debug>(
        &self,
        name: &str,
        pp: &PatchParams,
        patch: &Patch<P>,
    ) -> Result<Scale> {
        let mut req = self
            .request
            .patch_subresource("scale", name, pp, patch)
            .map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("patch_scale");
        self.client.request::<Scale>(req).await
    }

    /// Replace the scale subresource
    pub async fn replace_scale(&self, name: &str, pp: &PostParams, data: Vec<u8>) -> Result<Scale> {
        let mut req = self
            .request
            .replace_subresource("scale", name, pp, data)
            .map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("replace_scale");
        self.client.request::<Scale>(req).await
    }
}

/// Arbitrary subresources
impl<K> Api<K>
where
    K: Clone + DeserializeOwned + Debug,
{
    /// Display one or many sub-resources.
    pub async fn get_subresource(&self, subresource_name: &str, name: &str) -> Result<K> {
        let mut req = self
            .request
            .get_subresource(subresource_name, name)
            .map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("get_subresource");
        self.client.request::<K>(req).await
    }

    /// Create an instance of the subresource
    pub async fn create_subresource<T>(
        &self,
        subresource_name: &str,
        name: &str,
        pp: &PostParams,
        data: Vec<u8>,
    ) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let mut req = self
            .request
            .create_subresource(subresource_name, name, pp, data)
            .map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("create_subresource");
        self.client.request::<T>(req).await
    }

    /// Patch an instance of the subresource
    pub async fn patch_subresource<P: serde::Serialize + Debug>(
        &self,
        subresource_name: &str,
        name: &str,
        pp: &PatchParams,
        patch: &Patch<P>,
    ) -> Result<K> {
        let mut req = self
            .request
            .patch_subresource(subresource_name, name, pp, patch)
            .map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("patch_subresource");
        self.client.request::<K>(req).await
    }

    /// Replace an instance of the subresource
    pub async fn replace_subresource(
        &self,
        subresource_name: &str,
        name: &str,
        pp: &PostParams,
        data: Vec<u8>,
    ) -> Result<K> {
        let mut req = self
            .request
            .replace_subresource(subresource_name, name, pp, data)
            .map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("replace_subresource");
        self.client.request::<K>(req).await
    }
}

// ----------------------------------------------------------------------------
// Ephemeral containers
// ----------------------------------------------------------------------------

/// Marker trait for objects that support the ephemeral containers sub resource.
///
/// See [`Api::get_ephemeral_containers`] et al.
pub trait Ephemeral {}

impl Ephemeral for k8s_openapi::api::core::v1::Pod {}

impl<K> Api<K>
where
    K: Clone + DeserializeOwned + Ephemeral,
{
    /// Replace the ephemeral containers sub resource entirely.
    ///
    /// This functions in the same way as [`Api::replace`] except only `.spec.ephemeralcontainers` is replaced, everything else is ignored.
    ///
    /// Note that ephemeral containers may **not** be changed or removed once attached to a pod.
    ///
    ///
    /// You way want to patch the underlying resource to gain access to the main container process,
    /// see the [documentation](https://kubernetes.io/docs/tasks/configure-pod-container/share-process-namespace/) for `sharedProcessNamespace`.
    ///
    /// See the Kubernetes [documentation](https://kubernetes.io/docs/concepts/workloads/pods/ephemeral-containers/#what-is-an-ephemeral-container) for more details.
    ///
    /// [`Api::patch_ephemeral_containers`] may be more ergonomic, as you can will avoid having to first fetch the
    /// existing subresources with an approriate merge strategy, see the examples for more details.
    ///
    /// Example of using `replace_ephemeral_containers`:
    ///
    /// ```no_run
    /// use k8s_openapi::api::core::v1::Pod;
    /// use kube::{Api, api::PostParams};
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = kube::Client::try_default().await?;
    /// let pods: Api<Pod> = Api::namespaced(client, "apps");
    /// let pp = PostParams::default();
    ///
    /// // Get pod object with ephemeral containers.
    /// let mut mypod = pods.get_ephemeral_containers("mypod").await?;
    ///
    /// // If there were existing ephemeral containers, we would have to append
    /// // new containers to the list before calling replace_ephemeral_containers.
    /// assert_eq!(mypod.spec.as_mut().unwrap().ephemeral_containers, None);
    ///
    /// // Add an ephemeral container to the pod object.
    /// mypod.spec.as_mut().unwrap().ephemeral_containers = Some(serde_json::from_value(serde_json::json!([
    ///    {
    ///        "name": "myephemeralcontainer",
    ///        "image": "busybox:1.34.1",
    ///        "command": ["sh", "-c", "sleep 20"],
    ///    },
    /// ]))?);
    ///
    /// pods.replace_ephemeral_containers("mypod", &pp, &mypod).await?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub async fn replace_ephemeral_containers(&self, name: &str, pp: &PostParams, data: &K) -> Result<K>
    where
        K: Serialize,
    {
        let mut req = self
            .request
            .replace_subresource(
                "ephemeralcontainers",
                name,
                pp,
                serde_json::to_vec(data).map_err(Error::SerdeError)?,
            )
            .map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("replace_ephemeralcontainers");
        self.client.request::<K>(req).await
    }

    /// Patch the ephemeral containers sub resource
    ///
    /// Any partial object containing the ephemeral containers
    /// sub resource is valid as long as the complete structure
    /// for the object is present, as shown below.
    ///
    /// You way want to patch the underlying resource to gain access to the main container process,
    /// see the [docs](https://kubernetes.io/docs/tasks/configure-pod-container/share-process-namespace/) for `sharedProcessNamespace`.
    ///
    /// Ephemeral containers may **not** be changed or removed once attached to a pod.
    /// Therefore if the chosen merge strategy overwrites the existing ephemeral containers,
    /// you will have to fetch the existing ephemeral containers first.
    /// In order to append your new ephemeral containers to the existing list before patching. See some examples and
    /// discussion related to merge strategies in Kubernetes
    /// [here](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/update-api-object-kubectl-patch/#use-a-json-merge-patch-to-update-a-deployment). The example below uses a strategic merge patch which does not require
    ///
    /// See the `Kubernetes` [documentation](https://kubernetes.io/docs/concepts/workloads/pods/ephemeral-containers/)
    /// for more information about ephemeral containers.
    ///
    ///
    /// Example of using `patch_ephemeral_containers`:
    ///
    /// ```no_run
    /// use kube::api::{Api, PatchParams, Patch};
    /// use k8s_openapi::api::core::v1::Pod;
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = kube::Client::try_default().await?;
    /// let pods: Api<Pod> = Api::namespaced(client, "apps");
    /// let pp = PatchParams::default(); // stratetgic merge patch
    ///
    /// // Note that the strategic merge patch will concatenate the
    /// // lists of ephemeral containers so we avoid having to fetch the
    /// // current list and append to it manually.
    /// let patch = serde_json::json!({
    ///    "spec":{
    ///    "ephemeralContainers": [
    ///    {
    ///        "name": "myephemeralcontainer",
    ///        "image": "busybox:1.34.1",
    ///        "command": ["sh", "-c", "sleep 20"],
    ///    },
    ///    ]
    /// }});
    ///
    /// pods.patch_ephemeral_containers("mypod", &pp, &Patch::Strategic(patch)).await?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub async fn patch_ephemeral_containers<P: serde::Serialize>(
        &self,
        name: &str,
        pp: &PatchParams,
        patch: &Patch<P>,
    ) -> Result<K> {
        let mut req = self
            .request
            .patch_subresource("ephemeralcontainers", name, pp, patch)
            .map_err(Error::BuildRequest)?;

        req.extensions_mut().insert("patch_ephemeralcontainers");
        self.client.request::<K>(req).await
    }

    /// Get the named resource with the ephemeral containers subresource.
    ///
    /// This returns the whole K, with metadata and spec.
    pub async fn get_ephemeral_containers(&self, name: &str) -> Result<K> {
        let mut req = self
            .request
            .get_subresource("ephemeralcontainers", name)
            .map_err(Error::BuildRequest)?;

        req.extensions_mut().insert("get_ephemeralcontainers");
        self.client.request::<K>(req).await
    }
}

// ----------------------------------------------------------------------------

// TODO: Replace examples with owned custom resources. Bad practice to write to owned objects
// These examples work, but the job controller will totally overwrite what we do.
/// Methods for [status subresource](https://kubernetes.io/docs/tasks/access-kubernetes-api/custom-resources/custom-resource-definitions/#status-subresource).
impl<K> Api<K>
where
    K: DeserializeOwned,
{
    /// Get the named resource with a status subresource
    ///
    /// This actually returns the whole K, with metadata, and spec.
    pub async fn get_status(&self, name: &str) -> Result<K> {
        let mut req = self
            .request
            .get_subresource("status", name)
            .map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("get_status");
        self.client.request::<K>(req).await
    }

    /// Patch fields on the status object
    ///
    /// NB: Requires that the resource has a status subresource.
    ///
    /// ```no_run
    /// use kube::api::{Api, PatchParams, Patch};
    /// use k8s_openapi::api::batch::v1::Job;
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = kube::Client::try_default().await?;
    /// let jobs: Api<Job> = Api::namespaced(client, "apps");
    /// let mut j = jobs.get("baz").await?;
    /// let pp = PatchParams::default(); // json merge patch
    /// let data = serde_json::json!({
    ///     "status": {
    ///         "succeeded": 2
    ///     }
    /// });
    /// let o = jobs.patch_status("baz", &pp, &Patch::Merge(data)).await?;
    /// assert_eq!(o.status.unwrap().succeeded, Some(2));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn patch_status<P: serde::Serialize + Debug>(
        &self,
        name: &str,
        pp: &PatchParams,
        patch: &Patch<P>,
    ) -> Result<K> {
        let mut req = self
            .request
            .patch_subresource("status", name, pp, patch)
            .map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("patch_status");
        self.client.request::<K>(req).await
    }

    /// Replace every field on the status object
    ///
    /// This works similarly to the [`Api::replace`] method, but `.spec` is ignored.
    /// You can leave out the `.spec` entirely from the serialized output.
    ///
    /// ```no_run
    /// use kube::api::{Api, PostParams};
    /// use k8s_openapi::api::batch::v1::{Job, JobStatus};
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// #   let client = kube::Client::try_default().await?;
    /// let jobs: Api<Job> = Api::namespaced(client, "apps");
    /// let mut o = jobs.get_status("baz").await?; // retrieve partial object
    /// o.status = Some(JobStatus::default()); // update the job part
    /// let pp = PostParams::default();
    /// let o = jobs.replace_status("baz", &pp, serde_json::to_vec(&o)?).await?;
    /// #    Ok(())
    /// # }
    /// ```
    pub async fn replace_status(&self, name: &str, pp: &PostParams, data: Vec<u8>) -> Result<K> {
        let mut req = self
            .request
            .replace_subresource("status", name, pp, data)
            .map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("replace_status");
        self.client.request::<K>(req).await
    }
}

// ----------------------------------------------------------------------------
// Log subresource
// ----------------------------------------------------------------------------

#[test]
fn log_path() {
    use crate::api::{Request, Resource};
    use k8s_openapi::api::core::v1 as corev1;
    let lp = LogParams {
        container: Some("blah".into()),
        ..LogParams::default()
    };
    let url = corev1::Pod::url_path(&(), Some("ns"));
    let req = Request::new(url).logs("foo", &lp).unwrap();
    assert_eq!(req.uri(), "/api/v1/namespaces/ns/pods/foo/log?&container=blah");
}

/// Marker trait for objects that has logs
///
/// See [`Api::logs`] and [`Api::log_stream`] for usage.
pub trait Log {}

impl Log for k8s_openapi::api::core::v1::Pod {}

impl<K> Api<K>
where
    K: DeserializeOwned + Log,
{
    /// Fetch logs as a string
    pub async fn logs(&self, name: &str, lp: &LogParams) -> Result<String> {
        let mut req = self.request.logs(name, lp).map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("logs");
        self.client.request_text(req).await
    }

    /// Stream the logs via [`AsyncBufRead`].
    ///
    /// Log stream can be processsed using [`AsyncReadExt`](futures::AsyncReadExt)
    /// and [`AsyncBufReadExt`](futures::AsyncBufReadExt).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # use k8s_openapi::api::core::v1::Pod;
    /// # use kube::{api::{Api, LogParams}, Client};
    /// # let client: Client = todo!();
    /// use futures::{AsyncBufReadExt, TryStreamExt};
    ///
    /// let pods: Api<Pod> = Api::default_namespaced(client);
    /// let mut logs = pods
    ///     .log_stream("my-pod", &LogParams::default()).await?
    ///     .lines();
    ///
    /// while let Some(line) = logs.try_next().await? {
    ///     println!("{}", line);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn log_stream(&self, name: &str, lp: &LogParams) -> Result<impl AsyncBufRead> {
        let mut req = self.request.logs(name, lp).map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("log_stream");
        self.client.request_stream(req).await
    }
}

// ----------------------------------------------------------------------------
// Eviction subresource
// ----------------------------------------------------------------------------

#[test]
fn evict_path() {
    use crate::api::{Request, Resource};
    use k8s_openapi::api::core::v1 as corev1;
    let ep = EvictParams::default();
    let url = corev1::Pod::url_path(&(), Some("ns"));
    let req = Request::new(url).evict("foo", &ep).unwrap();
    assert_eq!(req.uri(), "/api/v1/namespaces/ns/pods/foo/eviction?");
}

/// Marker trait for objects that can be evicted
///
/// See [`Api::evic`] for usage
pub trait Evict {}

impl Evict for k8s_openapi::api::core::v1::Pod {}

impl<K> Api<K>
where
    K: DeserializeOwned + Evict,
{
    /// Create an eviction
    pub async fn evict(&self, name: &str, ep: &EvictParams) -> Result<Status> {
        let mut req = self.request.evict(name, ep).map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("evict");
        self.client.request::<Status>(req).await
    }
}

// ----------------------------------------------------------------------------
// Attach subresource
// ----------------------------------------------------------------------------

#[cfg(feature = "ws")]
#[test]
fn attach_path() {
    use crate::api::{Request, Resource};
    use k8s_openapi::api::core::v1 as corev1;
    let ap = AttachParams {
        container: Some("blah".into()),
        ..AttachParams::default()
    };
    let url = corev1::Pod::url_path(&(), Some("ns"));
    let req = Request::new(url).attach("foo", &ap).unwrap();
    assert_eq!(
        req.uri(),
        "/api/v1/namespaces/ns/pods/foo/attach?&stdout=true&stderr=true&container=blah"
    );
}

/// Marker trait for objects that has attach
///
/// See [`Api::attach`] for usage
#[cfg(feature = "ws")]
#[cfg_attr(docsrs, doc(cfg(feature = "ws")))]
pub trait Attach {}

#[cfg(feature = "ws")]
#[cfg_attr(docsrs, doc(cfg(feature = "ws")))]
impl Attach for k8s_openapi::api::core::v1::Pod {}

#[cfg(feature = "ws")]
#[cfg_attr(docsrs, doc(cfg(feature = "ws")))]
impl<K> Api<K>
where
    K: Clone + DeserializeOwned + Attach,
{
    /// Attach to pod
    pub async fn attach(&self, name: &str, ap: &AttachParams) -> Result<AttachedProcess> {
        let mut req = self.request.attach(name, ap).map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("attach");
        let stream = self.client.connect_exec(req).await?;
        Ok(AttachedProcess::new(stream, ap))
    }
}

// ----------------------------------------------------------------------------
// Exec subresource
// ----------------------------------------------------------------------------
#[cfg(feature = "ws")]
#[test]
fn exec_path() {
    use crate::api::{Request, Resource};
    use k8s_openapi::api::core::v1 as corev1;
    let ap = AttachParams {
        container: Some("blah".into()),
        ..AttachParams::default()
    };
    let url = corev1::Pod::url_path(&(), Some("ns"));
    let req = Request::new(url)
        .exec("foo", vec!["echo", "foo", "bar"], &ap)
        .unwrap();
    assert_eq!(
        req.uri(),
        "/api/v1/namespaces/ns/pods/foo/exec?&stdout=true&stderr=true&container=blah&command=echo&command=foo&command=bar"
    );
}

/// Marker trait for objects that has exec
///
/// See [`Api::exec`] for usage.
#[cfg(feature = "ws")]
#[cfg_attr(docsrs, doc(cfg(feature = "ws")))]
pub trait Execute {}

#[cfg(feature = "ws")]
#[cfg_attr(docsrs, doc(cfg(feature = "ws")))]
impl Execute for k8s_openapi::api::core::v1::Pod {}

#[cfg(feature = "ws")]
#[cfg_attr(docsrs, doc(cfg(feature = "ws")))]
impl<K> Api<K>
where
    K: Clone + DeserializeOwned + Execute,
{
    /// Execute a command in a pod
    pub async fn exec<I, T>(&self, name: &str, command: I, ap: &AttachParams) -> Result<AttachedProcess>
    where
        I: IntoIterator<Item = T> + Debug,
        T: Into<String>,
    {
        let mut req = self
            .request
            .exec(name, command, ap)
            .map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("exec");
        let stream = self.client.connect_exec(req).await?;
        Ok(AttachedProcess::new(stream, ap))
    }
}

// ----------------------------------------------------------------------------
// Portforward subresource
// ----------------------------------------------------------------------------
#[cfg(feature = "ws")]
#[test]
fn portforward_path() {
    use crate::api::{Request, Resource};
    use k8s_openapi::api::core::v1 as corev1;
    let url = corev1::Pod::url_path(&(), Some("ns"));
    let req = Request::new(url).portforward("foo", &[80, 1234]).unwrap();
    assert_eq!(
        req.uri(),
        "/api/v1/namespaces/ns/pods/foo/portforward?&ports=80%2C1234"
    );
}

/// Marker trait for objects that has portforward
///
/// See [`Api::portforward`] for usage.
#[cfg(feature = "ws")]
pub trait Portforward {}

#[cfg(feature = "ws")]
impl Portforward for k8s_openapi::api::core::v1::Pod {}

#[cfg(feature = "ws")]
impl<K> Api<K>
where
    K: Clone + DeserializeOwned + Portforward,
{
    /// Forward ports of a pod
    pub async fn portforward(&self, name: &str, ports: &[u16]) -> Result<Portforwarder> {
        let req = self
            .request
            .portforward(name, ports)
            .map_err(Error::BuildRequest)?;
        let connection = self.client.connect_portforward(req).await?;
        Ok(Portforwarder::new(connection.into_stream(), ports))
    }
}
