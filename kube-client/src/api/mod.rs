//! API helpers for structured interaction with the Kubernetes API

mod core_methods;
#[cfg(feature = "ws")] mod remote_command;
use std::fmt::Debug;

#[cfg(feature = "ws")] pub use remote_command::{AttachedProcess, TerminalSize};
#[cfg(feature = "ws")] mod portforward;
#[cfg(feature = "ws")] pub use portforward::Portforwarder;

mod subresource;
#[cfg(feature = "ws")]
#[cfg_attr(docsrs, doc(cfg(feature = "ws")))]
pub use subresource::{Attach, AttachParams, Ephemeral, Execute, Portforward};
pub use subresource::{Evict, EvictParams, Log, LogParams, ScaleSpec, ScaleStatus};

mod util;

pub mod entry;

// Re-exports from kube-core
#[cfg(feature = "admission")]
#[cfg_attr(docsrs, doc(cfg(feature = "admission")))]
pub use kube_core::admission;
pub(crate) use kube_core::params;
pub use kube_core::{
    dynamic::{ApiResource, DynamicObject},
    gvk::{GroupVersionKind, GroupVersionResource},
    metadata::{ListMeta, ObjectMeta, PartialObjectMeta, PartialObjectMetaExt, TypeMeta},
    object::{NotUsed, Object, ObjectList},
    request::Request,
    watch::WatchEvent,
    Resource, ResourceExt,
};
use kube_core::{DynamicResourceScope, NamespaceResourceScope};
pub use params::{
    DeleteParams, GetParams, ListParams, Patch, PatchParams, PostParams, Preconditions, PropagationPolicy,
    ValidationDirective, VersionMatch, WatchParams,
};

use crate::Client;
/// The generic Api abstraction
///
/// This abstracts over a [`Request`] and a type `K` so that
/// we get automatic serialization/deserialization on the api calls
/// implemented by the dynamic [`Resource`].
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
#[derive(Clone)]
pub struct Api<K> {
    /// The request builder object with its resource dependent url
    pub(crate) request: Request,
    /// The client to use (from this library)
    pub(crate) client: Client,
    namespace: Option<String>,
    /// Note: Using `iter::Empty` over `PhantomData`, because we never actually keep any
    /// `K` objects, so `Empty` better models our constraints (in particular, `Empty<K>`
    /// is `Send`, even if `K` may not be).
    pub(crate) _phantom: std::iter::Empty<K>,
}

/// Api constructors for Resource implementors with custom DynamicTypes
///
/// This generally means resources created via [`DynamicObject`](crate::api::DynamicObject).
impl<K: Resource> Api<K> {
    /// Cluster level resources, or resources viewed across all namespaces
    ///
    /// This function accepts `K::DynamicType` so it can be used with dynamic resources.
    ///
    /// # Warning
    ///
    /// This variant **can only `list` and `watch` namespaced resources** and is commonly used with a `watcher`.
    /// If you need to create/patch/replace/get on a namespaced resource, you need a separate `Api::namespaced`.
    pub fn all_with(client: Client, dyntype: &K::DynamicType) -> Self {
        let url = K::url_path(dyntype, None);
        Self {
            client,
            request: Request::new(url),
            namespace: None,
            _phantom: std::iter::empty(),
        }
    }

    /// Namespaced resource within a given namespace
    ///
    /// This function accepts `K::DynamicType` so it can be used with dynamic resources.
    pub fn namespaced_with(client: Client, ns: &str, dyntype: &K::DynamicType) -> Self
    where
        K: Resource<Scope = DynamicResourceScope>,
    {
        // TODO: inspect dyntype scope to verify somehow?
        let url = K::url_path(dyntype, Some(ns));
        Self {
            client,
            request: Request::new(url),
            namespace: Some(ns.to_string()),
            _phantom: std::iter::empty(),
        }
    }

    /// Namespaced resource within the default namespace
    ///
    /// This function accepts `K::DynamicType` so it can be used with dynamic resources.
    ///
    /// The namespace is either configured on `context` in the kubeconfig
    /// or falls back to `default` when running locally, and it's using the service account's
    /// namespace when deployed in-cluster.
    pub fn default_namespaced_with(client: Client, dyntype: &K::DynamicType) -> Self
    where
        K: Resource<Scope = DynamicResourceScope>,
    {
        let ns = client.default_namespace().to_string();
        Self::namespaced_with(client, &ns, dyntype)
    }

    /// Consume self and return the [`Client`]
    pub fn into_client(self) -> Client {
        self.into()
    }

    /// Return a reference to the current resource url path
    pub fn resource_url(&self) -> &str {
        &self.request.url_path
    }
}

/// Api constructors for Resource implementors with Default DynamicTypes
///
/// This generally means structs implementing `k8s_openapi::Resource`.
impl<K: Resource> Api<K>
where
    <K as Resource>::DynamicType: Default,
{
    /// Cluster level resources, or resources viewed across all namespaces
    ///
    /// Namespace scoped resource allowing querying across all namespaces:
    ///
    /// ```no_run
    /// # use kube::{Api, Client};
    /// # let client: Client = todo!();
    /// use k8s_openapi::api::core::v1::Pod;
    /// let api: Api<Pod> = Api::all(client);
    /// ```
    ///
    /// Cluster scoped resources also use this entrypoint:
    ///
    /// ```no_run
    /// # use kube::{Api, Client};
    /// # let client: Client = todo!();
    /// use k8s_openapi::api::core::v1::Node;
    /// let api: Api<Node> = Api::all(client);
    /// ```
    ///
    /// # Warning
    ///
    /// This variant **can only `list` and `watch` namespaced resources** and is commonly used with a `watcher`.
    /// If you need to create/patch/replace/get on a namespaced resource, you need a separate `Api::namespaced`.
    pub fn all(client: Client) -> Self {
        Self::all_with(client, &K::DynamicType::default())
    }

    /// Namespaced resource within a given namespace
    ///
    /// ```no_run
    /// # use kube::{Api, Client};
    /// # let client: Client = todo!();
    /// use k8s_openapi::api::core::v1::Pod;
    /// let api: Api<Pod> = Api::namespaced(client, "default");
    /// ```
    ///
    /// This will ONLY work on namespaced resources as set by `Scope`:
    ///
    /// ```compile_fail
    /// # use kube::{Api, Client};
    /// # let client: Client = todo!();
    /// use k8s_openapi::api::core::v1::Node;
    /// let api: Api<Node> = Api::namespaced(client, "default"); // resource not namespaced!
    /// ```
    ///
    /// For dynamic type information, use [`Api::namespaced_with`] variants.
    pub fn namespaced(client: Client, ns: &str) -> Self
    where
        K: Resource<Scope = NamespaceResourceScope>,
    {
        let dyntype = K::DynamicType::default();
        let url = K::url_path(&dyntype, Some(ns));
        Self {
            client,
            request: Request::new(url),
            namespace: Some(ns.to_string()),
            _phantom: std::iter::empty(),
        }
    }

    /// Namespaced resource within the default namespace
    ///
    /// The namespace is either configured on `context` in the kubeconfig
    /// or falls back to `default` when running locally, and it's using the service account's
    /// namespace when deployed in-cluster.
    ///
    /// ```no_run
    /// # use kube::{Api, Client};
    /// # let client: Client = todo!();
    /// use k8s_openapi::api::core::v1::Pod;
    /// let api: Api<Pod> = Api::default_namespaced(client);
    /// ```
    ///
    /// This will ONLY work on namespaced resources as set by `Scope`:
    ///
    /// ```compile_fail
    /// # use kube::{Api, Client};
    /// # let client: Client = todo!();
    /// use k8s_openapi::api::core::v1::Node;
    /// let api: Api<Node> = Api::default_namespaced(client); // resource not namespaced!
    /// ```
    pub fn default_namespaced(client: Client) -> Self
    where
        K: Resource<Scope = NamespaceResourceScope>,
    {
        let ns = client.default_namespace().to_string();
        Self::namespaced(client, &ns)
    }
}

impl<K> From<Api<K>> for Client {
    fn from(api: Api<K>) -> Self {
        api.client
    }
}

impl<K> Debug for Api<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Intentionally destructuring, to cause compile errors when new fields are added
        let Self {
            request,
            client: _,
            namespace,
            _phantom,
        } = self;
        f.debug_struct("Api")
            .field("request", &request)
            .field("client", &"...")
            .field("namespace", &namespace)
            .finish()
    }
}

/// Sanity test on scope restrictions
#[cfg(test)]
mod test {
    use crate::{client::Body, Api, Client};
    use k8s_openapi::api::core::v1 as corev1;

    use http::{Request, Response};
    use tower_test::mock;

    #[tokio::test]
    async fn scopes_should_allow_correct_interface() {
        let (mock_service, _handle) = mock::pair::<Request<Body>, Response<Body>>();
        let client = Client::new(mock_service, "default", None);

        let _: Api<corev1::Node> = Api::all(client.clone());
        let _: Api<corev1::Pod> = Api::default_namespaced(client.clone());
        let _: Api<corev1::PersistentVolume> = Api::all(client.clone());
        let _: Api<corev1::ConfigMap> = Api::namespaced(client, "default");
    }
}
