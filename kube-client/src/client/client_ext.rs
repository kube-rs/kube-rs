use crate::{Client, Error, Result};
use k8s_openapi::{
    api::core::v1::{LocalObjectReference, Namespace as k8sNs, ObjectReference},
    apimachinery::pkg::apis::meta::v1::OwnerReference,
};
use kube_core::{
    object::ObjectList,
    params::{GetParams, ListParams, Patch, PatchParams},
    request::Request,
    ApiResource, ClusterResourceScope, DynamicResourceScope, NamespaceResourceScope, Resource, ResourceExt,
    TypeMeta,
};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;

/// A marker trait to indicate cluster-wide operations are available
trait ClusterScope {}
/// A marker trait to indicate namespace-scoped operations are available
trait NamespaceScope {}

// k8s_openapi scopes get implementations for free
impl ClusterScope for ClusterResourceScope {}
impl NamespaceScope for NamespaceResourceScope {}
// our DynamicResourceScope can masquerade as either
impl NamespaceScope for DynamicResourceScope {}
impl ClusterScope for DynamicResourceScope {}

/// How to get the url for a collection
///
/// Pick one of `kube::client::Cluster` or `kube::client::Namespace`.
pub trait CollectionUrl<K> {
    fn url_path(&self) -> String;
}

/// How to get the url for an object
///
/// Pick one of `kube::client::Cluster` or `kube::client::Namespace`.
pub trait ObjectUrl<K> {
    fn url_path(&self) -> String;
}

/// Marker type for cluster level queries
#[derive(Debug, Clone)]
pub struct Cluster;
/// Namespace newtype for namespace level queries
///
/// You can create this directly, or convert `From` a `String` / `&str`, or `TryFrom` an `k8s_openapi::api::core::v1::Namespace`
#[derive(Debug, Clone)]
pub struct Namespace(String);

/// Referenced object name resolution
pub trait ObjectRef<K>: ObjectUrl<K> {
    fn name(&self) -> Option<&str>;
}

/// Reference resolver for a specified namespace
pub trait NamespacedRef<K> {
    /// Resolve reference in the provided namespace
    fn within(&self, namespace: impl Into<Option<String>>) -> impl ObjectRef<K>;
}

impl<K> ObjectUrl<K> for ObjectReference
where
    K: Resource,
{
    fn url_path(&self) -> String {
        url_path(
            &ApiResource::from_gvk(&self.clone().into()),
            self.namespace.clone(),
        )
    }
}

impl<K> ObjectRef<K> for ObjectReference
where
    K: Resource,
{
    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}

impl<K> NamespacedRef<K> for ObjectReference
where
    K: Resource,
    K::Scope: NamespaceScope,
{
    fn within(&self, namespace: impl Into<Option<String>>) -> impl ObjectRef<K> {
        Self {
            namespace: namespace.into(),
            ..self.clone()
        }
    }
}

impl<K> ObjectUrl<K> for OwnerReference
where
    K: Resource,
    K::Scope: ClusterScope,
{
    fn url_path(&self) -> String {
        url_path(&ApiResource::from_gvk(&self.clone().into()), None)
    }
}

impl<K> ObjectRef<K> for OwnerReference
where
    K: Resource,
    K::Scope: ClusterScope,
{
    fn name(&self) -> Option<&str> {
        self.name.as_str().into()
    }
}

impl<K> NamespacedRef<K> for OwnerReference
where
    K: Resource,
    K::Scope: NamespaceScope,
{
    fn within(&self, namespace: impl Into<Option<String>>) -> impl ObjectRef<K> {
        ObjectReference {
            api_version: self.api_version.clone().into(),
            namespace: namespace.into(),
            name: self.name.clone().into(),
            uid: self.uid.clone().into(),
            kind: self.kind.clone().into(),
            ..Default::default()
        }
    }
}

impl<K> NamespacedRef<K> for LocalObjectReference
where
    K: Resource,
    K::DynamicType: Default,
    K::Scope: NamespaceScope,
{
    fn within(&self, namespace: impl Into<Option<String>>) -> impl ObjectRef<K> {
        let dt = Default::default();
        ObjectReference {
            api_version: K::api_version(&dt).to_string().into(),
            namespace: namespace.into(),
            name: Some(self.name.clone()),
            kind: K::kind(&dt).to_string().into(),
            ..Default::default()
        }
    }
}

/// Scopes for `unstable-client` [`Client#impl-Client`] extension methods
pub mod scope {
    pub use super::{Cluster, Namespace, NamespacedRef};
}

// All objects can be listed cluster-wide
impl<K> CollectionUrl<K> for Cluster
where
    K: Resource,
    K::DynamicType: Default,
{
    fn url_path(&self) -> String {
        K::url_path(&K::DynamicType::default(), None)
    }
}

// Only cluster-scoped objects can be named globally
impl<K> ObjectUrl<K> for Cluster
where
    K: Resource,
    K::DynamicType: Default,
    K::Scope: ClusterScope,
{
    fn url_path(&self) -> String {
        K::url_path(&K::DynamicType::default(), None)
    }
}

// Only namespaced objects can be accessed via namespace
impl<K> CollectionUrl<K> for Namespace
where
    K: Resource,
    K::DynamicType: Default,
    K::Scope: NamespaceScope,
{
    fn url_path(&self) -> String {
        K::url_path(&K::DynamicType::default(), Some(&self.0))
    }
}

impl<K> ObjectUrl<K> for Namespace
where
    K: Resource,
    K::DynamicType: Default,
    K::Scope: NamespaceScope,
{
    fn url_path(&self) -> String {
        K::url_path(&K::DynamicType::default(), Some(&self.0))
    }
}

// can be created from a complete native object
impl TryFrom<&k8sNs> for Namespace {
    type Error = NamespaceError;

    fn try_from(ns: &k8sNs) -> Result<Namespace, Self::Error> {
        if let Some(n) = &ns.meta().name {
            Ok(Namespace(n.to_owned()))
        } else {
            Err(NamespaceError::MissingName)
        }
    }
}
// and from literals + owned strings
impl From<&str> for Namespace {
    fn from(ns: &str) -> Namespace {
        Namespace(ns.to_owned())
    }
}
impl From<String> for Namespace {
    fn from(ns: String) -> Namespace {
        Namespace(ns)
    }
}

#[derive(thiserror::Error, Debug)]
/// Failures to infer a namespace
pub enum NamespaceError {
    /// MissingName
    #[error("Missing Namespace Name")]
    MissingName,
}

#[derive(Serialize, Clone, Debug)]
/// ApplyObject allows to wrap an object into Patch::Apply compatible structure,
/// with populated TypeMeta.
pub struct ApplyObject<R: Serialize> {
    /// Contains the API version and type of the request.
    #[serde(flatten)]
    pub types: TypeMeta,
    /// Contains the object data.
    #[serde(flatten)]
    pub data: R,
}

/// Generic client extensions for the `unstable-client` feature
///
/// These methods allow users to query across a wide-array of resources without needing
/// to explicitly create an [`Api`](crate::Api) for each one of them.
///
/// ## Usage
/// 1. Create a [`Client`]
/// 2. Specify the [`scope`] you are querying at via [`Cluster`] or [`Namespace`] as args
/// 3. Specify the resource type you are using for serialization (e.g. a top level k8s-openapi type)
///
/// ## Example
///
/// ```no_run
/// # use k8s_openapi::api::core::v1::{Pod, Service};
/// # use kube::client::scope::{Namespace, Cluster};
/// # use kube::prelude::*;
/// # use kube::api::ListParams;
/// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
/// # let client: kube::Client = todo!();
/// let lp = ListParams::default();
/// // List at Cluster level for Pod resource:
/// for pod in client.list::<Pod>(&lp, &Cluster).await? {
///     println!("Found pod {} in {}", pod.name_any(), pod.namespace().unwrap());
/// }
/// // Namespaced Get for Service resource:
/// let svc = client.get::<Service>("kubernetes", &Namespace::from("default")).await?;
/// assert_eq!(svc.name_unchecked(), "kubernetes");
/// # Ok(())
/// # }
/// ```
impl Client {
    /// Get a single instance of a `Resource` implementing type `K` at the specified scope.
    ///
    /// ```no_run
    /// # use k8s_openapi::api::rbac::v1::ClusterRole;
    /// # use k8s_openapi::api::core::v1::Service;
    /// # use kube::client::scope::{Namespace, Cluster};
    /// # use kube::prelude::*;
    /// # use kube::api::GetParams;
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    /// let cr = client.get::<ClusterRole>("cluster-admin", &Cluster).await?;
    /// assert_eq!(cr.name_unchecked(), "cluster-admin");
    /// let svc = client.get::<Service>("kubernetes", &Namespace::from("default")).await?;
    /// assert_eq!(svc.name_unchecked(), "kubernetes");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get<K>(&self, name: &str, scope: &impl ObjectUrl<K>) -> Result<K>
    where
        K: Resource + Serialize + DeserializeOwned + Clone + Debug,
        <K as Resource>::DynamicType: Default,
    {
        let mut req = Request::new(scope.url_path())
            .get(name, &GetParams::default())
            .map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("get");
        self.request::<K>(req).await
    }

    /// Fetch a single instance of a `Resource` from a provided object reference.
    ///
    /// ```no_run
    /// # use k8s_openapi::api::rbac::v1::ClusterRole;
    /// # use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
    /// # use k8s_openapi::api::core::v1::{ObjectReference, LocalObjectReference};
    /// # use k8s_openapi::api::core::v1::{Node, Pod, Service, Secret};
    /// # use kube::client::scope::NamespacedRef;
    /// # use kube::api::GetParams;
    /// # use kube::prelude::*;
    /// # use kube::api::DynamicObject;
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    /// // cluster scoped
    /// let cr: ClusterRole = todo!();
    /// let cr: ClusterRole = client.fetch(&cr.object_ref(&())).await?;
    /// assert_eq!(cr.name_unchecked(), "cluster-admin");
    /// // namespace scoped
    /// let svc: Service = todo!();
    /// let svc: Service = client.fetch(&svc.object_ref(&())).await?;
    /// assert_eq!(svc.name_unchecked(), "kubernetes");
    /// // Fetch an owner of the resource
    /// let pod: Pod = todo!();
    /// let owner = pod
    ///     .owner_references()
    ///     .to_vec()
    ///     .into_iter()
    ///     .find(|r| r.kind == Node::kind(&()))
    ///     .ok_or("Not Found")?;
    /// let node: Node = client.fetch(&owner).await?;
    /// // Namespace scoped objects require namespace
    /// let pod: Pod = client.fetch(&owner.within("ns".to_string())).await?;
    /// // Fetch dynamic object to resolve type later
    /// let dynamic: DynamicObject = client.fetch(&owner.within("ns".to_string())).await?;
    /// // Fetch using local object reference
    /// let secret_ref = pod
    ///     .spec
    ///     .unwrap_or_default()
    ///     .image_pull_secrets
    ///     .unwrap_or_default()
    ///     .get(0)
    ///     .unwrap_or(&LocalObjectReference{name: "pull_secret".into()});
    /// let secret: Secret = client.fetch(&secret_ref.within(pod.namespace())).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn fetch<K>(&self, reference: &impl ObjectRef<K>) -> Result<K>
    where
        K: Resource + Serialize + DeserializeOwned + Clone + Debug,
    {
        let mut req = Request::new(reference.url_path())
            .get(
                reference
                    .name()
                    .ok_or(Error::RefResolve("Reference is empty".to_string()))?,
                &GetParams::default(),
            )
            .map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("get");
        self.request::<K>(req).await
    }

    /// List instances of a `Resource` implementing type `K` at the specified scope.
    ///
    /// ```no_run
    /// # use k8s_openapi::api::core::v1::Pod;
    /// # use k8s_openapi::api::core::v1::Service;
    /// # use kube::client::scope::{Namespace, Cluster};
    /// # use kube::prelude::*;
    /// # use kube::api::ListParams;
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    /// let lp = ListParams::default();
    /// for pod in client.list::<Pod>(&lp, &Cluster).await? {
    ///     println!("Found pod {} in {}", pod.name_any(), pod.namespace().unwrap());
    /// }
    /// for svc in client.list::<Service>(&lp, &Namespace::from("default")).await? {
    ///     println!("Found service {}", svc.name_any());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list<K>(&self, lp: &ListParams, scope: &impl CollectionUrl<K>) -> Result<ObjectList<K>>
    where
        K: Resource + Serialize + DeserializeOwned + Clone + Debug,
        <K as Resource>::DynamicType: Default,
    {
        let mut req = Request::new(scope.url_path())
            .list(lp)
            .map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("list");
        self.request::<ObjectList<K>>(req).await
    }

    /// Perform apply patch on the provided `Resource` implementing type `K`
    ///
    /// ```no_run
    /// # use k8s_openapi::api::core::v1::Pod;
    /// # use k8s_openapi::api::core::v1::Service;
    /// # use kube::client::scope::Namespace;
    /// # use kube::prelude::*;
    /// # use kube::api::{PatchParams, Patch};
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    /// let pod: Pod = client.get("some_pod", &Namespace::from("default")).await?;
    /// let pp = &PatchParams::apply("controller").force();
    /// // Perform an apply patch on the resource
    /// client.apply(&pod, pp).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn apply<K>(&self, resource: &K, pp: &PatchParams) -> Result<K>
    where
        K: ResourceExt + Serialize + DeserializeOwned + Clone + Debug,
        <K as Resource>::DynamicType: Default,
    {
        let meta = resource.meta();
        let name = meta.name.as_ref().ok_or(Error::NameResolve)?;
        let url = K::url_path(&Default::default(), meta.namespace.as_deref());
        let req = Request::new(url);

        let apply = ApplyObject::<K> {
            types: TypeMeta::resource::<K>(),
            data: {
                let mut resource = resource.clone();
                resource.meta_mut().managed_fields = None;
                resource
            },
        };
        let req = req
            .patch(name, pp, &Patch::Apply(apply))
            .map_err(Error::BuildRequest)?;
        self.request::<K>(req).await
    }

    /// Perform apply patch on the provided `Resource` status, implementing type `K`
    ///
    /// ```no_run
    /// # use k8s_openapi::api::core::v1::Pod;
    /// # use k8s_openapi::api::core::v1::Service;
    /// # use kube::client::scope::Namespace;
    /// # use kube::prelude::*;
    /// # use kube::api::{PatchParams, Patch};
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: kube::Client = todo!();
    /// let pod: Pod = client.get("some_pod", &Namespace::from("default")).await?;
    /// let pp = &PatchParams::apply("controller").force();
    /// // Perform an apply patch on the resource status
    /// client.apply_status(&pod, pp).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn apply_status<K>(&self, resource: &K, pp: &PatchParams) -> Result<K>
    where
        K: ResourceExt + Serialize + DeserializeOwned + Clone + Debug,
        <K as Resource>::DynamicType: Default,
    {
        let meta = resource.meta();
        let name = meta.name.as_ref().ok_or(Error::NameResolve)?;
        let url = K::url_path(&Default::default(), meta.namespace.as_deref());
        let req = Request::new(url);

        let apply = ApplyObject::<K> {
            types: TypeMeta::resource::<K>(),
            data: {
                let mut resource = resource.clone();
                resource.meta_mut().managed_fields = None;
                resource
            },
        };
        let req = req
            .patch_subresource("status", name, pp, &Patch::Apply(apply))
            .map_err(Error::BuildRequest)?;
        self.request::<K>(req).await
    }
}

// Resource url_path resolver
fn url_path(r: &ApiResource, namespace: Option<String>) -> String {
    let n = if let Some(ns) = namespace {
        format!("namespaces/{ns}/")
    } else {
        "".into()
    };
    format!(
        "/{group}/{api_version}/{namespaces}{plural}",
        group = if r.group.is_empty() { "api" } else { "apis" },
        api_version = r.api_version,
        namespaces = n,
        plural = r.plural
    )
}

#[cfg(test)]
mod test {
    use crate::{
        client::{
            client_ext::NamespacedRef as _,
            scope::{Cluster, Namespace},
        },
        Client,
    };

    use super::ListParams;
    use k8s_openapi::api::core::v1::LocalObjectReference;
    use kube_core::{DynamicObject, Resource as _, ResourceExt as _};

    #[tokio::test]
    #[ignore = "needs cluster (will list/get namespaces, pods, jobs, svcs, clusterroles)"]
    async fn client_ext_list_get_pods_svcs() -> Result<(), Box<dyn std::error::Error>> {
        use k8s_openapi::api::{
            batch::v1::Job,
            core::v1::{Namespace as k8sNs, Pod, Service},
            rbac::v1::ClusterRole,
        };

        let client = Client::try_default().await?;
        let lp = ListParams::default();
        // cluster-scoped list
        for ns in client.list::<k8sNs>(&lp, &Cluster).await? {
            // namespaced list
            for p in client.list::<Pod>(&lp, &Namespace::try_from(&ns)?).await? {
                println!("Found pod {} in {}", p.name_any(), ns.name_any());
            }
        }
        // across-namespace list
        for j in client.list::<Job>(&lp, &Cluster).await? {
            println!("Found job {} in {}", j.name_any(), j.namespace().unwrap());
        }
        // namespaced get
        let default: Namespace = "default".into();
        let svc = client.get::<Service>("kubernetes", &default).await?;
        assert_eq!(svc.name_unchecked(), "kubernetes");
        // global get
        let ca = client.get::<ClusterRole>("cluster-admin", &Cluster).await?;
        assert_eq!(ca.name_unchecked(), "cluster-admin");

        Ok(())
    }

    #[tokio::test]
    #[ignore = "needs cluster (will get svcs, clusterroles, pods, nodes)"]
    async fn client_ext_fetch_ref_pods_svcs() -> Result<(), Box<dyn std::error::Error>> {
        use k8s_openapi::api::{
            core::v1::{Node, ObjectReference, Pod, Service},
            rbac::v1::ClusterRole,
        };

        let client = Client::try_default().await?;
        // namespaced fetch
        let svc: Service = client
            .fetch(&ObjectReference {
                kind: Some(Service::kind(&()).into()),
                api_version: Some(Service::api_version(&()).into()),
                name: Some("kubernetes".into()),
                namespace: Some("default".into()),
                ..Default::default()
            })
            .await?;
        assert_eq!(svc.name_unchecked(), "kubernetes");
        // global fetch
        let ca: ClusterRole = client
            .fetch(&ObjectReference {
                kind: Some(ClusterRole::kind(&()).into()),
                api_version: Some(ClusterRole::api_version(&()).into()),
                name: Some("cluster-admin".into()),
                ..Default::default()
            })
            .await?;
        assert_eq!(ca.name_unchecked(), "cluster-admin");
        // namespaced fetch untyped
        let svc: DynamicObject = client.fetch(&svc.object_ref(&())).await?;
        assert_eq!(svc.name_unchecked(), "kubernetes");
        // global fetch untyped
        let ca: DynamicObject = client.fetch(&ca.object_ref(&())).await?;
        assert_eq!(ca.name_unchecked(), "cluster-admin");

        // Fetch using local object reference
        let svc: Service = client
            .fetch(
                &LocalObjectReference {
                    name: svc.name_any().into(),
                }
                .within(svc.namespace()),
            )
            .await?;
        assert_eq!(svc.name_unchecked(), "kubernetes");

        let kube_system: Namespace = "kube-system".into();
        for pod in client
            .list::<Pod>(
                &ListParams::default().labels("component=kube-apiserver"),
                &kube_system,
            )
            .await?
        {
            let owner = pod
                .owner_references()
                .iter()
                .find(|r| r.kind == Node::kind(&()))
                .ok_or("Not found")?;
            let _: Node = client.fetch(owner).await?;
        }

        Ok(())
    }

    #[tokio::test]
    #[ignore = "needs cluster (will get svcs, clusterroles, pods, nodes)"]
    async fn fetch_fails() -> Result<(), Box<dyn std::error::Error>> {
        use crate::error::Error;
        use k8s_openapi::api::core::v1::{ObjectReference, Pod, Service};

        let client = Client::try_default().await?;
        // namespaced fetch
        let svc: Service = client
            .fetch(&ObjectReference {
                kind: Some(Service::kind(&()).into()),
                api_version: Some(Service::api_version(&()).into()),
                name: Some("kubernetes".into()),
                namespace: Some("default".into()),
                ..Default::default()
            })
            .await?;
        let err = client.fetch::<Pod>(&svc.object_ref(&())).await.unwrap_err();
        assert!(matches!(err, Error::SerdeError(_)));
        assert_eq!(err.to_string(), "Error deserializing response: invalid value: string \"Service\", expected Pod at line 1 column 17".to_string());

        let obj: DynamicObject = client.fetch(&svc.object_ref(&())).await?;
        let err = obj.try_parse::<Pod>().unwrap_err();
        assert_eq!(err.to_string(), "failed to parse this DynamicObject into a Resource: invalid value: string \"Service\", expected Pod".to_string());

        Ok(())
    }
}
