pub use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::{
    api::core::v1::ObjectReference,
    apimachinery::pkg::apis::meta::v1::{ManagedFieldsEntry, OwnerReference, Time},
};

use std::{borrow::Cow, collections::BTreeMap};

pub use k8s_openapi::{ClusterResourceScope, NamespaceResourceScope, ResourceScope, SubResourceScope};

use crate::GroupVersionKind;

/// Indicates that a [`Resource`] is of an indeterminate dynamic scope.
pub struct DynamicResourceScope {}
impl ResourceScope for DynamicResourceScope {}

/// An accessor trait for a kubernetes Resource.
///
/// This is for a subset of Kubernetes type that do not end in `List`.
/// These types, using [`ObjectMeta`], SHOULD all have required properties:
/// - `.metadata`
/// - `.metadata.name`
///
/// And these optional properties:
/// - `.metadata.namespace`
/// - `.metadata.resource_version`
///
/// This avoids a bunch of the unnecessary unwrap mechanics for apps.
pub trait Resource {
    /// Type information for types that do not know their resource information at compile time.
    ///
    /// Types that know their metadata at compile time should select `DynamicType = ()`.
    /// Types that require some information at runtime should select `DynamicType`
    /// as type of this information.
    ///
    /// See [`DynamicObject`](crate::dynamic::DynamicObject) for a valid implementation of non-k8s-openapi resources.
    type DynamicType: Send + Sync + 'static;
    /// Type information for the api scope of the resource when known at compile time
    ///
    /// Types from k8s_openapi come with an explicit k8s_openapi::ResourceScope
    /// Dynamic types should select `Scope = DynamicResourceScope`
    type Scope;

    /// Returns kind of this object
    fn kind(dt: &Self::DynamicType) -> Cow<'_, str>;
    /// Returns group of this object
    fn group(dt: &Self::DynamicType) -> Cow<'_, str>;
    /// Returns version of this object
    fn version(dt: &Self::DynamicType) -> Cow<'_, str>;
    /// Returns apiVersion of this object
    fn api_version(dt: &Self::DynamicType) -> Cow<'_, str> {
        api_version_from_group_version(Self::group(dt), Self::version(dt))
    }
    /// Returns the plural name of the kind
    ///
    /// This is known as the resource in apimachinery, we rename it for disambiguation.
    fn plural(dt: &Self::DynamicType) -> Cow<'_, str>;

    /// Generates an object reference for the resource
    fn gvk(dt: &Self::DynamicType) -> GroupVersionKind {
        GroupVersionKind::gvk(&Self::group(dt), &Self::version(dt), &Self::kind(dt))
    }

    /// Creates a url path for http requests for this resource
    fn url_path(dt: &Self::DynamicType, namespace: Option<&str>) -> String {
        let n = if let Some(ns) = namespace {
            format!("namespaces/{ns}/")
        } else {
            "".into()
        };
        let group = Self::group(dt);
        let api_version = Self::api_version(dt);
        let plural = Self::plural(dt);
        format!(
            "/{group}/{api_version}/{namespaces}{plural}",
            group = if group.is_empty() { "api" } else { "apis" },
            api_version = api_version,
            namespaces = n,
            plural = plural
        )
    }

    /// Metadata that all persisted resources must have
    fn meta(&self) -> &ObjectMeta;
    /// Metadata that all persisted resources must have
    fn meta_mut(&mut self) -> &mut ObjectMeta;

    /// Generates an object reference for the resource
    fn object_ref(&self, dt: &Self::DynamicType) -> ObjectReference {
        let meta = self.meta();
        ObjectReference {
            name: meta.name.clone(),
            namespace: meta.namespace.clone(),
            uid: meta.uid.clone(),
            api_version: Some(Self::api_version(dt).to_string()),
            kind: Some(Self::kind(dt).to_string()),
            ..Default::default()
        }
    }

    /// Generates a controller owner reference pointing to this resource
    ///
    /// Note: this returns an `Option`, but for objects populated from the apiserver,
    /// this Option can be safely unwrapped.
    ///
    /// ```
    /// use k8s_openapi::api::core::v1::ConfigMap;
    /// use k8s_openapi::api::core::v1::Pod;
    /// use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    /// use kube_core::Resource;
    ///
    /// let p = Pod::default();
    /// let controller_ref = p.controller_owner_ref(&());
    /// let cm = ConfigMap {
    ///     metadata: ObjectMeta {
    ///         name: Some("pod-configmap".to_string()),
    ///         owner_references: Some(controller_ref.into_iter().collect()),
    ///         ..ObjectMeta::default()
    ///     },
    ///     ..Default::default()
    /// };
    /// ```
    fn controller_owner_ref(&self, dt: &Self::DynamicType) -> Option<OwnerReference> {
        Some(OwnerReference {
            controller: Some(true),
            ..self.owner_ref(dt)?
        })
    }

    /// Generates an owner reference pointing to this resource
    ///
    /// Note: this returns an `Option`, but for objects populated from the apiserver,
    /// this Option can be safely unwrapped.
    ///
    /// ```
    /// use k8s_openapi::api::core::v1::ConfigMap;
    /// use k8s_openapi::api::core::v1::Pod;
    /// use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    /// use kube_core::Resource;
    ///
    /// let p = Pod::default();
    /// let owner_ref = p.owner_ref(&());
    /// let cm = ConfigMap {
    ///     metadata: ObjectMeta {
    ///         name: Some("pod-configmap".to_string()),
    ///         owner_references: Some(owner_ref.into_iter().collect()),
    ///         ..ObjectMeta::default()
    ///     },
    ///     ..Default::default()
    /// };
    /// ```
    fn owner_ref(&self, dt: &Self::DynamicType) -> Option<OwnerReference> {
        let meta = self.meta();
        Some(OwnerReference {
            api_version: Self::api_version(dt).to_string(),
            kind: Self::kind(dt).to_string(),
            name: meta.name.clone()?,
            uid: meta.uid.clone()?,
            ..OwnerReference::default()
        })
    }
}

/// Helper function that creates the `apiVersion` field from the group and version strings.
pub fn api_version_from_group_version<'a>(group: Cow<'a, str>, version: Cow<'a, str>) -> Cow<'a, str> {
    if group.is_empty() {
        return version;
    }

    let mut output = group;
    output.to_mut().push('/');
    output.to_mut().push_str(&version);
    output
}

/// Implement accessor trait for any ObjectMeta-using Kubernetes Resource
impl<K, S> Resource for K
where
    K: k8s_openapi::Metadata<Ty = ObjectMeta>,
    K: k8s_openapi::Resource<Scope = S>,
{
    type DynamicType = ();
    type Scope = S;

    fn kind(_: &()) -> Cow<'_, str> {
        K::KIND.into()
    }

    fn group(_: &()) -> Cow<'_, str> {
        K::GROUP.into()
    }

    fn version(_: &()) -> Cow<'_, str> {
        K::VERSION.into()
    }

    fn api_version(_: &()) -> Cow<'_, str> {
        K::API_VERSION.into()
    }

    fn plural(_: &()) -> Cow<'_, str> {
        K::URL_PATH_SEGMENT.into()
    }

    fn meta(&self) -> &ObjectMeta {
        self.metadata()
    }

    fn meta_mut(&mut self) -> &mut ObjectMeta {
        self.metadata_mut()
    }
}

/// Helper methods for resources.
pub trait ResourceExt: Resource {
    /// Returns the name of the resource, panicking if it is unset
    ///
    /// Only use this function if you know that name is set; for example when
    /// the resource was received from the apiserver (post-admission),
    /// or if you constructed the resource with the name.
    ///
    /// At admission, `.metadata.generateName` can be set instead of name
    /// and in those cases this function can panic.
    ///
    /// Prefer using `.meta().name` or [`name_any`](ResourceExt::name_any)
    /// for the more general cases.
    fn name_unchecked(&self) -> String;

    /// Returns the most useful name identifier available
    ///
    /// This is tries `name`, then `generateName`, and falls back on an empty string when neither is set.
    /// Generally you always have one of the two unless you are creating the object locally.
    ///
    /// This is intended to provide something quick and simple for standard logging purposes.
    /// For more precise use cases, prefer doing your own defaulting.
    /// For true uniqueness, prefer [`uid`](ResourceExt::uid).
    fn name_any(&self) -> String;

    /// The namespace the resource is in
    fn namespace(&self) -> Option<String>;
    /// The resource version
    fn resource_version(&self) -> Option<String>;
    /// Unique ID (if you delete resource and then create a new
    /// resource with the same name, it will have different ID)
    fn uid(&self) -> Option<String>;
    /// Returns the creation timestamp
    ///
    /// This is guaranteed to exist on resources received by the apiserver.
    fn creation_timestamp(&self) -> Option<Time>;
    /// Returns resource labels
    fn labels(&self) -> &BTreeMap<String, String>;
    /// Provides mutable access to the labels
    fn labels_mut(&mut self) -> &mut BTreeMap<String, String>;
    /// Returns resource annotations
    fn annotations(&self) -> &BTreeMap<String, String>;
    /// Provider mutable access to the annotations
    fn annotations_mut(&mut self) -> &mut BTreeMap<String, String>;
    /// Returns resource owner references
    fn owner_references(&self) -> &[OwnerReference];
    /// Provides mutable access to the owner references
    fn owner_references_mut(&mut self) -> &mut Vec<OwnerReference>;
    /// Returns resource finalizers
    fn finalizers(&self) -> &[String];
    /// Provides mutable access to the finalizers
    fn finalizers_mut(&mut self) -> &mut Vec<String>;
    /// Returns managed fields
    fn managed_fields(&self) -> &[ManagedFieldsEntry];
    /// Provides mutable access to managed fields
    fn managed_fields_mut(&mut self) -> &mut Vec<ManagedFieldsEntry>;
}

static EMPTY_MAP: BTreeMap<String, String> = BTreeMap::new();

impl<K: Resource> ResourceExt for K {
    fn name_unchecked(&self) -> String {
        self.meta().name.clone().expect(".metadata.name missing")
    }

    fn name_any(&self) -> String {
        self.meta()
            .name
            .clone()
            .or_else(|| self.meta().generate_name.clone())
            .unwrap_or_default()
    }

    fn namespace(&self) -> Option<String> {
        self.meta().namespace.clone()
    }

    fn resource_version(&self) -> Option<String> {
        self.meta().resource_version.clone()
    }

    fn uid(&self) -> Option<String> {
        self.meta().uid.clone()
    }

    fn creation_timestamp(&self) -> Option<Time> {
        self.meta().creation_timestamp.clone()
    }

    fn labels(&self) -> &BTreeMap<String, String> {
        self.meta().labels.as_ref().unwrap_or(&EMPTY_MAP)
    }

    fn labels_mut(&mut self) -> &mut BTreeMap<String, String> {
        self.meta_mut().labels.get_or_insert_with(BTreeMap::new)
    }

    fn annotations(&self) -> &BTreeMap<String, String> {
        self.meta().annotations.as_ref().unwrap_or(&EMPTY_MAP)
    }

    fn annotations_mut(&mut self) -> &mut BTreeMap<String, String> {
        self.meta_mut().annotations.get_or_insert_with(BTreeMap::new)
    }

    fn owner_references(&self) -> &[OwnerReference] {
        self.meta().owner_references.as_deref().unwrap_or_default()
    }

    fn owner_references_mut(&mut self) -> &mut Vec<OwnerReference> {
        self.meta_mut().owner_references.get_or_insert_with(Vec::new)
    }

    fn finalizers(&self) -> &[String] {
        self.meta().finalizers.as_deref().unwrap_or_default()
    }

    fn finalizers_mut(&mut self) -> &mut Vec<String> {
        self.meta_mut().finalizers.get_or_insert_with(Vec::new)
    }

    fn managed_fields(&self) -> &[ManagedFieldsEntry] {
        self.meta().managed_fields.as_deref().unwrap_or_default()
    }

    fn managed_fields_mut(&mut self) -> &mut Vec<ManagedFieldsEntry> {
        self.meta_mut().managed_fields.get_or_insert_with(Vec::new)
    }
}
