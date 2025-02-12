//! Metadata structs used in traits, lists, and dynamic objects.
use std::{borrow::Cow, marker::PhantomData};

pub use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ListMeta, ObjectMeta};
use serde::{Deserialize, Serialize};

use crate::{DynamicObject, Resource};

/// Type information that is flattened into every kubernetes object
#[derive(Deserialize, Serialize, Clone, Default, Debug, Eq, PartialEq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct TypeMeta {
    /// The version of the API
    pub api_version: String,

    /// The name of the API
    pub kind: String,
}

impl TypeMeta {
    /// Construct a new `TypeMeta` for the object list from the given resource.
    ///
    /// ```
    /// # use k8s_openapi::api::core::v1::Pod;
    /// # use kube_core::TypeMeta;
    ///
    /// let type_meta = TypeMeta::list::<Pod>();
    /// assert_eq!(type_meta.kind, "PodList");
    /// assert_eq!(type_meta.api_version, "v1");
    /// ```
    pub fn list<K: Resource<DynamicType = impl Default>>() -> Self {
        TypeMeta {
            api_version: K::api_version(&Default::default()).into(),
            kind: K::kind(&Default::default()).to_string() + "List",
        }
    }

    /// Construct a new `TypeMeta` for the object from the given resource.
    ///
    /// ```
    /// # use k8s_openapi::api::core::v1::Pod;
    /// # use kube_core::TypeMeta;
    ///
    /// let type_meta = TypeMeta::resource::<Pod>();
    /// assert_eq!(type_meta.kind, "Pod");
    /// assert_eq!(type_meta.api_version, "v1");
    /// ```
    pub fn resource<K: Resource<DynamicType = impl Default>>() -> Self {
        TypeMeta {
            api_version: K::api_version(&Default::default()).into(),
            kind: K::kind(&Default::default()).into(),
        }
    }
}

/// A generic representation of any object with `ObjectMeta`.
///
/// It allows clients to get access to a particular `ObjectMeta`
/// schema without knowing the details of the version.
///
/// See the [`PartialObjectMetaExt`] trait for how to construct one safely.
#[derive(Deserialize, Serialize, Clone, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PartialObjectMeta<K = DynamicObject> {
    /// The type fields, not always present
    #[serde(flatten, default)]
    pub types: Option<TypeMeta>,
    /// Standard object's metadata
    #[serde(default)]
    pub metadata: ObjectMeta,
    /// Type information for static dispatch
    #[serde(skip, default)]
    pub _phantom: PhantomData<K>,
}

mod private {
    pub trait Sealed {}
    impl Sealed for super::ObjectMeta {}
}
/// Helper trait for converting `ObjectMeta` into useful `PartialObjectMeta` variants
pub trait PartialObjectMetaExt: private::Sealed {
    /// Convert `ObjectMeta` into a Patch-serializable `PartialObjectMeta`
    ///
    /// This object can be passed to `Patch::Apply` and used with `Api::patch_metadata`,
    /// for an `Api<K>` using the underlying types `TypeMeta`:
    ///
    /// ```
    /// # use k8s_openapi::api::core::v1::Pod;
    /// # use kube::core::{ObjectMeta, PartialObjectMetaExt, ResourceExt};
    /// let partial = ObjectMeta {
    ///     labels: Some([("key".to_string(), "value".to_string())].into()),
    ///     ..Default::default()
    /// }.into_request_partial::<Pod>();
    ///
    /// // request partials are generally closer to patches than fully valid resources:
    /// assert_eq!(partial.name_any(), "");
    ///
    /// // typemeta is re-used from K:
    /// assert_eq!(partial.types.unwrap().kind, "Pod");
    /// ```
    fn into_request_partial<K: Resource<DynamicType = ()>>(self) -> PartialObjectMeta<K>;

    /// Convert `ObjectMeta` into a response object for a specific `Resource`
    ///
    /// This object emulates a response object and **cannot** be used in request bodies
    /// because it contains erased `TypeMeta` (and the apiserver is doing the erasing).
    ///
    /// This method is **mostly useful for unit testing** behaviour.
    ///
    /// ```
    /// # use k8s_openapi::api::apps::v1::Deployment;
    /// # use kube::core::{ObjectMeta, PartialObjectMetaExt, ResourceExt};
    /// let partial = ObjectMeta {
    ///     name: Some("my-deploy".to_string()),
    ///     namespace: Some("default".to_string()),
    ///     ..Default::default()
    /// }.into_response_partial::<Deployment>();
    ///
    /// assert_eq!(partial.name_any(), "my-deploy");
    /// assert_eq!(partial.types.unwrap().kind, "PartialObjectMetadata"); // NB: Pod erased
    /// ```
    fn into_response_partial<K>(self) -> PartialObjectMeta<K>;
}

impl PartialObjectMetaExt for ObjectMeta {
    fn into_request_partial<K: Resource<DynamicType = ()>>(self) -> PartialObjectMeta<K> {
        PartialObjectMeta {
            types: Some(TypeMeta {
                api_version: K::api_version(&()).into(),
                kind: K::kind(&()).into(),
            }),
            metadata: self,
            _phantom: PhantomData,
        }
    }

    fn into_response_partial<K>(self) -> PartialObjectMeta<K> {
        PartialObjectMeta {
            types: Some(TypeMeta {
                api_version: "meta.k8s.io/v1".to_string(),
                kind: "PartialObjectMetadata".to_string(),
            }),
            metadata: self,
            _phantom: PhantomData,
        }
    }
}

impl<K: Resource> Resource for PartialObjectMeta<K> {
    type DynamicType = K::DynamicType;
    type Scope = K::Scope;

    fn kind(dt: &Self::DynamicType) -> Cow<'_, str> {
        K::kind(dt)
    }

    fn group(dt: &Self::DynamicType) -> Cow<'_, str> {
        K::group(dt)
    }

    fn version(dt: &Self::DynamicType) -> Cow<'_, str> {
        K::version(dt)
    }

    fn plural(dt: &Self::DynamicType) -> Cow<'_, str> {
        K::plural(dt)
    }

    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

#[cfg(test)]
mod test {
    use super::{ObjectMeta, PartialObjectMeta, PartialObjectMetaExt};
    use crate::Resource;
    use k8s_openapi::api::core::v1::Pod;

    #[test]
    fn can_convert_and_derive_partial_metadata() {
        // can use generic type for static dispatch;
        assert_eq!(PartialObjectMeta::<Pod>::kind(&()), "Pod");
        assert_eq!(PartialObjectMeta::<Pod>::api_version(&()), "v1");

        // can convert from objectmeta to partials for different use cases:
        let meta = ObjectMeta {
            name: Some("mypod".into()),
            ..Default::default()
        };
        let request_pom = meta.clone().into_request_partial::<Pod>();
        let response_pom = meta.into_response_partial::<Pod>();

        // they both basically just inline the metadata;
        assert_eq!(request_pom.metadata.name, Some("mypod".to_string()));
        assert_eq!(response_pom.metadata.name, Some("mypod".to_string()));

        // the request_pom will use the TypeMeta from K to support POST/PUT requests
        assert_eq!(request_pom.types.as_ref().unwrap().api_version, "v1");
        assert_eq!(request_pom.types.as_ref().unwrap().kind, "Pod");

        // but the response_pom will use the type-erased kinds from the apiserver
        assert_eq!(response_pom.types.as_ref().unwrap().api_version, "meta.k8s.io/v1");
        assert_eq!(response_pom.types.as_ref().unwrap().kind, "PartialObjectMetadata");
    }
}
