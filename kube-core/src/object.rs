//! Generic object and objectlist wrappers.
use crate::{
    discovery::ApiResource,
    metadata::{ListMeta, ObjectMeta, TypeMeta},
    resource::{DynamicResourceScope, Resource},
    DynamicObject,
};
use serde::{
    de::{self, DeserializeOwned},
    Deserialize, Deserializer, Serialize,
};
use std::borrow::Cow;

/// A generic Kubernetes object list
///
/// This is used instead of a full struct for `DeploymentList`, `PodList`, etc.
/// Kubernetes' API [always seem to expose list structs in this manner](https://docs.rs/k8s-openapi/0.10.0/k8s_openapi/apimachinery/pkg/apis/meta/v1/struct.ObjectMeta.html?search=List).
///
/// Note that this is only used internally within reflectors and informers,
/// and is generally produced from list/watch/delete collection queries on an [`Resource`](super::Resource).
///
/// This is almost equivalent to [`k8s_openapi::List<T>`](k8s_openapi::List), but iterable.
#[derive(Serialize, Debug, Clone)]
pub struct ObjectList<T>
where
    T: Clone,
{
    /// The type fields, always present
    #[serde(flatten, deserialize_with = "deserialize_v1_list_as_default")]
    pub types: TypeMeta,

    /// ListMeta - only really used for its `resourceVersion`
    ///
    /// See [ListMeta](k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta)
    #[serde(default)]
    pub metadata: ListMeta,

    /// The items we are actually interested in. In practice; `T := Resource<T,U>`.
    #[serde(
        deserialize_with = "deserialize_null_as_default",
        bound(deserialize = "Vec<T>: Deserialize<'de>")
    )]
    pub items: Vec<T>,
}

fn deserialize_v1_list_as_default<'de, D>(deserializer: D) -> Result<TypeMeta, D::Error>
where
    D: Deserializer<'de>,
{
    let meta = Option::<TypeMeta>::deserialize(deserializer)?;
    Ok(meta.unwrap_or(TypeMeta {
        api_version: "v1".to_owned(),
        kind: "List".to_owned(),
    }))
}

fn deserialize_null_as_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    T: Default + Deserialize<'de>,
    D: Deserializer<'de>,
{
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

impl<T: Clone> ObjectList<T> {
    /// `iter` returns an Iterator over the elements of this ObjectList
    ///
    /// # Example
    ///
    /// ```
    /// use kube::api::{ListMeta, ObjectList, TypeMeta};
    /// use k8s_openapi::api::core::v1::Pod;
    ///
    /// let types: TypeMeta = TypeMeta::list::<Pod>();
    /// let metadata: ListMeta = Default::default();
    /// let items = vec![1, 2, 3];
    /// # let objectlist = ObjectList { types, metadata, items };
    ///
    /// let first = objectlist.iter().next();
    /// println!("First element: {:?}", first); // prints "First element: Some(1)"
    /// ```
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.items.iter()
    }

    /// `iter_mut` returns an Iterator of mutable references to the elements of this ObjectList
    ///
    /// # Example
    ///
    /// ```
    /// use kube::api::{ListMeta, ObjectList, TypeMeta};
    /// use k8s_openapi::api::core::v1::Pod;
    ///
    /// let types: TypeMeta = TypeMeta::list::<Pod>();
    /// let metadata: ListMeta = Default::default();
    /// let items = vec![1, 2, 3];
    /// # let mut objectlist = ObjectList { types, metadata, items };
    ///
    /// let mut first = objectlist.iter_mut().next();
    ///
    /// // Reassign the value in first
    /// if let Some(elem) = first {
    ///     *elem = 2;
    ///     println!("First element: {:?}", elem); // prints "First element: 2"
    /// }
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.items.iter_mut()
    }
}

impl<T: Clone> IntoIterator for ObjectList<T> {
    type IntoIter = ::std::vec::IntoIter<Self::Item>;
    type Item = T;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

impl<'a, T: Clone> IntoIterator for &'a ObjectList<T> {
    type IntoIter = ::std::slice::Iter<'a, T>;
    type Item = &'a T;

    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}

impl<'a, T: Clone> IntoIterator for &'a mut ObjectList<T> {
    type IntoIter = ::std::slice::IterMut<'a, T>;
    type Item = &'a mut T;

    fn into_iter(self) -> Self::IntoIter {
        self.items.iter_mut()
    }
}

#[derive(Deserialize)]
struct DynamicList {
    #[serde(flatten, deserialize_with = "deserialize_v1_list_as_default")]
    types: TypeMeta,

    #[serde(default)]
    metadata: ListMeta,

    #[serde(
        deserialize_with = "deserialize_null_as_default",
        bound(deserialize = "Vec<DynamicObject>: Deserialize<'de>")
    )]
    items: Vec<DynamicObject>,
}

impl<'de, T: DeserializeOwned + Clone> serde::Deserialize<'de> for ObjectList<T> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<ObjectList<T>, D::Error> {
        let DynamicList {
            types,
            metadata,
            mut items,
        } = DynamicList::deserialize(d)?;
        let mut resources = vec![];
        for o in items.iter_mut() {
            o.types = Some(types.clone().singular());
            let item = serde_json::to_value(o).map_err(de::Error::custom)?;
            resources.push(serde_json::from_value(item).map_err(de::Error::custom)?)
        }
        Ok(ObjectList::<T> {
            types,
            metadata,
            items: resources,
        })
    }
}

/// A trait to access the `spec` of a Kubernetes resource.
///
/// Some built-in Kubernetes resources and all custom resources do have a `spec` field.
/// This trait can be used to access this field.
///
/// This trait is automatically implemented by the kube-derive macro and is _not_ currently
/// implemented for the Kubernetes API objects from `k8s_openapi`.
///
/// Note: Not all Kubernetes resources have a spec (e.g. `ConfigMap`, `Secret`, ...).
pub trait HasSpec {
    /// The type of the `spec` of this resource
    type Spec;

    /// Returns a reference to the `spec` of the object
    fn spec(&self) -> &Self::Spec;

    /// Returns a mutable reference to the `spec` of the object
    fn spec_mut(&mut self) -> &mut Self::Spec;
}

/// A trait to access the `status` of a Kubernetes resource.
///
/// Some built-in Kubernetes resources and custom resources do have a `status` field.
/// This trait can be used to access this field.
///
/// This trait is automatically implemented by the kube-derive macro and is _not_ currently
/// implemented for the Kubernetes API objects from `k8s_openapi`.
///
/// Note: Not all Kubernetes resources have a status (e.g. `ConfigMap`, `Secret`, ...).
pub trait HasStatus {
    /// The type of the `status` object
    type Status;

    /// Returns an optional reference to the `status` of the object
    fn status(&self) -> Option<&Self::Status>;

    /// Returns an optional mutable reference to the `status` of the object
    fn status_mut(&mut self) -> &mut Option<Self::Status>;
}

// -------------------------------------------------------

/// A standard Kubernetes object with `.spec` and `.status`.
///
/// This is a convenience struct provided for serialization/deserialization.
/// It is slightly stricter than ['DynamicObject`] in that it enforces the spec/status convention,
/// and as such will not in general work with all api-discovered resources.
///
/// This can be used to tie existing resources to smaller, local struct variants to optimize for memory use.
/// E.g. if you are only interested in a few fields, but you store tons of them in memory with reflectors.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct Object<P, U>
where
    P: Clone,
    U: Clone,
{
    /// The type fields, not always present
    #[serde(flatten, default)]
    pub types: Option<TypeMeta>,

    /// Resource metadata
    ///
    /// Contains information common to most resources about the Resource,
    /// including the object name, annotations, labels and more.
    pub metadata: ObjectMeta,

    /// The Spec struct of a resource. I.e. `PodSpec`, `DeploymentSpec`, etc.
    ///
    /// This defines the desired state of the Resource as specified by the user.
    pub spec: P,

    /// The Status of a resource. I.e. `PodStatus`, `DeploymentStatus`, etc.
    ///
    /// This publishes the state of the Resource as observed by the controller.
    /// Use `U = NotUsed` when a status does not exist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<U>,
}

impl<P, U> Object<P, U>
where
    P: Clone,
    U: Clone,
{
    /// A constructor that takes Resource values from an `ApiResource`
    pub fn new(name: &str, ar: &ApiResource, spec: P) -> Self {
        Self {
            types: Some(TypeMeta {
                api_version: ar.api_version.clone(),
                kind: ar.kind.clone(),
            }),
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                ..Default::default()
            },
            spec,
            status: None,
        }
    }

    /// Attach a namespace to an Object
    #[must_use]
    pub fn within(mut self, ns: &str) -> Self {
        self.metadata.namespace = Some(ns.into());
        self
    }
}

impl<P, U> Resource for Object<P, U>
where
    P: Clone,
    U: Clone,
{
    type DynamicType = ApiResource;
    type Scope = DynamicResourceScope;

    fn group(dt: &ApiResource) -> Cow<'_, str> {
        dt.group.as_str().into()
    }

    fn version(dt: &ApiResource) -> Cow<'_, str> {
        dt.version.as_str().into()
    }

    fn kind(dt: &ApiResource) -> Cow<'_, str> {
        dt.kind.as_str().into()
    }

    fn plural(dt: &ApiResource) -> Cow<'_, str> {
        dt.plural.as_str().into()
    }

    fn api_version(dt: &ApiResource) -> Cow<'_, str> {
        dt.api_version.as_str().into()
    }

    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

impl<P, U> HasSpec for Object<P, U>
where
    P: Clone,
    U: Clone,
{
    type Spec = P;

    fn spec(&self) -> &Self::Spec {
        &self.spec
    }

    fn spec_mut(&mut self) -> &mut Self::Spec {
        &mut self.spec
    }
}

impl<P, U> HasStatus for Object<P, U>
where
    P: Clone,
    U: Clone,
{
    type Status = U;

    fn status(&self) -> Option<&Self::Status> {
        self.status.as_ref()
    }

    fn status_mut(&mut self) -> &mut Option<Self::Status> {
        &mut self.status
    }
}

/// Empty struct for when data should be discarded
///
/// Not using [`()`](https://doc.rust-lang.org/stable/std/primitive.unit.html), because serde's
/// [`Deserialize`](serde::Deserialize) `impl` is too strict.
#[derive(Clone, Deserialize, Serialize, Default, Debug)]
pub struct NotUsed {}

#[cfg(test)]
mod test {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ListMeta, ObjectMeta};

    use super::{ApiResource, HasSpec, HasStatus, NotUsed, Object, ObjectList, Resource, TypeMeta};
    use crate::resource::ResourceExt;

    #[test]
    fn simplified_k8s_object() {
        use k8s_openapi::api::core::v1::Pod;
        // Replacing heavy type k8s_openapi::api::core::v1::PodSpec with:
        #[derive(Clone)]
        struct PodSpecSimple {
            #[allow(dead_code)]
            containers: Vec<ContainerSimple>,
        }
        #[derive(Clone, Debug, PartialEq)]
        struct ContainerSimple {
            #[allow(dead_code)]
            image: String,
        }
        type PodSimple = Object<PodSpecSimple, NotUsed>;
        // by grabbing the ApiResource info from the Resource trait
        let ar = ApiResource::erase::<Pod>(&());
        assert_eq!(ar.group, "");
        assert_eq!(ar.kind, "Pod");
        let data = PodSpecSimple {
            containers: vec![ContainerSimple { image: "blog".into() }],
        };
        let mypod = PodSimple::new("blog", &ar, data).within("dev");

        let meta = mypod.meta();
        assert_eq!(&mypod.metadata, meta);
        assert_eq!(meta.namespace.as_ref().unwrap(), "dev");
        assert_eq!(meta.name.as_ref().unwrap(), "blog");
        assert_eq!(mypod.types.as_ref().unwrap().kind, "Pod");
        assert_eq!(mypod.types.as_ref().unwrap().api_version, "v1");

        assert_eq!(mypod.namespace().unwrap(), "dev");
        assert_eq!(mypod.name_unchecked(), "blog");
        assert!(mypod.status().is_none());
        assert_eq!(
            mypod.spec().containers[0],
            ContainerSimple { image: "blog".into() }
        );

        assert_eq!(PodSimple::api_version(&ar), "v1");
        assert_eq!(PodSimple::version(&ar), "v1");
        assert_eq!(PodSimple::plural(&ar), "pods");
        assert_eq!(PodSimple::kind(&ar), "Pod");
        assert_eq!(PodSimple::group(&ar), "");
    }

    #[test]
    fn k8s_object_list() {
        use k8s_openapi::api::core::v1::Pod;
        // by grabbing the ApiResource info from the Resource trait
        let ar = ApiResource::erase::<Pod>(&());
        assert_eq!(ar.group, "");
        assert_eq!(ar.kind, "Pod");
        let podlist: ObjectList<Pod> = ObjectList {
            types: TypeMeta {
                api_version: ar.api_version,
                kind: ar.kind + "List",
            },
            metadata: ListMeta { ..Default::default() },
            items: vec![Pod {
                metadata: ObjectMeta {
                    name: Some("test".into()),
                    namespace: Some("dev".into()),
                    ..ObjectMeta::default()
                },
                spec: None,
                status: None,
            }],
        };

        assert_eq!(&podlist.types.kind, "PodList");
        assert_eq!(&podlist.types.api_version, "v1");

        let mypod = &podlist.items[0];
        let meta = mypod.meta();
        assert_eq!(&mypod.metadata, meta);
        assert_eq!(meta.namespace.as_ref().unwrap(), "dev");
        assert_eq!(meta.name.as_ref().unwrap(), "test");
        assert_eq!(mypod.namespace().unwrap(), "dev");
        assert_eq!(mypod.name_unchecked(), "test");
        assert!(mypod.status.is_none());
        assert!(mypod.spec.is_none());
    }

    #[test]
    fn k8s_object_list_default_types() {
        use k8s_openapi::api::core::v1::Pod;

        let raw_value = serde_json::json!({
            "metadata": {
                "resourceVersion": ""
            },
            "items": []
        });
        let pod_list: ObjectList<Pod> = serde_json::from_value(raw_value).unwrap();
        assert_eq!(
            TypeMeta {
                api_version: "v1".to_owned(),
                kind: "List".to_owned(),
            },
            pod_list.types,
        );
    }
}
