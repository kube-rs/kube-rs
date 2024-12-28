use educe::Educe;
use k8s_openapi::{api::core::v1::ObjectReference, apimachinery::pkg::apis::meta::v1::OwnerReference};
#[cfg(doc)] use kube_client::core::ObjectMeta;
use kube_client::{
    api::{DynamicObject, Resource},
    core::api_version_from_group_version,
};
use std::{
    borrow::Cow,
    fmt::{Debug, Display},
    hash::Hash,
};

/// Minimal lookup behaviour needed by a [reflector store](super::Store).
///
/// This trait is blanket-implemented for all [`Resource`] objects.
pub trait Lookup {
    /// Type information for types that do not know their resource information at compile time.
    /// This is equivalent to [`Resource::DynamicType`].
    type DynamicType;

    /// The [kind](Resource::kind) for this object.
    fn kind(dyntype: &Self::DynamicType) -> Cow<'_, str>;

    /// The [group](Resource::group) for this object.
    fn group(dyntype: &Self::DynamicType) -> Cow<'_, str>;

    /// The [version](Resource::version) for this object.
    fn version(dyntype: &Self::DynamicType) -> Cow<'_, str>;

    /// The [apiVersion](Resource::_version) for this object.
    fn api_version(dyntype: &Self::DynamicType) -> Cow<'_, str> {
        api_version_from_group_version(Self::group(dyntype), Self::version(dyntype))
    }

    /// The [plural](Resource::plural) for this object.
    fn plural(dyntype: &Self::DynamicType) -> Cow<'_, str>;

    /// The [name](ObjectMeta#structfield.name) of the object.
    fn name(&self) -> Option<Cow<'_, str>>;

    /// The [namespace](ObjectMeta#structfield.namespace) of the object.
    fn namespace(&self) -> Option<Cow<'_, str>>;

    /// The [resource version](ObjectMeta#structfield.resource_version) of the object.
    fn resource_version(&self) -> Option<Cow<'_, str>>;

    /// The [UID](ObjectMeta#structfield.uid) of the object.
    fn uid(&self) -> Option<Cow<'_, str>>;

    /// Constructs an [`ObjectRef`] for this object.
    fn to_object_ref(&self, dyntype: Self::DynamicType) -> ObjectRef<Self> {
        ObjectRef {
            dyntype,
            name: self.name().expect(".metadata.name missing").into_owned(),
            namespace: self.namespace().map(Cow::into_owned),
            extra: Extra {
                resource_version: self.resource_version().map(Cow::into_owned),
                uid: self.uid().map(Cow::into_owned),
                remaining_lookups: None,
            },
        }
    }
}

impl<K: Resource> Lookup for K {
    type DynamicType = K::DynamicType;

    fn kind(dyntype: &Self::DynamicType) -> Cow<'_, str> {
        K::kind(dyntype)
    }

    fn version(dyntype: &Self::DynamicType) -> Cow<'_, str> {
        K::version(dyntype)
    }

    fn group(dyntype: &Self::DynamicType) -> Cow<'_, str> {
        K::group(dyntype)
    }

    fn plural(dyntype: &Self::DynamicType) -> Cow<'_, str> {
        K::plural(dyntype)
    }

    fn name(&self) -> Option<Cow<'_, str>> {
        self.meta().name.as_deref().map(Cow::Borrowed)
    }

    fn namespace(&self) -> Option<Cow<'_, str>> {
        self.meta().namespace.as_deref().map(Cow::Borrowed)
    }

    fn resource_version(&self) -> Option<Cow<'_, str>> {
        self.meta().resource_version.as_deref().map(Cow::Borrowed)
    }

    fn uid(&self) -> Option<Cow<'_, str>> {
        self.meta().uid.as_deref().map(Cow::Borrowed)
    }
}

#[derive(Educe)]
#[educe(
    Debug(bound("K::DynamicType: Debug")),
    PartialEq(bound("K::DynamicType: PartialEq")),
    Hash(bound("K::DynamicType: Hash")),
    Clone(bound("K::DynamicType: Clone"))
)]
/// A typed and namedspaced (if relevant) reference to a Kubernetes object
///
/// `K` may be either the object type or `DynamicObject`, in which case the
/// type is stored at runtime. Erased `ObjectRef`s pointing to different types
/// are still considered different.
///
/// ```
/// use kube_runtime::reflector::ObjectRef;
/// use k8s_openapi::api::core::v1::{ConfigMap, Secret};
/// assert_ne!(
///     ObjectRef::<ConfigMap>::new("a").erase(),
///     ObjectRef::<Secret>::new("a").erase(),
/// );
/// ```
#[non_exhaustive]
pub struct ObjectRef<K: Lookup + ?Sized> {
    pub dyntype: K::DynamicType,
    /// The name of the object
    pub name: String,
    /// The namespace of the object
    ///
    /// May only be `None` if the kind is cluster-scoped (not located in a namespace).
    /// Note that it *is* acceptable for an `ObjectRef` to a cluster-scoped resource to
    /// have a namespace. These are, however, not considered equal:
    ///
    /// ```
    /// # use kube_runtime::reflector::ObjectRef;
    /// # use k8s_openapi::api::core::v1::ConfigMap;
    /// assert_ne!(ObjectRef::<ConfigMap>::new("foo"), ObjectRef::new("foo").within("bar"));
    /// ```
    pub namespace: Option<String>,
    /// Extra information about the object being referred to
    ///
    /// This is *not* considered when comparing objects, but may be used when converting to and from other representations,
    /// such as [`OwnerReference`] or [`ObjectReference`].
    #[educe(Hash(ignore), PartialEq(ignore))]
    pub extra: Extra,
}

impl<K: Lookup + ?Sized> Eq for ObjectRef<K> where K::DynamicType: Eq {}

/// Non-vital information about an object being referred to
///
/// See [`ObjectRef::extra`].
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct Extra {
    /// The version of the resource at the time of reference
    pub resource_version: Option<String>,
    /// The uid of the object
    pub uid: Option<String>,
    /// Number of remaining cache lookups on this reference
    pub remaining_lookups: Option<usize>,
}

impl<K: Lookup> ObjectRef<K>
where
    K::DynamicType: Default,
{
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self::new_with(name, Default::default())
    }

    #[must_use]
    pub fn from_obj(obj: &K) -> Self {
        obj.to_object_ref(Default::default())
    }
}

impl<K: Lookup> From<&K> for ObjectRef<K>
where
    K::DynamicType: Default,
{
    fn from(obj: &K) -> Self {
        Self::from_obj(obj)
    }
}

impl<K: Lookup> ObjectRef<K> {
    #[must_use]
    pub fn new_with(name: &str, dyntype: K::DynamicType) -> Self {
        Self {
            dyntype,
            name: name.into(),
            namespace: None,
            extra: Extra::default(),
        }
    }

    #[must_use]
    pub fn within(mut self, namespace: &str) -> Self {
        self.namespace = Some(namespace.to_string());
        self
    }

    /// Creates `ObjectRef` from the resource and dynamic type.
    #[must_use]
    pub fn from_obj_with(obj: &K, dyntype: K::DynamicType) -> Self
    where
        K: Lookup,
    {
        obj.to_object_ref(dyntype)
    }

    /// Create an `ObjectRef` from an `OwnerReference`
    ///
    /// Returns `None` if the types do not match.
    #[must_use]
    pub fn from_owner_ref(
        namespace: Option<&str>,
        owner: &OwnerReference,
        dyntype: K::DynamicType,
    ) -> Option<Self> {
        if owner.api_version == K::api_version(&dyntype) && owner.kind == K::kind(&dyntype) {
            Some(Self {
                dyntype,
                name: owner.name.clone(),
                namespace: namespace.map(String::from),
                extra: Extra {
                    resource_version: None,
                    uid: Some(owner.uid.clone()),
                    remaining_lookups: None,
                },
            })
        } else {
            None
        }
    }

    /// Convert into a reference to `K2`
    ///
    /// Note that no checking is done on whether this conversion makes sense. For example, every `Service`
    /// has a corresponding `Endpoints`, but it wouldn't make sense to convert a `Pod` into a `Deployment`.
    #[must_use]
    pub fn into_kind_unchecked<K2: Lookup>(self, dt2: K2::DynamicType) -> ObjectRef<K2> {
        ObjectRef {
            dyntype: dt2,
            name: self.name,
            namespace: self.namespace,
            extra: self.extra,
        }
    }

    pub fn erase(self) -> ObjectRef<DynamicObject> {
        ObjectRef {
            dyntype: kube_client::api::ApiResource {
                group: K::group(&self.dyntype).to_string(),
                version: K::version(&self.dyntype).to_string(),
                api_version: K::api_version(&self.dyntype).to_string(),
                kind: K::kind(&self.dyntype).to_string(),
                plural: K::plural(&self.dyntype).to_string(),
            },
            name: self.name,
            namespace: self.namespace,
            extra: self.extra,
        }
    }
}

impl<K: Lookup> From<ObjectRef<K>> for ObjectReference {
    fn from(val: ObjectRef<K>) -> Self {
        let ObjectRef {
            dyntype: dt,
            name,
            namespace,
            extra: Extra {
                resource_version,
                uid,
                ..
            },
        } = val;
        ObjectReference {
            api_version: Some(K::api_version(&dt).into_owned()),
            kind: Some(K::kind(&dt).into_owned()),
            field_path: None,
            name: Some(name),
            namespace,
            resource_version,
            uid,
        }
    }
}

impl<K: Lookup> Display for ObjectRef<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{}.{}/{}",
            K::kind(&self.dyntype),
            K::version(&self.dyntype),
            K::group(&self.dyntype),
            self.name
        )?;
        if let Some(namespace) = &self.namespace {
            write!(f, ".{namespace}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };

    use super::{Extra, ObjectRef};
    use k8s_openapi::api::{
        apps::v1::Deployment,
        core::v1::{Node, Pod},
    };

    #[test]
    fn display_should_follow_expected_format() {
        assert_eq!(
            format!("{}", ObjectRef::<Pod>::new("my-pod").within("my-namespace")),
            "Pod.v1./my-pod.my-namespace"
        );
        assert_eq!(
            format!(
                "{}",
                ObjectRef::<Deployment>::new("my-deploy").within("my-namespace")
            ),
            "Deployment.v1.apps/my-deploy.my-namespace"
        );
        assert_eq!(
            format!("{}", ObjectRef::<Node>::new("my-node")),
            "Node.v1./my-node"
        );
    }

    #[test]
    fn display_should_be_transparent_to_representation() {
        let pod_ref = ObjectRef::<Pod>::new("my-pod").within("my-namespace");
        assert_eq!(format!("{pod_ref}"), format!("{}", pod_ref.erase()));
        let deploy_ref = ObjectRef::<Deployment>::new("my-deploy").within("my-namespace");
        assert_eq!(format!("{deploy_ref}"), format!("{}", deploy_ref.erase()));
        let node_ref = ObjectRef::<Node>::new("my-node");
        assert_eq!(format!("{node_ref}"), format!("{}", node_ref.erase()));
    }

    #[test]
    fn comparison_should_ignore_extra() {
        let minimal = ObjectRef::<Pod>::new("my-pod").within("my-namespace");
        let with_extra = ObjectRef {
            extra: Extra {
                resource_version: Some("123".to_string()),
                uid: Some("638ffacd-f666-4402-ba10-7848c66ef576".to_string()),
                remaining_lookups: None,
            },
            ..minimal.clone()
        };

        // Eq and PartialEq should be unaffected by the contents of `extra`
        assert_eq!(minimal, with_extra);

        // Hash should be unaffected by the contents of `extra`
        let hash_value = |value: &ObjectRef<Pod>| {
            let mut hasher = DefaultHasher::new();
            value.hash(&mut hasher);
            hasher.finish()
        };
        assert_eq!(hash_value(&minimal), hash_value(&with_extra));
    }
}
