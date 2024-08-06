//! Types for isolating deserialization failures. See [`ErrorBoundary`].

use std::{borrow::Cow, fmt::Display};

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use serde::Deserialize;
use serde_value::DeserializerError;

use crate::Resource;

/// A wrapper type for T that lets deserializing the parent object succeed, even if the T is invalid.
///
/// For example, this can be used to still access valid objects from an `Api::list` call or `watcher`.
#[derive(Debug, Clone)]
pub struct ErrorBoundary<T>(pub Result<T, InvalidObject>);

/// An object that failed to be deserialized by the [`ErrorBoundary`].
#[derive(Debug, Clone)]
pub struct InvalidObject {
    // Should ideally be D::Error, but we don't know what type it has outside of Deserialize::deserialize()
    // It *could* be Box<std::error::Error>, but we don't know that it is Send+Sync
    /// The error message from deserializing the object.
    pub error: String,
    /// The metadata of the invalid object.
    pub metadata: ObjectMeta,
}

impl Display for InvalidObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl<'de, T> Deserialize<'de> for ErrorBoundary<T>
where
    T: Deserialize<'de>,
    // Not actually used, but we assume that T is a Kubernetes-style resource with a `metadata` section
    T: Resource,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct ObjectMetaContainer {
            metadata: ObjectMeta,
        }

        // Deserialize::deserialize consumes the deserializer, and we want to retry parsing as an ObjectMetaContainer
        // if the initial parse fails, so that we can still implement Resource for the error case
        let buffer = serde_value::Value::deserialize(deserializer)?;

        // FIXME: can we avoid cloning the whole object? metadata should be enough, and even then we could prune managedFields
        T::deserialize(buffer.clone())
            .map(Ok)
            .or_else(|err| {
                let ObjectMetaContainer { metadata } =
                    ObjectMetaContainer::deserialize(buffer).map_err(DeserializerError::into_error)?;
                Ok(Err(InvalidObject {
                    error: err.to_string(),
                    metadata,
                }))
            })
            .map(ErrorBoundary)
    }
}

impl<T: Resource> Resource for ErrorBoundary<T> {
    type DynamicType = T::DynamicType;
    type Scope = T::Scope;

    fn kind(dt: &Self::DynamicType) -> Cow<str> {
        T::kind(dt)
    }

    fn group(dt: &Self::DynamicType) -> Cow<str> {
        T::group(dt)
    }

    fn version(dt: &Self::DynamicType) -> Cow<str> {
        T::version(dt)
    }

    fn plural(dt: &Self::DynamicType) -> Cow<str> {
        T::plural(dt)
    }

    fn meta(&self) -> &ObjectMeta {
        self.0.as_ref().map_or_else(|err| &err.metadata, T::meta)
    }

    fn meta_mut(&mut self) -> &mut ObjectMeta {
        self.0.as_mut().map_or_else(|err| &mut err.metadata, T::meta_mut)
    }
}
