//! Types and traits necessary for interacting with the Kubernetes API
//!
//! This crate provides the minimal apimachinery necessary to make requests to the kubernetes API.
//!
//! It does not export export a client, but it also has almost no dependencies.
//!
//! Everything in this crate is re-exported from [`kube`](https://crates.io/crates/kube)
//! (even with zero features) under [`kube::core`]((https://docs.rs/kube/*/kube/core/index.html)).
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg_attr(docsrs, doc(cfg(feature = "admission")))]
#[cfg(feature = "admission")]
pub mod admission;

pub mod conversion;

pub mod discovery;

pub mod duration;
pub use duration::Duration;

pub mod dynamic;
pub use dynamic::{ApiResource, DynamicObject};

pub mod crd;
pub use crd::CustomResourceExt;

pub mod cel;
pub use cel::{Message, Reason, Rule};

#[cfg(feature = "schema")]
pub use cel::{merge_properties, validate, validate_property};

pub mod gvk;
pub use gvk::{GroupVersion, GroupVersionKind, GroupVersionResource};

pub mod metadata;
pub use metadata::{ListMeta, ObjectMeta, PartialObjectMeta, PartialObjectMetaExt, TypeMeta, TypedResource};

pub mod labels;

#[cfg(feature = "kubelet-debug")] pub mod kubelet_debug;

pub mod object;
pub use object::{NotUsed, Object, ObjectList};

pub mod params;

pub mod request;
pub use request::Request;

mod resource;
pub use resource::{
    api_version_from_group_version, ClusterResourceScope, DynamicResourceScope, NamespaceResourceScope,
    Resource, ResourceExt, ResourceScope, SubResourceScope,
};

pub mod response;
pub use response::Status;

pub use labels::{Expression, ParseExpressionError, Selector, SelectorExt};

#[cfg_attr(docsrs, doc(cfg(feature = "schema")))]
#[cfg(feature = "schema")]
pub mod schema;

pub mod subresource;

pub mod util;

pub mod watch;
pub use watch::WatchEvent;

mod error;
pub use error::ErrorResponse;

mod version;
pub use version::Version;

pub mod error_boundary;
pub use error_boundary::DeserializeGuard;
