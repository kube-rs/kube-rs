//! Waits for objects to reach desired states
use futures::TryStreamExt;
use kube_client::{Api, Resource};
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use thiserror::Error;

use crate::watcher::{self, watch_object};

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to probe for whether the condition is fulfilled yet: {0}")]
    ProbeFailed(#[source] watcher::Error),
}

/// Watch an object, and Wait for some condition `cond` to return `true`.
///
/// `cond` is passed `Some` if the object is found, otherwise `None`.
///
/// # Caveats
///
/// Keep in mind that the condition is typically fulfilled by an external service, which might not even be available. `await_condition`
/// does *not* automatically add a timeout. If this is desired, wrap it in [`tokio::time::timeout`].
///
/// # Errors
///
/// Fails if the type is not known to the Kubernetes API, or if the [`Api`] does not have
/// permission to `watch` and `list` it.
///
/// Does *not* fail if the object is not found.
///
/// # Usage
///
/// ```
/// use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
/// use kube::{Api, runtime::wait::{await_condition, conditions}};
/// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
/// # let client: kube::Client = todo!();
///
/// let crds: Api<CustomResourceDefinition> = Api::all(client);
/// // .. create or apply a crd here ..
/// let establish = await_condition(crds, "foos.clux.dev", conditions::is_crd_established());
/// let _ = tokio::time::timeout(std::time::Duration::from_secs(10), establish).await?;
/// # Ok(())
/// # }
/// ```
pub async fn await_condition<K>(api: Api<K>, name: &str, cond: impl Condition<K>) -> Result<(), Error>
where
    K: Clone + Debug + Send + DeserializeOwned + Resource + 'static,
{
    watch_object(api, name)
        .map_err(Error::ProbeFailed)
        .try_take_while(|obj| {
            let result = !cond.matches_object(obj.as_ref());
            async move { Ok(result) }
        })
        .try_for_each(|_| async { Ok(()) })
        .await
}

/// A trait for condition functions to be used by [`await_condition`]
///
/// Note that this is auto-implemented for functions of type `fn(Option<&K>) -> bool`.
///
/// # Usage
///
/// ```
/// use kube::runtime::wait::Condition;
/// use k8s_openapi::api::core::v1::Pod;
/// fn my_custom_condition(my_cond: &str) -> impl Condition<Pod> + '_ {
///     move |obj: Option<&Pod>| {
///         if let Some(pod) = &obj {
///             if let Some(status) = &pod.status {
///                 if let Some(conds) = &status.conditions {
///                     if let Some(pcond) = conds.iter().find(|c| c.type_ == my_cond) {
///                         return pcond.status == "True";
///                     }
///                 }
///             }
///         }
///         false
///     }
/// }
/// ```
pub trait Condition<K: ?Sized> {
    fn matches_object(&self, obj: Option<&K>) -> bool;

    /// Returns a `Condition` that holds if `self` does not
    ///
    /// # Usage
    ///
    /// ```rust
    /// # use kube_runtime::wait::Condition;
    /// let condition: fn(Option<&()>) -> bool = |_| true;
    /// assert!(condition.matches_object(None));
    /// assert!(!condition.not().matches_object(None));
    /// ```
    fn not(self) -> conditions::Not<Self>
    where
        Self: Sized,
    {
        conditions::Not(self)
    }

    /// Returns a `Condition` that holds if `self` and `other` both do
    ///
    /// # Usage
    ///
    /// ```rust
    /// # use kube_runtime::wait::Condition;
    /// let cond_false: fn(Option<&()>) -> bool = |_| false;
    /// let cond_true: fn(Option<&()>) -> bool = |_| true;
    /// assert!(!cond_false.and(cond_false).matches_object(None));
    /// assert!(!cond_false.and(cond_true).matches_object(None));
    /// assert!(!cond_true.and(cond_false).matches_object(None));
    /// assert!(cond_true.and(cond_true).matches_object(None));
    /// ```
    fn and<Other: Condition<K>>(self, other: Other) -> conditions::And<Self, Other>
    where
        Self: Sized,
    {
        conditions::And(self, other)
    }

    /// Returns a `Condition` that holds if either `self` or `other` does
    ///
    /// # Usage
    ///
    /// ```rust
    /// # use kube_runtime::wait::Condition;
    /// let cond_false: fn(Option<&()>) -> bool = |_| false;
    /// let cond_true: fn(Option<&()>) -> bool = |_| true;
    /// assert!(!cond_false.or(cond_false).matches_object(None));
    /// assert!(cond_false.or(cond_true).matches_object(None));
    /// assert!(cond_true.or(cond_false).matches_object(None));
    /// assert!(cond_true.or(cond_true).matches_object(None));
    /// ```
    fn or<Other: Condition<K>>(self, other: Other) -> conditions::Or<Self, Other>
    where
        Self: Sized,
    {
        conditions::Or(self, other)
    }
}

impl<K: ?Sized, F: Fn(Option<&K>) -> bool> Condition<K> for F {
    fn matches_object(&self, obj: Option<&K>) -> bool {
        (self)(obj)
    }
}

/// Common conditions to wait for
pub mod conditions {
    pub use super::Condition;
    use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
    use kube_client::Resource;
    use serde::Serialize;

    /// An await condition that returns `true` once the object has been deleted.
    ///
    /// An object is considered to be deleted if the object can no longer be found, or if its
    /// [`uid`](kube_client::api::ObjectMeta#structfield.uid) changes. This means that an object is considered to be deleted even if we miss
    /// the deletion event and the object is recreated in the meantime.
    #[must_use]
    pub fn is_deleted<K: Resource>(uid: &str) -> impl Condition<K> + '_ {
        move |obj: Option<&K>| {
            obj.map_or(
                // Object is not found, success!
                true,
                // Object is found, but a changed uid would mean that it was deleted and recreated
                |obj| obj.meta().uid.as_deref() != Some(uid),
            )
        }
    }

    /// An await condition for `CustomResourceDefinition` that returns `true` once it has been accepted and established
    #[must_use]
    pub fn is_crd_established() -> impl Condition<CustomResourceDefinition> {
        |obj: Option<&CustomResourceDefinition>| {
            if let Some(o) = obj {
                if let Some(s) = &o.status {
                    if let Some(conds) = &s.conditions {
                        if let Some(pcond) = conds.iter().find(|c| c.type_ == "Established") {
                            return pcond.status == "True";
                        }
                    }
                }
            }
            false
        }
    }

    /// An await condition that returns `true` if the object exists.
    ///
    /// NOTE: If waiting for an object to be deleted, do _not_ [invert](`Condition::not`) this [`Condition`].
    /// Instead, use [`is_deleted`], which considers a deleted-then-recreated object to have been deleted.
    #[must_use]
    pub fn exists<K>() -> impl Condition<K> {
        |obj: Option<&K>| obj.is_some()
    }

    /// A condition that returns true if an arbitrary condition matches a condition value
    ///
    /// # Value condition
    ///
    /// The value condition is passed `None` if the object does not exist or does not have the given condition (combine with
    /// [`exists`] if you need to validate whether the object exists). Otherwise, the value should be one of `"True"`, `"False"`, or `"Unknown"`
    /// (see <https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.22/#condition-v1-meta> for more details).
    ///
    /// # Stability
    ///
    /// This is an experimental API that should be expected to change. It has a few particular problems:
    ///
    /// 1. It is completely untyped
    /// 2. It makes fairly deep assumptions about the structure of the object and its status
    /// 3. It doesn't have any way to signal errors gracefully
    /// 4. It has some unfortunate lifetime problems that prevent bringing in a closure context
    ///
    /// # Usage
    ///
    /// ```rust
    /// # use k8s_openapi::api::core::v1::{Pod, PodCondition, PodStatus};
    /// # use kube_runtime::wait::{conditions::unstable_has_status_condition, Condition};
    /// let pod = |ready: String| Pod {
    ///     status: Some(PodStatus {
    ///         conditions: Some(vec![PodCondition {
    ///             type_: "Ready".to_string(),
    ///             status: ready,
    ///             ..PodCondition::default()
    ///         }]),
    ///         ..PodStatus::default()
    ///     }),
    ///     ..Pod::default()
    /// };
    /// let cond_status_ready: fn(Option<&str>) -> bool = |status| status == Some("True");
    /// let cond_pod_ready = unstable_has_status_condition("Ready", cond_status_ready);
    /// assert!(!cond_pod_ready.matches_object(Some(&pod("False".to_string()))));
    /// assert!(cond_pod_ready.matches_object(Some(&pod("True".to_string()))));
    /// ```
    #[must_use]
    pub fn unstable_has_status_condition<'a, K: Serialize + Resource, StatusCond: Condition<str> + 'a>(
        condition_type: &'a str,
        status_cond: StatusCond,
    ) -> impl Condition<K> + 'a {
        move |obj: Option<&K>| {
            let serialized_obj = serde_json::to_value(obj).ok();
            status_cond.matches_object(serialized_obj.as_ref().and_then(|obj| {
                obj.get("status")?
                    .get("conditions")?
                    .as_array()?
                    .iter()
                    .find(|cond| {
                        cond.get("type").and_then(serde_json::Value::as_str) == Some(condition_type)
                    })?
                    .get("status")?
                    .as_str()
            }))
        }
    }

    /// See [`Condition::not`]
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub struct Not<A>(pub(super) A);
    impl<A: Condition<K>, K> Condition<K> for Not<A> {
        fn matches_object(&self, obj: Option<&K>) -> bool {
            !self.0.matches_object(obj)
        }
    }

    /// See [`Condition::and`]
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub struct And<A, B>(pub(super) A, pub(super) B);
    impl<A, B, K> Condition<K> for And<A, B>
    where
        A: Condition<K>,
        B: Condition<K>,
    {
        fn matches_object(&self, obj: Option<&K>) -> bool {
            self.0.matches_object(obj) && self.1.matches_object(obj)
        }
    }

    /// See [`Condition::or`]
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub struct Or<A, B>(pub(super) A, pub(super) B);
    impl<A, B, K> Condition<K> for Or<A, B>
    where
        A: Condition<K>,
        B: Condition<K>,
    {
        fn matches_object(&self, obj: Option<&K>) -> bool {
            self.0.matches_object(obj) || self.1.matches_object(obj)
        }
    }
}

/// Utilities for deleting objects
pub mod delete {
    use super::{await_condition, conditions};
    use kube_client::{api::DeleteParams, Api, Resource};
    use serde::de::DeserializeOwned;
    use std::fmt::Debug;
    use thiserror::Error;

    #[derive(Debug, Error)]
    pub enum Error {
        #[error("deleted object has no UID to wait for")]
        NoUid,
        #[error("failed to delete object: {0}")]
        Delete(#[source] kube_client::Error),
        #[error("failed to wait for object to be deleted: {0}")]
        Await(#[source] super::Error),
    }

    /// Delete an object, and wait for it to be removed from the Kubernetes API (including waiting for all finalizers to unregister themselves).
    ///
    /// # Errors
    ///
    /// Returns an [`Error`](enum@super::Error) if the object was unable to be deleted, or if the wait was interrupted.
    #[allow(clippy::module_name_repetitions)]
    pub async fn delete_and_finalize<K: Clone + Debug + Send + DeserializeOwned + Resource + 'static>(
        api: Api<K>,
        name: &str,
        delete_params: &DeleteParams,
    ) -> Result<(), Error> {
        let deleted_obj_uid = api
            .delete(name, delete_params)
            .await
            .map_err(Error::Delete)?
            .either(
                |mut obj| obj.meta_mut().uid.take(),
                |status| status.details.map(|details| details.uid),
            )
            .ok_or(Error::NoUid)?;
        await_condition(api, name, conditions::is_deleted(&deleted_obj_uid))
            .await
            .map_err(Error::Await)
    }
}
