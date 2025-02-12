//! Waits for objects to reach desired states
use std::{future, pin::pin};

use futures::TryStreamExt;
use kube_client::{Api, Resource};
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use thiserror::Error;

use crate::watcher::{self, watch_object};

/// Wait errors from [`await_condition`]
#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to probe for whether the condition is fulfilled yet: {0}")]
    ProbeFailed(#[source] watcher::Error),
}

/// Watch an object, and wait for some condition `cond` to return `true`.
///
/// `cond` is passed `Some` if the object is found, otherwise `None`.
///
/// The object is returned when the condition is fulfilled.
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
#[allow(clippy::missing_panics_doc)] // watch never actually terminates, expect cannot fail
pub async fn await_condition<K>(api: Api<K>, name: &str, cond: impl Condition<K>) -> Result<Option<K>, Error>
where
    K: Clone + Debug + Send + DeserializeOwned + Resource + 'static,
{
    // Skip updates until the condition is satisfied.
    let mut stream = pin!(watch_object(api, name).try_skip_while(|obj| {
        let matches = cond.matches_object(obj.as_ref());
        future::ready(Ok(!matches))
    }));

    // Then take the first update that satisfies the condition.
    let obj = stream
        .try_next()
        .await
        .map_err(Error::ProbeFailed)?
        .expect("stream must not terminate");
    Ok(obj)
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
///         let cond = obj.status.as_ref()?.conditions.as_ref()?.iter().find(|c| c.type_ == my_cond);
///         Some(cond.status == "True")
///     }
/// }
/// ```
pub trait Condition<K> {
    /// Condition function giving a clear answer
    fn matches_object(&self, obj: Option<&K>) -> bool {
        self.matches_object_option(obj).unwrap_or_default()
    }
    /// Condition function giving an option wrapped answer
    fn matches_object_option(&self, _obj: Option<&K>) -> Option<bool>;

    /// Returns a `Condition` that holds if `self` does not
    ///
    /// # Usage
    ///
    /// ```
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
    /// ```
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
    /// ```
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

//impl<K, F: Fn(Option<&K>) -> bool> Condition<K> for F {
//    fn matches_object(&self, obj: Option<&K>) -> bool {
//        (self)(obj)
//    }
//}
impl<K, F: Fn(Option<&K>) -> Option<bool>> Condition<K> for F {
    fn matches_object_option(&self, obj: Option<&K>) -> Option<bool> {
        (self)(obj)
    }
}

/// Common conditions to wait for
pub mod conditions {
    pub use super::Condition;
    use k8s_openapi::{
        api::{batch::v1::Job, core::v1::Pod},
        apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition,
    };
    use kube_client::Resource;

    /// An await condition that returns `true` once the object has been deleted.
    ///
    /// An object is considered to be deleted if the object can no longer be found, or if its
    /// [`uid`](kube_client::api::ObjectMeta#structfield.uid) changes. This means that an object is considered to be deleted even if we miss
    /// the deletion event and the object is recreated in the meantime.
    #[must_use]
    pub fn is_deleted<K: Resource>(uid: &str) -> impl Condition<K> + '_ {
        move |obj: Option<&K>| {
            Some(obj.map_or(
                // Object is not found, success!
                true,
                // Object is found, but a changed uid would mean that it was deleted and recreated
                |obj| obj.meta().uid.as_deref() != Some(uid),
            ))
        }
    }

    /// An await condition for `CustomResourceDefinition` that returns `true` once it has been accepted and established
    ///
    /// Note that this condition only guarantees you that you can use `Api<CustomResourceDefinition>` when it is ready.
    /// It usually takes extra time for Discovery to notice the custom resource, and there is no condition for this.
    #[must_use]
    pub fn is_crd_established() -> impl Condition<CustomResourceDefinition> {
        |obj: Option<&CustomResourceDefinition>| {
            let cond = obj
                .as_ref()?
                .status
                .as_ref()?
                .conditions
                .as_ref()?
                .iter()
                .find(|c| c.type_ == "Established")?;
            Some(cond.status == "True")
        }
    }

    /// An await condition for `Pod` that returns `true` once it is running
    #[must_use]
    pub fn is_pod_running() -> impl Condition<Pod> {
        |obj: Option<&Pod>| Some(obj?.status.as_ref()?.phase.as_ref()? == "Running")
    }

    /// An await condition for `Job` that returns `true` once it is completed
    #[must_use]
    pub fn is_job_completed() -> impl Condition<Job> {
        |obj: Option<&Job>| {
            let cond = obj?
                .status
                .as_ref()?
                .conditions
                .as_ref()?
                .iter()
                .find(|c| c.type_ == "Complete")?;
            Some(cond.status == "True")
        }
    }

    /// See [`Condition::not`]
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub struct Not<A>(pub(super) A);
    impl<A: Condition<K>, K> Condition<K> for Not<A> {
        fn matches_object_option(&self, obj: Option<&K>) -> Option<bool> {
            Some(!self.0.matches_object(obj))
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
        fn matches_object_option(&self, obj: Option<&K>) -> Option<bool> {
            Some(self.0.matches_object(obj) && self.1.matches_object(obj))
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
        fn matches_object_option(&self, obj: Option<&K>) -> Option<bool> {
            Some(self.0.matches_object(obj) || self.1.matches_object(obj))
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
            .map_err(Error::Await)?;
        Ok(())
    }
}
