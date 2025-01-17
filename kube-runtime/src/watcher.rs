//! Watches a Kubernetes Resource for changes, with error recovery
//!
//! See [`watcher`] for the primary entry point.

use crate::utils::{Backoff, ResetTimerBackoff};

use async_trait::async_trait;
use backon::BackoffBuilder;
use educe::Educe;
use futures::{stream::BoxStream, Stream, StreamExt};
use kube_client::{
    api::{ListParams, Resource, ResourceExt, VersionMatch, WatchEvent, WatchParams},
    core::{metadata::PartialObjectMeta, ObjectList, Selector},
    error::ErrorResponse,
    Api, Error as ClientErr,
};
use serde::de::DeserializeOwned;
use std::{clone::Clone, collections::VecDeque, fmt::Debug, future, time::Duration};
use thiserror::Error;
use tracing::{debug, error, warn};

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to perform initial object list: {0}")]
    InitialListFailed(#[source] kube_client::Error),
    #[error("failed to start watching object: {0}")]
    WatchStartFailed(#[source] kube_client::Error),
    #[error("error returned by apiserver during watch: {0}")]
    WatchError(#[source] ErrorResponse),
    #[error("watch stream failed: {0}")]
    WatchFailed(#[source] kube_client::Error),
    #[error("no metadata.resourceVersion in watch result (does resource support watch?)")]
    NoResourceVersion,
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
/// Watch events returned from the [`watcher`]
pub enum Event<K> {
    /// An object was added or modified
    Apply(K),
    /// An object was deleted
    ///
    /// NOTE: This should not be used for managing persistent state elsewhere, since
    /// events may be lost if the watcher is unavailable. Use Finalizers instead.
    Delete(K),
    /// The watch stream was restarted.
    ///
    /// A series of `InitApply` events are expected to follow until all matching objects
    /// have been listed. This event can be used to prepare a buffer for `InitApply` events.
    Init,
    /// Received an object during `Init`.
    ///
    /// Objects returned here are either from the initial stream using the `StreamingList` strategy,
    /// or from pages using the `ListWatch` strategy.
    ///
    /// These events can be passed up if having a complete set of objects is not a concern.
    /// If you need to wait for a complete set, please buffer these events until an `InitDone`.
    InitApply(K),
    /// The initialisation is complete.
    ///
    /// This can be used as a signal to replace buffered store contents atomically.
    /// No more `InitApply` events will happen until the next `Init` event.
    ///
    /// Any objects that were previously [`Applied`](Event::Applied) but are not listed in any of
    /// the `InitApply` events should be assumed to have been [`Deleted`](Event::Deleted).
    InitDone,
}

impl<K> Event<K> {
    /// Flattens out all objects that were added or modified in the event.
    ///
    /// `Deleted` objects are ignored, all objects mentioned by `Restarted` events are
    /// emitted individually.
    #[deprecated(
        since = "0.92.0",
        note = "unnecessary to flatten a single object. This fn will be removed in 0.96.0."
    )]
    pub fn into_iter_applied(self) -> impl Iterator<Item = K> {
        match self {
            Self::Apply(obj) | Self::InitApply(obj) => Some(obj),
            Self::Delete(_) | Self::Init | Self::InitDone => None,
        }
        .into_iter()
    }

    /// Flattens out all objects that were added, modified, or deleted in the event.
    ///
    /// Note that `Deleted` events may be missed when restarting the stream. Use finalizers
    /// or owner references instead if you care about cleaning up external resources after
    /// deleted objects.
    #[deprecated(
        since = "0.92.0",
        note = "unnecessary to flatten a single object. This fn will be removed in 0.96.0."
    )]
    pub fn into_iter_touched(self) -> impl Iterator<Item = K> {
        match self {
            Self::Apply(obj) | Self::Delete(obj) | Self::InitApply(obj) => Some(obj),
            Self::Init | Self::InitDone => None,
        }
        .into_iter()
    }

    /// Map each object in an event through a mutator fn
    ///
    /// This allows for memory optimizations in watch streams.
    /// If you are chaining a watch stream into a reflector as an in memory state store,
    /// you can control the space used by each object by dropping fields.
    ///
    /// ```no_run
    /// use k8s_openapi::api::core::v1::Pod;
    /// use kube::ResourceExt;
    /// # use kube::runtime::watcher::Event;
    /// # let event: Event<Pod> = todo!();
    /// event.modify(|pod| {
    ///     pod.managed_fields_mut().clear();
    ///     pod.annotations_mut().clear();
    ///     pod.status = None;
    /// });
    /// ```
    #[must_use]
    pub fn modify(mut self, mut f: impl FnMut(&mut K)) -> Self {
        match &mut self {
            Self::Apply(obj) | Self::Delete(obj) | Self::InitApply(obj) => (f)(obj),
            Self::Init | Self::InitDone => {} // markers, nothing to modify
        }
        self
    }
}

#[derive(Educe, Default)]
#[educe(Debug)]
/// The internal finite state machine driving the [`watcher`]
enum State<K> {
    /// The Watcher is empty, and the next [`poll`](Stream::poll_next) will start the initial LIST to get all existing objects
    #[default]
    Empty,
    /// The Watcher is in the process of paginating through the initial LIST
    InitPage {
        continue_token: Option<String>,
        objects: VecDeque<K>,
        last_bookmark: Option<String>,
    },
    /// Kubernetes 1.27 Streaming Lists
    /// The initial watch is in progress
    InitialWatch {
        #[educe(Debug(ignore))]
        stream: BoxStream<'static, kube_client::Result<WatchEvent<K>>>,
    },
    /// The initial LIST was successful, so we should move on to starting the actual watch.
    InitListed { resource_version: String },
    /// The watch is in progress, from this point we just return events from the server.
    ///
    /// If the connection is disrupted then we propagate the error but try to restart the watch stream by
    /// returning to the `InitListed` state.
    /// If we fall out of the K8s watch window then we propagate the error and fall back doing a re-list
    /// with `Empty`.
    Watching {
        resource_version: String,
        #[educe(Debug(ignore))]
        stream: BoxStream<'static, kube_client::Result<WatchEvent<K>>>,
    },
}

/// Used to control whether the watcher receives the full object, or only the
/// metadata
#[async_trait]
trait ApiMode {
    type Value: Clone;

    async fn list(&self, lp: &ListParams) -> kube_client::Result<ObjectList<Self::Value>>;
    async fn watch(
        &self,
        wp: &WatchParams,
        version: &str,
    ) -> kube_client::Result<BoxStream<'static, kube_client::Result<WatchEvent<Self::Value>>>>;
}

/// A wrapper around the `Api` of a `Resource` type that when used by the
/// watcher will return the entire (full) object
struct FullObject<'a, K> {
    api: &'a Api<K>,
}

/// Configurable list semantics for `watcher` relists
#[derive(Clone, Default, Debug, PartialEq)]
pub enum ListSemantic {
    /// List calls perform a full quorum read for most recent results
    ///
    /// Prefer this if you have strong consistency requirements. Note that this
    /// is more taxing for the apiserver and can be less scalable for the cluster.
    ///
    /// If you are observing large resource sets (such as congested `Controller` cases),
    /// you typically have a delay between the list call completing, and all the events
    /// getting processed. In such cases, it is probably worth picking `Any` over `MostRecent`,
    /// as your events are not guaranteed to be up-to-date by the time you get to them anyway.
    #[default]
    MostRecent,

    /// List calls returns cached results from apiserver
    ///
    /// This is faster and much less taxing on the apiserver, but can result
    /// in much older results than has previously observed for `Restarted` events,
    /// particularly in HA configurations, due to partitions or stale caches.
    ///
    /// This option makes the most sense for controller usage where events have
    /// some delay between being seen by the runtime, and it being sent to the reconciler.
    Any,
}

/// Configurable watcher listwatch semantics

#[derive(Clone, Default, Debug, PartialEq)]
pub enum InitialListStrategy {
    /// List first, then watch from given resouce version
    ///
    /// This is the old and default way of watching. The watcher will do a paginated list call first before watching.
    /// When using this mode, you can configure the `page_size` on the watcher.
    #[default]
    ListWatch,
    /// Kubernetes 1.27 Streaming Lists
    ///
    /// See [upstream documentation on streaming lists](https://kubernetes.io/docs/reference/using-api/api-concepts/#streaming-lists),
    /// and the [KEP](https://github.com/kubernetes/enhancements/tree/master/keps/sig-api-machinery/3157-watch-list#design-details).
    StreamingList,
}

/// Accumulates all options that can be used on the watcher invocation.
#[derive(Clone, Debug, PartialEq)]
pub struct Config {
    /// A selector to restrict the list of returned objects by their labels.
    ///
    /// Defaults to everything if `None`.
    pub label_selector: Option<String>,

    /// A selector to restrict the list of returned objects by their fields.
    ///
    /// Defaults to everything if `None`.
    pub field_selector: Option<String>,

    /// Timeout for the list/watch call.
    ///
    /// This limits the duration of the call, regardless of any activity or inactivity.
    /// If unset for a watch call, we will use 290s.
    /// We limit this to 295s due to [inherent watch limitations](https://github.com/kubernetes/kubernetes/issues/6513).
    pub timeout: Option<u32>,

    /// Semantics for list calls.
    ///
    /// Configures re-list for performance vs. consistency.
    ///
    /// NB: This option only has an effect for [`InitialListStrategy::ListWatch`].
    pub list_semantic: ListSemantic,

    /// Control how the watcher fetches the initial list of objects.
    ///
    /// - `ListWatch`: The watcher will fetch the initial list of objects using a list call.
    /// - `StreamingList`: The watcher will fetch the initial list of objects using a watch call.
    ///
    /// `StreamingList` is more efficient than `ListWatch`, but it requires the server to support
    /// streaming list bookmarks (opt-in feature gate in Kubernetes 1.27).
    ///
    /// See [upstream documentation on streaming lists](https://kubernetes.io/docs/reference/using-api/api-concepts/#streaming-lists),
    /// and the [KEP](https://github.com/kubernetes/enhancements/tree/master/keps/sig-api-machinery/3157-watch-list#design-details).
    pub initial_list_strategy: InitialListStrategy,

    /// Maximum number of objects retrieved per list operation resyncs.
    ///
    /// This can reduce the memory consumption during resyncs, at the cost of requiring more
    /// API roundtrips to complete.
    ///
    /// Defaults to 500. Note that `None` represents unbounded.
    ///
    /// NB: This option only has an effect for [`InitialListStrategy::ListWatch`].
    pub page_size: Option<u32>,

    /// Enables watch events with type "BOOKMARK".
    ///
    /// Requests watch bookmarks from the apiserver when enabled for improved watch precision and reduced list calls.
    /// This is default enabled and should generally not be turned off.
    pub bookmarks: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bookmarks: true,
            label_selector: None,
            field_selector: None,
            timeout: None,
            list_semantic: ListSemantic::default(),
            // same default page size limit as client-go
            // https://github.com/kubernetes/client-go/blob/aed71fa5cf054e1c196d67b2e21f66fd967b8ab1/tools/pager/pager.go#L31
            page_size: Some(500),
            initial_list_strategy: InitialListStrategy::ListWatch,
        }
    }
}

/// Builder interface to Config
///
/// Usage:
/// ```
/// use kube::runtime::watcher::Config;
/// let wc = Config::default()
///     .timeout(60)
///     .labels("kubernetes.io/lifecycle=spot");
/// ```
impl Config {
    /// Configure the timeout for list/watch calls
    ///
    /// This limits the duration of the call, regardless of any activity or inactivity.
    /// Defaults to 290s
    #[must_use]
    pub fn timeout(mut self, timeout_secs: u32) -> Self {
        self.timeout = Some(timeout_secs);
        self
    }

    /// Configure the selector to restrict the list of returned objects by their fields.
    ///
    /// Defaults to everything.
    /// Supports `=`, `==`, `!=`, and can be comma separated: `key1=value1,key2=value2`.
    /// The server only supports a limited number of field queries per type.
    #[must_use]
    pub fn fields(mut self, field_selector: &str) -> Self {
        self.field_selector = Some(field_selector.to_string());
        self
    }

    /// Configure the selector to restrict the list of returned objects by their labels.
    ///
    /// Defaults to everything.
    /// Supports `=`, `==`, `!=`, and can be comma separated: `key1=value1,key2=value2`.
    #[must_use]
    pub fn labels(mut self, label_selector: &str) -> Self {
        self.label_selector = Some(label_selector.to_string());
        self
    }

    /// Configure typed label selectors
    ///
    /// Configure typed selectors from [`Selector`](kube_client::core::Selector) and [`Expression`](kube_client::core::Expression) lists.
    ///
    /// ```
    /// use kube_runtime::watcher::Config;
    /// use kube_client::core::{Expression, Selector, ParseExpressionError};
    /// use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
    /// let selector: Selector = Expression::In("env".into(), ["development".into(), "sandbox".into()].into()).into();
    /// let cfg = Config::default().labels_from(&selector);
    /// let cfg = Config::default().labels_from(&Expression::Exists("foo".into()).into());
    /// let selector: Selector = LabelSelector::default().try_into()?;
    /// let cfg = Config::default().labels_from(&selector);
    /// # Ok::<(), ParseExpressionError>(())
    ///```
    #[must_use]
    pub fn labels_from(mut self, selector: &Selector) -> Self {
        self.label_selector = Some(selector.to_string());
        self
    }

    /// Sets list semantic to configure re-list performance and consistency
    ///
    /// NB: This option only has an effect for [`InitialListStrategy::ListWatch`].
    #[must_use]
    pub fn list_semantic(mut self, semantic: ListSemantic) -> Self {
        self.list_semantic = semantic;
        self
    }

    /// Sets list semantic to `Any` to improve list performance
    ///
    /// NB: This option only has an effect for [`InitialListStrategy::ListWatch`].
    #[must_use]
    pub fn any_semantic(self) -> Self {
        self.list_semantic(ListSemantic::Any)
    }

    /// Disables watch bookmarks to simplify watch handling
    ///
    /// This is not recommended to use with production watchers as it can cause desyncs.
    /// See [#219](https://github.com/kube-rs/kube/issues/219) for details.
    #[must_use]
    pub fn disable_bookmarks(mut self) -> Self {
        self.bookmarks = false;
        self
    }

    /// Limits the number of objects retrieved in each list operation during resync.
    ///
    /// This can reduce the memory consumption during resyncs, at the cost of requiring more
    /// API roundtrips to complete.
    ///
    /// NB: This option only has an effect for [`InitialListStrategy::ListWatch`].
    #[must_use]
    pub fn page_size(mut self, page_size: u32) -> Self {
        self.page_size = Some(page_size);
        self
    }

    /// Kubernetes 1.27 Streaming Lists
    /// Sets list semantic to `Stream` to make use of watch bookmarks
    #[must_use]
    pub fn streaming_lists(mut self) -> Self {
        self.initial_list_strategy = InitialListStrategy::StreamingList;
        self
    }

    /// Converts generic `watcher::Config` structure to the instance of `ListParams` used for list requests.
    fn to_list_params(&self) -> ListParams {
        let (resource_version, version_match) = match self.list_semantic {
            ListSemantic::Any => (Some("0".into()), Some(VersionMatch::NotOlderThan)),
            ListSemantic::MostRecent => (None, None),
        };
        ListParams {
            label_selector: self.label_selector.clone(),
            field_selector: self.field_selector.clone(),
            timeout: self.timeout,
            version_match,
            resource_version,
            // The watcher handles pagination internally.
            limit: self.page_size,
            continue_token: None,
        }
    }

    /// Converts generic `watcher::Config` structure to the instance of `WatchParams` used for watch requests.
    fn to_watch_params(&self) -> WatchParams {
        WatchParams {
            label_selector: self.label_selector.clone(),
            field_selector: self.field_selector.clone(),
            timeout: self.timeout,
            bookmarks: self.bookmarks,
            send_initial_events: self.initial_list_strategy == InitialListStrategy::StreamingList,
        }
    }
}

#[async_trait]
impl<K> ApiMode for FullObject<'_, K>
where
    K: Clone + Debug + DeserializeOwned + Send + 'static,
{
    type Value = K;

    async fn list(&self, lp: &ListParams) -> kube_client::Result<ObjectList<Self::Value>> {
        self.api.list(lp).await
    }

    async fn watch(
        &self,
        wp: &WatchParams,
        version: &str,
    ) -> kube_client::Result<BoxStream<'static, kube_client::Result<WatchEvent<Self::Value>>>> {
        self.api.watch(wp, version).await.map(StreamExt::boxed)
    }
}

/// A wrapper around the `Api` of a `Resource` type that when used by the
/// watcher will return only the metadata associated with an object
struct MetaOnly<'a, K> {
    api: &'a Api<K>,
}

#[async_trait]
impl<K> ApiMode for MetaOnly<'_, K>
where
    K: Clone + Debug + DeserializeOwned + Send + 'static,
{
    type Value = PartialObjectMeta<K>;

    async fn list(&self, lp: &ListParams) -> kube_client::Result<ObjectList<Self::Value>> {
        self.api.list_metadata(lp).await
    }

    async fn watch(
        &self,
        wp: &WatchParams,
        version: &str,
    ) -> kube_client::Result<BoxStream<'static, kube_client::Result<WatchEvent<Self::Value>>>> {
        self.api.watch_metadata(wp, version).await.map(StreamExt::boxed)
    }
}

/// Progresses the watcher a single step, returning (event, state)
///
/// This function should be trampolined: if event == `None`
/// then the function should be called again until it returns a Some.
#[allow(clippy::too_many_lines)] // for now
async fn step_trampolined<A>(
    api: &A,
    wc: &Config,
    state: State<A::Value>,
) -> (Option<Result<Event<A::Value>>>, State<A::Value>)
where
    A: ApiMode,
    A::Value: Resource + 'static,
{
    match state {
        State::Empty => match wc.initial_list_strategy {
            InitialListStrategy::ListWatch => (Some(Ok(Event::Init)), State::InitPage {
                continue_token: None,
                objects: VecDeque::default(),
                last_bookmark: None,
            }),
            InitialListStrategy::StreamingList => match api.watch(&wc.to_watch_params(), "0").await {
                Ok(stream) => (None, State::InitialWatch { stream }),
                Err(err) => {
                    if std::matches!(err, ClientErr::Api(ErrorResponse { code: 403, .. })) {
                        warn!("watch initlist error with 403: {err:?}");
                    } else {
                        debug!("watch initlist error: {err:?}");
                    }
                    (Some(Err(Error::WatchStartFailed(err))), State::default())
                }
            },
        },
        State::InitPage {
            continue_token,
            mut objects,
            last_bookmark,
        } => {
            if let Some(next) = objects.pop_front() {
                return (Some(Ok(Event::InitApply(next))), State::InitPage {
                    continue_token,
                    objects,
                    last_bookmark,
                });
            }
            // check if we need to perform more pages
            if continue_token.is_none() {
                if let Some(resource_version) = last_bookmark {
                    // we have drained the last page - move on to next stage
                    return (Some(Ok(Event::InitDone)), State::InitListed { resource_version });
                }
            }
            let mut lp = wc.to_list_params();
            lp.continue_token = continue_token;
            match api.list(&lp).await {
                Ok(list) => {
                    let last_bookmark = list.metadata.resource_version.filter(|s| !s.is_empty());
                    let continue_token = list.metadata.continue_.filter(|s| !s.is_empty());
                    if last_bookmark.is_none() && continue_token.is_none() {
                        return (Some(Err(Error::NoResourceVersion)), State::Empty);
                    }
                    // Buffer page here, causing us to return to this enum branch (State::InitPage)
                    // until the objects buffer has drained
                    (None, State::InitPage {
                        continue_token,
                        objects: list.items.into_iter().collect(),
                        last_bookmark,
                    })
                }
                Err(err) => {
                    if std::matches!(err, ClientErr::Api(ErrorResponse { code: 403, .. })) {
                        warn!("watch list error with 403: {err:?}");
                    } else {
                        debug!("watch list error: {err:?}");
                    }
                    (Some(Err(Error::InitialListFailed(err))), State::Empty)
                }
            }
        }
        State::InitialWatch { mut stream } => {
            match stream.next().await {
                Some(Ok(WatchEvent::Added(obj) | WatchEvent::Modified(obj))) => {
                    (Some(Ok(Event::InitApply(obj))), State::InitialWatch { stream })
                }
                Some(Ok(WatchEvent::Deleted(_obj))) => {
                    // Kubernetes claims these events are impossible
                    // https://kubernetes.io/docs/reference/using-api/api-concepts/#streaming-lists
                    error!("got deleted event during initial watch. this is a bug");
                    (None, State::InitialWatch { stream })
                }
                Some(Ok(WatchEvent::Bookmark(bm))) => {
                    let marks_initial_end = bm.metadata.annotations.contains_key("k8s.io/initial-events-end");
                    if marks_initial_end {
                        (Some(Ok(Event::InitDone)), State::Watching {
                            resource_version: bm.metadata.resource_version,
                            stream,
                        })
                    } else {
                        (None, State::InitialWatch { stream })
                    }
                }
                Some(Ok(WatchEvent::Error(err))) => {
                    // HTTP GONE, means we have desynced and need to start over and re-list :(
                    let new_state = if err.code == 410 {
                        State::default()
                    } else {
                        State::InitialWatch { stream }
                    };
                    if err.code == 403 {
                        warn!("watcher watchevent error 403: {err:?}");
                    } else {
                        debug!("error watchevent error: {err:?}");
                    }
                    (Some(Err(Error::WatchError(err))), new_state)
                }
                Some(Err(err)) => {
                    if std::matches!(err, ClientErr::Api(ErrorResponse { code: 403, .. })) {
                        warn!("watcher error 403: {err:?}");
                    } else {
                        debug!("watcher error: {err:?}");
                    }
                    (Some(Err(Error::WatchFailed(err))), State::InitialWatch { stream })
                }
                None => (None, State::default()),
            }
        }
        State::InitListed { resource_version } => {
            match api.watch(&wc.to_watch_params(), &resource_version).await {
                Ok(stream) => (None, State::Watching {
                    resource_version,
                    stream,
                }),
                Err(err) => {
                    if std::matches!(err, ClientErr::Api(ErrorResponse { code: 403, .. })) {
                        warn!("watch initlist error with 403: {err:?}");
                    } else {
                        debug!("watch initlist error: {err:?}");
                    }
                    (Some(Err(Error::WatchStartFailed(err))), State::InitListed {
                        resource_version,
                    })
                }
            }
        }
        State::Watching {
            resource_version,
            mut stream,
        } => match stream.next().await {
            Some(Ok(WatchEvent::Added(obj) | WatchEvent::Modified(obj))) => {
                let resource_version = obj.resource_version().unwrap_or_default();
                if resource_version.is_empty() {
                    (Some(Err(Error::NoResourceVersion)), State::default())
                } else {
                    (Some(Ok(Event::Apply(obj))), State::Watching {
                        resource_version,
                        stream,
                    })
                }
            }
            Some(Ok(WatchEvent::Deleted(obj))) => {
                let resource_version = obj.resource_version().unwrap_or_default();
                if resource_version.is_empty() {
                    (Some(Err(Error::NoResourceVersion)), State::default())
                } else {
                    (Some(Ok(Event::Delete(obj))), State::Watching {
                        resource_version,
                        stream,
                    })
                }
            }
            Some(Ok(WatchEvent::Bookmark(bm))) => (None, State::Watching {
                resource_version: bm.metadata.resource_version,
                stream,
            }),
            Some(Ok(WatchEvent::Error(err))) => {
                // HTTP GONE, means we have desynced and need to start over and re-list :(
                let new_state = if err.code == 410 {
                    State::default()
                } else {
                    State::Watching {
                        resource_version,
                        stream,
                    }
                };
                if err.code == 403 {
                    warn!("watcher watchevent error 403: {err:?}");
                } else {
                    debug!("error watchevent error: {err:?}");
                }
                (Some(Err(Error::WatchError(err))), new_state)
            }
            Some(Err(err)) => {
                if std::matches!(err, ClientErr::Api(ErrorResponse { code: 403, .. })) {
                    warn!("watcher error 403: {err:?}");
                } else {
                    debug!("watcher error: {err:?}");
                }
                (Some(Err(Error::WatchFailed(err))), State::Watching {
                    resource_version,
                    stream,
                })
            }
            None => (None, State::InitListed { resource_version }),
        },
    }
}

/// Trampoline helper for `step_trampolined`
async fn step<A>(
    api: &A,
    config: &Config,
    mut state: State<A::Value>,
) -> (Result<Event<A::Value>>, State<A::Value>)
where
    A: ApiMode,
    A::Value: Resource + 'static,
{
    loop {
        match step_trampolined(api, config, state).await {
            (Some(result), new_state) => return (result, new_state),
            (None, new_state) => state = new_state,
        }
    }
}

/// Watches a Kubernetes Resource for changes continuously
///
/// Compared to [`Api::watch`], this automatically tries to recover the stream upon errors.
///
/// Errors from the underlying watch are propagated, after which the stream will go into recovery mode on the next poll.
/// You can apply your own backoff by not polling the stream for a duration after errors.
/// Keep in mind that some [`TryStream`](futures::TryStream) combinators (such as
/// [`try_for_each`](futures::TryStreamExt::try_for_each) and [`try_concat`](futures::TryStreamExt::try_concat))
/// will terminate eagerly as soon as they receive an [`Err`].
///
/// The events are intended to provide a safe input interface for a state store like a [`reflector`].
/// Direct users may want to use [`WatchStreamExt`] for higher-level constructs.
///
/// ```no_run
/// use kube::{
///   api::{Api, ResourceExt}, Client,
///   runtime::{watcher, WatchStreamExt}
/// };
/// use k8s_openapi::api::core::v1::Pod;
/// use futures::TryStreamExt;
/// #[tokio::main]
/// async fn main() -> Result<(), watcher::Error> {
///     let client = Client::try_default().await.unwrap();
///     let pods: Api<Pod> = Api::namespaced(client, "apps");
///
///     watcher(pods, watcher::Config::default()).applied_objects()
///         .try_for_each(|p| async move {
///          println!("Applied: {}", p.name_any());
///             Ok(())
///         })
///         .await?;
///     Ok(())
/// }
/// ```
/// [`WatchStreamExt`]: super::WatchStreamExt
/// [`reflector`]: super::reflector::reflector
/// [`Api::watch`]: kube_client::Api::watch
///
/// # Recovery
///
/// The stream will attempt to be recovered on the next poll after an [`Err`] is returned.
/// This will normally happen immediately, but you can use [`StreamBackoff`](crate::utils::StreamBackoff)
/// to introduce an artificial delay. [`default_backoff`] returns a suitable default set of parameters.
///
/// If the watch connection is interrupted, then `watcher` will attempt to restart the watch using the last
/// [resource version](https://kubernetes.io/docs/reference/using-api/api-concepts/#efficient-detection-of-changes)
/// that we have seen on the stream. If this is successful then the stream is simply resumed from where it left off.
/// If this fails because the resource version is no longer valid then we start over with a new stream, starting with
/// an [`Event::Init`]. The internals mechanics of recovery should be considered an implementation detail.
pub fn watcher<K: Resource + Clone + DeserializeOwned + Debug + Send + 'static>(
    api: Api<K>,
    watcher_config: Config,
) -> impl Stream<Item = Result<Event<K>>> + Send {
    futures::stream::unfold(
        (api, watcher_config, State::default()),
        |(api, watcher_config, state)| async {
            let (event, state) = step(&FullObject { api: &api }, &watcher_config, state).await;
            Some((event, (api, watcher_config, state)))
        },
    )
}

/// Watches a Kubernetes Resource for changes continuously and receives only the
/// metadata
///
/// Compared to [`Api::watch_metadata`], this automatically tries to recover the stream upon errors.
///
/// Errors from the underlying watch are propagated, after which the stream will go into recovery mode on the next poll.
/// You can apply your own backoff by not polling the stream for a duration after errors.
/// Keep in mind that some [`TryStream`](futures::TryStream) combinators (such as
/// [`try_for_each`](futures::TryStreamExt::try_for_each) and [`try_concat`](futures::TryStreamExt::try_concat))
/// will terminate eagerly as soon as they receive an [`Err`].
///
/// The events are intended to provide a safe input interface for a state store like a [`reflector`].
/// Direct users may want to use [`WatchStreamExt`] for higher-level constructs.
///
/// ```no_run
/// use kube::{
///   api::{Api, ResourceExt}, Client,
///   runtime::{watcher, metadata_watcher, WatchStreamExt}
/// };
/// use k8s_openapi::api::core::v1::Pod;
/// use futures::TryStreamExt;
/// #[tokio::main]
/// async fn main() -> Result<(), watcher::Error> {
///     let client = Client::try_default().await.unwrap();
///     let pods: Api<Pod> = Api::namespaced(client, "apps");
///
///     metadata_watcher(pods, watcher::Config::default()).applied_objects()
///         .try_for_each(|p| async move {
///          println!("Applied: {}", p.name_any());
///             Ok(())
///         })
///         .await?;
///     Ok(())
/// }
/// ```
/// [`WatchStreamExt`]: super::WatchStreamExt
/// [`reflector`]: super::reflector::reflector
/// [`Api::watch`]: kube_client::Api::watch
///
/// # Recovery
///
/// The stream will attempt to be recovered on the next poll after an [`Err`] is returned.
/// This will normally happen immediately, but you can use [`StreamBackoff`](crate::utils::StreamBackoff)
/// to introduce an artificial delay. [`default_backoff`] returns a suitable default set of parameters.
///
/// If the watch connection is interrupted, then `watcher` will attempt to restart the watch using the last
/// [resource version](https://kubernetes.io/docs/reference/using-api/api-concepts/#efficient-detection-of-changes)
/// that we have seen on the stream. If this is successful then the stream is simply resumed from where it left off.
/// If this fails because the resource version is no longer valid then we start over with a new stream, starting with
/// an [`Event::Init`]. The internals mechanics of recovery should be considered an implementation detail.
#[allow(clippy::module_name_repetitions)]
pub fn metadata_watcher<K: Resource + Clone + DeserializeOwned + Debug + Send + 'static>(
    api: Api<K>,
    watcher_config: Config,
) -> impl Stream<Item = Result<Event<PartialObjectMeta<K>>>> + Send {
    futures::stream::unfold(
        (api, watcher_config, State::default()),
        |(api, watcher_config, state)| async {
            let (event, state) = step(&MetaOnly { api: &api }, &watcher_config, state).await;
            Some((event, (api, watcher_config, state)))
        },
    )
}

/// Watch a single named object for updates
///
/// Emits `None` if the object is deleted (or not found), and `Some` if an object is updated (or created/found).
///
/// Often invoked indirectly via [`await_condition`](crate::wait::await_condition()).
///
/// ## Scope Warning
///
/// When using this with an `Api::all` on namespaced resources there is a chance of duplicated names.
/// To avoid getting confusing / wrong answers for this, use `Api::namespaced` bound to a specific namespace
/// when watching for transitions to namespaced objects.
pub fn watch_object<K: Resource + Clone + DeserializeOwned + Debug + Send + 'static>(
    api: Api<K>,
    name: &str,
) -> impl Stream<Item = Result<Option<K>>> + Send {
    // filtering by object name in given scope, so there's at most one matching object
    // footgun: Api::all may generate events from namespaced objects with the same name in different namespaces
    let fields = format!("metadata.name={name}");
    watcher(api, Config::default().fields(&fields))
        // The `obj_seen` state is used to track whether the object exists in each Init / InitApply / InitDone
        // sequence of events. If the object wasn't seen in any particular sequence it is treated as deleted and
        // `None` is emitted when the InitDone event is received.
        //
        // The first check ensures `None` is emitted if the object was already gone (or not found), subsequent
        // checks ensure `None` is emitted even if for some reason the Delete event wasn't received, which
        // could happen given K8S events aren't guaranteed delivery.
        .scan(false, |obj_seen, event| {
            if matches!(event, Ok(Event::Init)) {
                *obj_seen = false;
            } else if matches!(event, Ok(Event::InitApply(_))) {
                *obj_seen = true;
            }
            future::ready(Some((*obj_seen, event)))
        })
        .filter_map(|(obj_seen, event)| async move {
            match event {
                // Pass up `Some` for Found / Updated
                Ok(Event::Apply(obj) | Event::InitApply(obj)) => Some(Ok(Some(obj))),
                // Pass up `None` for Deleted
                Ok(Event::Delete(_)) => Some(Ok(None)),
                // Pass up `None` if the object wasn't seen in the initial list
                Ok(Event::InitDone) if !obj_seen => Some(Ok(None)),
                // Ignore marker events
                Ok(Event::Init | Event::InitDone) => None,
                // Bubble up errors
                Err(err) => Some(Err(err)),
            }
        })
}

struct ExponentialBackoff {
    inner: backon::ExponentialBackoff,
    min_delay: Duration,
    max_delay: Duration,
    factor: f32,
    enable_jitter: bool,
}

impl ExponentialBackoff {
    fn new(min_delay: Duration, max_delay: Duration, factor: f32, enable_jitter: bool) -> Self {
        Self {
            inner: backon::ExponentialBuilder::default()
                .with_min_delay(min_delay)
                .with_max_delay(max_delay)
                .with_factor(factor)
                .with_jitter()
                .build(),
            min_delay,
            max_delay,
            factor,
            enable_jitter,
        }
    }
}

impl Backoff for ExponentialBackoff {
    fn reset(&mut self) {
        let mut builder = backon::ExponentialBuilder::default()
            .with_min_delay(self.min_delay)
            .with_max_delay(self.max_delay)
            .with_factor(self.factor);
        if self.enable_jitter {
            builder = builder.with_jitter();
        }
        self.inner = builder.build();
    }
}

impl Iterator for ExponentialBackoff {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

/// Default watcher backoff inspired by Kubernetes' client-go.
///
/// The parameters currently optimize for being kind to struggling apiservers.
/// The exact parameters are taken from
/// [client-go's reflector source](https://github.com/kubernetes/client-go/blob/980663e185ab6fc79163b1c2565034f6d58368db/tools/cache/reflector.go#L177-L181)
/// and should not be considered stable.
///
/// This struct implements [`Backoff`] and is the default strategy used
/// when calling `WatchStreamExt::default_backoff`. If you need to create
/// this manually then [`DefaultBackoff::default`] can be used.
pub struct DefaultBackoff(Strategy);
type Strategy = ResetTimerBackoff<ExponentialBackoff>;

impl Default for DefaultBackoff {
    fn default() -> Self {
        Self(ResetTimerBackoff::new(
            ExponentialBackoff::new(Duration::from_millis(800), Duration::from_secs(30), 2.0, true),
            Duration::from_secs(120),
        ))
    }
}

impl Iterator for DefaultBackoff {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl Backoff for DefaultBackoff {
    fn reset(&mut self) {
        self.0.reset();
    }
}
