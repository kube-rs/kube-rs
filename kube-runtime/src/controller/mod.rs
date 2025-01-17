//! Runs a user-supplied reconciler function on objects when they (or related objects) are updated

use self::runner::Runner;
use crate::{
    reflector::{
        self, reflector,
        store::{Store, Writer},
        ObjectRef,
    },
    scheduler::{debounced_scheduler, ScheduleRequest},
    utils::{
        trystream_try_via, Backoff, CancelableJoinHandle, KubeRuntimeStreamExt, StreamBackoff, WatchStreamExt,
    },
    watcher::{self, metadata_watcher, watcher, DefaultBackoff},
};
use educe::Educe;
use futures::{
    channel,
    future::{self, BoxFuture},
    stream, FutureExt, Stream, StreamExt, TryFuture, TryFutureExt, TryStream, TryStreamExt,
};
use kube_client::api::{Api, DynamicObject, Resource};
use pin_project::pin_project;
use serde::de::DeserializeOwned;
use std::{
    fmt::{Debug, Display},
    future::Future,
    hash::Hash,
    sync::Arc,
    task::{ready, Poll},
    time::Duration,
};
use stream::BoxStream;
use thiserror::Error;
use tokio::{runtime::Handle, time::Instant};
use tracing::{info_span, Instrument};

mod future_hash_map;
mod runner;

pub type RunnerError = runner::Error<reflector::store::WriterDropped>;

#[derive(Debug, Error)]
pub enum Error<ReconcilerErr: 'static, QueueErr: 'static> {
    #[error("tried to reconcile object {0} that was not found in local store")]
    ObjectNotFound(ObjectRef<DynamicObject>),
    #[error("reconciler for object {1} failed")]
    ReconcilerFailed(#[source] ReconcilerErr, ObjectRef<DynamicObject>),
    #[error("event queue error")]
    QueueError(#[source] QueueErr),
    #[error("runner error")]
    RunnerError(#[source] RunnerError),
}

/// Results of the reconciliation attempt
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Action {
    /// Whether (and when) to next trigger the reconciliation if no external watch triggers hit
    ///
    /// For example, use this to query external systems for updates, expire time-limited resources, or
    /// (in your `error_policy`) retry after errors.
    requeue_after: Option<Duration>,
}

impl Action {
    /// Action to the reconciliation at this time even if no external watch triggers hit
    ///
    /// This is the best-practice action that ensures eventual consistency of your controller
    /// even in the case of missed changes (which can happen).
    ///
    /// Watch events are not normally missed, so running this once per hour (`Default`) as a fallback is reasonable.
    #[must_use]
    pub fn requeue(duration: Duration) -> Self {
        Self {
            requeue_after: Some(duration),
        }
    }

    /// Do nothing until a change is detected
    ///
    /// This stops the controller periodically reconciling this object until a relevant watch event
    /// was **detected**.
    ///
    /// **Warning**: If you have watch desyncs, it is possible to miss changes entirely.
    /// It is therefore not recommended to disable requeuing this way, unless you have
    /// frequent changes to the underlying object, or some other hook to retain eventual consistency.
    #[must_use]
    pub fn await_change() -> Self {
        Self { requeue_after: None }
    }
}

/// Helper for building custom trigger filters, see the implementations of [`trigger_self`] and [`trigger_owners`] for some examples.
pub fn trigger_with<T, K, I, S>(
    stream: S,
    mapper: impl Fn(T) -> I,
) -> impl Stream<Item = Result<ReconcileRequest<K>, S::Error>>
where
    S: TryStream<Ok = T>,
    I: IntoIterator,
    I::Item: Into<ReconcileRequest<K>>,
    K: Resource,
{
    stream
        .map_ok(move |obj| stream::iter(mapper(obj).into_iter().map(Into::into).map(Ok)))
        .try_flatten()
}

/// Enqueues the object itself for reconciliation
pub fn trigger_self<K, S>(
    stream: S,
    dyntype: K::DynamicType,
) -> impl Stream<Item = Result<ReconcileRequest<K>, S::Error>>
where
    S: TryStream<Ok = K>,
    K: Resource,
    K::DynamicType: Clone,
{
    trigger_with(stream, move |obj| {
        Some(ReconcileRequest {
            obj_ref: ObjectRef::from_obj_with(&obj, dyntype.clone()),
            reason: ReconcileReason::ObjectUpdated,
        })
    })
}

/// Enqueues the object itself for reconciliation when the object is behind a
/// shared pointer
#[cfg(feature = "unstable-runtime-subscribe")]
fn trigger_self_shared<K, S>(
    stream: S,
    dyntype: K::DynamicType,
) -> impl Stream<Item = Result<ReconcileRequest<K>, S::Error>>
where
    // Input stream has item as some Arc'd Resource (via
    // Controller::for_shared_stream)
    S: TryStream<Ok = Arc<K>>,
    K: Resource,
    K::DynamicType: Clone,
{
    trigger_with(stream, move |obj| {
        Some(ReconcileRequest {
            obj_ref: ObjectRef::from_obj_with(obj.as_ref(), dyntype.clone()),
            reason: ReconcileReason::ObjectUpdated,
        })
    })
}

/// Enqueues any mapper returned `K` types for reconciliation
fn trigger_others<S, K, I>(
    stream: S,
    mapper: impl Fn(S::Ok) -> I + Sync + Send + 'static,
    dyntype: <S::Ok as Resource>::DynamicType,
) -> impl Stream<Item = Result<ReconcileRequest<K>, S::Error>>
where
    // Input stream has items as some Resource (via Controller::watches)
    S: TryStream,
    S::Ok: Resource,
    <S::Ok as Resource>::DynamicType: Clone,
    // Output stream is requests for the root type K
    K: Resource,
    K::DynamicType: Clone,
    // but the mapper can produce many of them
    I: 'static + IntoIterator<Item = ObjectRef<K>>,
    I::IntoIter: Send,
{
    trigger_with(stream, move |obj| {
        let watch_ref = ObjectRef::from_obj_with(&obj, dyntype.clone()).erase();
        mapper(obj)
            .into_iter()
            .map(move |mapped_obj_ref| ReconcileRequest {
                obj_ref: mapped_obj_ref,
                reason: ReconcileReason::RelatedObjectUpdated {
                    obj_ref: Box::new(watch_ref.clone()),
                },
            })
    })
}

/// Enqueues any mapper returned `Arc<K>` types for reconciliation
#[cfg(feature = "unstable-runtime-subscribe")]
fn trigger_others_shared<S, O, K, I>(
    stream: S,
    mapper: impl Fn(S::Ok) -> I + Sync + Send + 'static,
    dyntype: O::DynamicType,
) -> impl Stream<Item = Result<ReconcileRequest<K>, S::Error>>
where
    // Input is some shared resource (`Arc<O>`) obtained via `reflect`
    S: TryStream<Ok = Arc<O>>,
    O: Resource,
    O::DynamicType: Clone,
    // Output stream is requests for the root type K
    K: Resource,
    K::DynamicType: Clone,
    // but the mapper can produce many of them
    I: 'static + IntoIterator<Item = ObjectRef<K>>,
    I::IntoIter: Send,
{
    trigger_with(stream, move |obj| {
        let watch_ref = ObjectRef::from_obj_with(obj.as_ref(), dyntype.clone()).erase();
        mapper(obj)
            .into_iter()
            .map(move |mapped_obj_ref| ReconcileRequest {
                obj_ref: mapped_obj_ref,
                reason: ReconcileReason::RelatedObjectUpdated {
                    obj_ref: Box::new(watch_ref.clone()),
                },
            })
    })
}

/// Enqueues any owners of type `KOwner` for reconciliation
pub fn trigger_owners<KOwner, S>(
    stream: S,
    owner_type: KOwner::DynamicType,
    child_type: <S::Ok as Resource>::DynamicType,
) -> impl Stream<Item = Result<ReconcileRequest<KOwner>, S::Error>>
where
    S: TryStream,
    S::Ok: Resource,
    <S::Ok as Resource>::DynamicType: Clone,
    KOwner: Resource,
    KOwner::DynamicType: Clone,
{
    let mapper = move |obj: S::Ok| {
        let meta = obj.meta().clone();
        let ns = meta.namespace;
        let owner_type = owner_type.clone();
        meta.owner_references
            .into_iter()
            .flatten()
            .filter_map(move |owner| ObjectRef::from_owner_ref(ns.as_deref(), &owner, owner_type.clone()))
    };
    trigger_others(stream, mapper, child_type)
}

// TODO: do we really need to deal with a trystream? can we simplify this at
// all?
/// Enqueues any owners of type `KOwner` for reconciliation based on a stream of
/// owned `K` objects
#[cfg(feature = "unstable-runtime-subscribe")]
fn trigger_owners_shared<KOwner, S, K>(
    stream: S,
    owner_type: KOwner::DynamicType,
    child_type: K::DynamicType,
) -> impl Stream<Item = Result<ReconcileRequest<KOwner>, S::Error>>
where
    S: TryStream<Ok = Arc<K>>,
    K: Resource,
    K::DynamicType: Clone,
    KOwner: Resource,
    KOwner::DynamicType: Clone,
{
    let mapper = move |obj: S::Ok| {
        let meta = obj.meta().clone();
        let ns = meta.namespace;
        let owner_type = owner_type.clone();
        meta.owner_references
            .into_iter()
            .flatten()
            .filter_map(move |owner| ObjectRef::from_owner_ref(ns.as_deref(), &owner, owner_type.clone()))
    };
    trigger_others_shared(stream, mapper, child_type)
}

/// A request to reconcile an object, annotated with why that request was made.
///
/// NOTE: The reason is ignored for comparison purposes. This means that, for example,
/// an object can only occupy one scheduler slot, even if it has been scheduled for multiple reasons.
/// In this case, only *the first* reason is stored.
#[derive(Educe)]
#[educe(
    Debug(bound("K::DynamicType: Debug")),
    Clone(bound("K::DynamicType: Clone")),
    PartialEq(bound("K::DynamicType: PartialEq")),
    Hash(bound("K::DynamicType: Hash"))
)]
pub struct ReconcileRequest<K: Resource> {
    pub obj_ref: ObjectRef<K>,
    #[educe(PartialEq(ignore), Hash(ignore))]
    pub reason: ReconcileReason,
}

impl<K: Resource> Eq for ReconcileRequest<K> where K::DynamicType: Eq {}

impl<K: Resource> From<ObjectRef<K>> for ReconcileRequest<K> {
    fn from(obj_ref: ObjectRef<K>) -> Self {
        ReconcileRequest {
            obj_ref,
            reason: ReconcileReason::Unknown,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ReconcileReason {
    Unknown,
    ObjectUpdated,
    RelatedObjectUpdated { obj_ref: Box<ObjectRef<DynamicObject>> },
    ReconcilerRequestedRetry,
    ErrorPolicyRequestedRetry,
    BulkReconcile,
    Custom { reason: String },
}

impl Display for ReconcileReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReconcileReason::Unknown => f.write_str("unknown"),
            ReconcileReason::ObjectUpdated => f.write_str("object updated"),
            ReconcileReason::RelatedObjectUpdated { obj_ref: object } => {
                f.write_fmt(format_args!("related object updated: {object}"))
            }
            ReconcileReason::BulkReconcile => f.write_str("bulk reconcile requested"),
            ReconcileReason::ReconcilerRequestedRetry => f.write_str("reconciler requested retry"),
            ReconcileReason::ErrorPolicyRequestedRetry => f.write_str("error policy requested retry"),
            ReconcileReason::Custom { reason } => f.write_str(reason),
        }
    }
}

const APPLIER_REQUEUE_BUF_SIZE: usize = 100;

/// Apply a reconciler to an input stream, with a given retry policy
///
/// Takes a `store` parameter for the core objects, which should usually be updated by a [`reflector()`].
///
/// The `queue` indicates which objects should be reconciled. For the core objects this will usually be
/// the [`reflector()`] (piped through [`trigger_self`]). If your core objects own any subobjects then you
/// can also make them trigger reconciliations by [merging](`futures::stream::select`) the [`reflector()`]
/// with a [`watcher()`] or [`reflector()`] for the subobject.
///
/// This is the "hard-mode" version of [`Controller`], which allows you some more customization
/// (such as triggering from arbitrary [`Stream`]s), at the cost of being a bit more verbose.
#[allow(clippy::needless_pass_by_value)]
#[allow(clippy::type_complexity)]
pub fn applier<K, QueueStream, ReconcilerFut, Ctx>(
    mut reconciler: impl FnMut(Arc<K>, Arc<Ctx>) -> ReconcilerFut,
    error_policy: impl Fn(Arc<K>, &ReconcilerFut::Error, Arc<Ctx>) -> Action,
    context: Arc<Ctx>,
    store: Store<K>,
    queue: QueueStream,
    config: Config,
) -> impl Stream<Item = Result<(ObjectRef<K>, Action), Error<ReconcilerFut::Error, QueueStream::Error>>>
where
    K: Clone + Resource + 'static,
    K::DynamicType: Debug + Eq + Hash + Clone + Unpin,
    ReconcilerFut: TryFuture<Ok = Action> + Unpin,
    ReconcilerFut::Error: std::error::Error + 'static,
    QueueStream: TryStream,
    QueueStream::Ok: Into<ReconcileRequest<K>>,
    QueueStream::Error: std::error::Error + 'static,
{
    let (scheduler_shutdown_tx, scheduler_shutdown_rx) = channel::oneshot::channel();
    let (scheduler_tx, scheduler_rx) =
        channel::mpsc::channel::<ScheduleRequest<ReconcileRequest<K>>>(APPLIER_REQUEUE_BUF_SIZE);
    let error_policy = Arc::new(error_policy);
    let delay_store = store.clone();
    // Create a stream of ObjectRefs that need to be reconciled
    trystream_try_via(
        // input: stream combining scheduled tasks and user specified inputs event
        Box::pin(stream::select(
            // 1. inputs from users queue stream
            queue
                .map_err(Error::QueueError)
                .map_ok(|request| ScheduleRequest {
                    message: request.into(),
                    run_at: Instant::now(),
                })
                .on_complete(async move {
                    // On error: scheduler has already been shut down and there is nothing for us to do
                    let _ = scheduler_shutdown_tx.send(());
                    tracing::debug!("applier queue terminated, starting graceful shutdown")
                }),
            // 2. requests sent to scheduler_tx
            scheduler_rx
                .map(Ok)
                .take_until(scheduler_shutdown_rx)
                .on_complete(async { tracing::debug!("applier scheduler consumer terminated") }),
        )),
        // all the Oks from the select gets passed through the scheduler stream, and are then executed
        move |s| {
            Runner::new(
                debounced_scheduler(s, config.debounce),
                config.concurrency,
                move |request| {
                    let request = request.clone();
                    match store.get(&request.obj_ref) {
                        Some(obj) => {
                            let scheduler_tx = scheduler_tx.clone();
                            let error_policy_ctx = context.clone();
                            let error_policy = error_policy.clone();
                            let reconciler_span = info_span!(
                                "reconciling object",
                                "object.ref" = %request.obj_ref,
                                object.reason = %request.reason
                            );
                            reconciler_span
                                .in_scope(|| reconciler(Arc::clone(&obj), context.clone()))
                                .into_future()
                                .then(move |res| {
                                    let error_policy = error_policy;
                                    RescheduleReconciliation::new(
                                        res,
                                        |err| error_policy(obj, err, error_policy_ctx),
                                        request.obj_ref.clone(),
                                        scheduler_tx,
                                    )
                                    // Reconciler errors are OK from the applier's PoV, we need to apply the error policy
                                    // to them separately
                                    .map(|res| Ok((request.obj_ref, res)))
                                })
                                .instrument(reconciler_span)
                                .left_future()
                        }
                        None => std::future::ready(Err(Error::ObjectNotFound(request.obj_ref.erase())))
                            .right_future(),
                    }
                },
            )
            .delay_tasks_until(async move {
                tracing::debug!("applier runner held until store is ready");
                let res = delay_store.wait_until_ready().await;
                tracing::debug!("store is ready, starting runner");
                res
            })
            .map(|runner_res| runner_res.unwrap_or_else(|err| Err(Error::RunnerError(err))))
            .on_complete(async { tracing::debug!("applier runner terminated") })
        },
    )
    .on_complete(async { tracing::debug!("applier runner-merge terminated") })
    // finally, for each completed reconcile call:
    .and_then(move |(obj_ref, reconciler_result)| async move {
        match reconciler_result {
            Ok(action) => Ok((obj_ref, action)),
            Err(err) => Err(Error::ReconcilerFailed(err, obj_ref.erase())),
        }
    })
    .on_complete(async { tracing::debug!("applier terminated") })
}

/// Internal helper [`Future`] that reschedules reconciliation of objects (if required), in the scheduled context of the reconciler
///
/// This could be an `async fn`, but isn't because we want it to be [`Unpin`]
#[pin_project]
#[must_use]
struct RescheduleReconciliation<K: Resource, ReconcilerErr> {
    reschedule_tx: channel::mpsc::Sender<ScheduleRequest<ReconcileRequest<K>>>,

    reschedule_request: Option<ScheduleRequest<ReconcileRequest<K>>>,
    result: Option<Result<Action, ReconcilerErr>>,
}

impl<K, ReconcilerErr> RescheduleReconciliation<K, ReconcilerErr>
where
    K: Resource,
{
    fn new(
        result: Result<Action, ReconcilerErr>,
        error_policy: impl FnOnce(&ReconcilerErr) -> Action,
        obj_ref: ObjectRef<K>,
        reschedule_tx: channel::mpsc::Sender<ScheduleRequest<ReconcileRequest<K>>>,
    ) -> Self {
        let reconciler_finished_at = Instant::now();

        let (action, reschedule_reason) = result.as_ref().map_or_else(
            |err| (error_policy(err), ReconcileReason::ErrorPolicyRequestedRetry),
            |action| (action.clone(), ReconcileReason::ReconcilerRequestedRetry),
        );

        Self {
            reschedule_tx,
            reschedule_request: action.requeue_after.map(|requeue_after| ScheduleRequest {
                message: ReconcileRequest {
                    obj_ref,
                    reason: reschedule_reason,
                },
                run_at: reconciler_finished_at
                    .checked_add(requeue_after)
                    .unwrap_or_else(crate::scheduler::far_future),
            }),
            result: Some(result),
        }
    }
}

impl<K, ReconcilerErr> Future for RescheduleReconciliation<K, ReconcilerErr>
where
    K: Resource,
{
    type Output = Result<Action, ReconcilerErr>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if this.reschedule_request.is_some() {
            let rescheduler_ready = ready!(this.reschedule_tx.poll_ready(cx));
            let reschedule_request = this
                .reschedule_request
                .take()
                .expect("PostReconciler::reschedule_request was taken during processing");
            // Failure to schedule item = in graceful shutdown mode, ignore
            if let Ok(()) = rescheduler_ready {
                let _ = this.reschedule_tx.start_send(reschedule_request);
            }
        }

        Poll::Ready(
            this.result
                .take()
                .expect("PostReconciler::result was already taken"),
        )
    }
}

/// Accumulates all options that can be used on a [`Controller`] invocation.
#[derive(Clone, Debug, Default)]
pub struct Config {
    debounce: Duration,
    concurrency: u16,
}

impl Config {
    /// The debounce duration used to deduplicate reconciliation requests.
    ///
    /// When set to a non-zero duration, debouncing is enabled in the [`scheduler`](crate::scheduler())
    /// resulting in __trailing edge debouncing__ of reconciler requests.
    /// This option can help to reduce the amount of unnecessary reconciler calls
    /// when using multiple controller relations, or during rapid phase transitions.
    ///
    /// ## Warning
    /// This option delays (and keeps delaying) reconcile requests for objects while
    /// the object is updated. It can **permanently hide** updates from your reconciler
    /// if set too high on objects that are updated frequently (like nodes).
    #[must_use]
    pub fn debounce(mut self, debounce: Duration) -> Self {
        self.debounce = debounce;
        self
    }

    /// The number of concurrent reconciliations of that are allowed to run at an given moment.
    ///
    /// This can be adjusted to the controller's needs to increase
    /// performance and/or make performance predictable. By default, its 0 meaning
    /// the controller runs with unbounded concurrency.
    ///
    /// Note that despite concurrency, a controller never schedules concurrent reconciles
    /// on the same object.
    #[must_use]
    pub fn concurrency(mut self, concurrency: u16) -> Self {
        self.concurrency = concurrency;
        self
    }
}

/// Controller for a Resource `K`
///
/// A controller is an infinite stream of objects to be reconciled.
///
/// Once `run` and continuously awaited, it continuously calls out to user provided
/// `reconcile` and `error_policy` callbacks whenever relevant changes are detected
/// or if errors are seen from `reconcile`.
///
/// Reconciles are generally requested for all changes on your root objects.
/// Changes to managed child resources will also trigger the reconciler for the
/// managing object by traversing owner references (for `Controller::owns`),
/// or traverse a custom mapping (for `Controller::watches`).
///
/// This mapping mechanism ultimately hides the reason for the reconciliation request,
/// and forces you to write an idempotent reconciler.
///
/// General setup:
/// ```no_run
/// use kube::{Api, Client, CustomResource};
/// use kube::runtime::{controller::{Controller, Action}, watcher};
/// # use serde::{Deserialize, Serialize};
/// # use tokio::time::Duration;
/// use futures::StreamExt;
/// use k8s_openapi::api::core::v1::ConfigMap;
/// use schemars::JsonSchema;
/// # use std::sync::Arc;
/// use thiserror::Error;
///
/// #[derive(Debug, Error)]
/// enum Error {}
///
/// /// A custom resource
/// #[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
/// #[kube(group = "nullable.se", version = "v1", kind = "ConfigMapGenerator", namespaced)]
/// struct ConfigMapGeneratorSpec {
///     content: String,
/// }
///
/// /// The reconciler that will be called when either object change
/// async fn reconcile(g: Arc<ConfigMapGenerator>, _ctx: Arc<()>) -> Result<Action, Error> {
///     // .. use api here to reconcile a child ConfigMap with ownerreferences
///     // see configmapgen_controller example for full info
///     Ok(Action::requeue(Duration::from_secs(300)))
/// }
/// /// an error handler that will be called when the reconciler fails with access to both the
/// /// object that caused the failure and the actual error
/// fn error_policy(obj: Arc<ConfigMapGenerator>, _error: &Error, _ctx: Arc<()>) -> Action {
///     Action::requeue(Duration::from_secs(60))
/// }
///
/// /// something to drive the controller
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let client = Client::try_default().await?;
///     let context = Arc::new(()); // bad empty context - put client in here
///     let cmgs = Api::<ConfigMapGenerator>::all(client.clone());
///     let cms = Api::<ConfigMap>::all(client.clone());
///     Controller::new(cmgs, watcher::Config::default())
///         .owns(cms, watcher::Config::default())
///         .run(reconcile, error_policy, context)
///         .for_each(|res| async move {
///             match res {
///                 Ok(o) => println!("reconciled {:?}", o),
///                 Err(e) => println!("reconcile failed: {:?}", e),
///             }
///         })
///         .await; // controller does nothing unless polled
///     Ok(())
/// }
/// ```
pub struct Controller<K>
where
    K: Clone + Resource + Debug + 'static,
    K::DynamicType: Eq + Hash,
{
    // NB: Need to Unpin for stream::select_all
    trigger_selector: stream::SelectAll<BoxStream<'static, Result<ReconcileRequest<K>, watcher::Error>>>,
    trigger_backoff: Box<dyn Backoff + Send>,
    /// [`run`](crate::Controller::run) starts a graceful shutdown when any of these [`Future`]s complete,
    /// refusing to start any new reconciliations but letting any existing ones finish.
    graceful_shutdown_selector: Vec<BoxFuture<'static, ()>>,
    /// [`run`](crate::Controller::run) terminates immediately when any of these [`Future`]s complete,
    /// requesting that all running reconciliations be aborted.
    /// However, note that they *will* keep running until their next yield point (`.await`),
    /// blocking [`tokio::runtime::Runtime`] destruction (unless you follow up by calling [`std::process::exit`] after `run`).
    forceful_shutdown_selector: Vec<BoxFuture<'static, ()>>,
    dyntype: K::DynamicType,
    reader: Store<K>,
    config: Config,
}

impl<K> Controller<K>
where
    K: Clone + Resource + DeserializeOwned + Debug + Send + Sync + 'static,
    K::DynamicType: Eq + Hash + Clone,
{
    /// Create a Controller for a resource `K`
    ///
    /// Takes an [`Api`] object that determines how the `Controller` listens for changes to the `K`.
    ///
    /// The [`watcher::Config`] controls to the possible subset of objects of `K` that you want to manage
    /// and receive reconcile events for.
    /// For the full set of objects `K` in the given `Api` scope, you can use [`watcher::Config::default`].
    #[must_use]
    pub fn new(main_api: Api<K>, wc: watcher::Config) -> Self
    where
        K::DynamicType: Default,
    {
        Self::new_with(main_api, wc, Default::default())
    }

    /// Create a Controller for a resource `K`
    ///
    /// Takes an [`Api`] object that determines how the `Controller` listens for changes to the `K`.
    ///
    /// The [`watcher::Config`] lets you define a possible subset of objects of `K` that you want the [`Api`]
    /// to watch - in the Api's  configured scope - and receive reconcile events for.
    /// For the full set of objects `K` in the given `Api` scope, you can use [`Config::default`].
    ///
    /// This variant constructor is for [`dynamic`] types found through discovery. Prefer [`Controller::new`] for static types.
    ///
    /// [`watcher::Config`]: crate::watcher::Config
    /// [`Api`]: kube_client::Api
    /// [`dynamic`]: kube_client::core::dynamic
    /// [`Config::default`]: crate::watcher::Config::default
    pub fn new_with(main_api: Api<K>, wc: watcher::Config, dyntype: K::DynamicType) -> Self {
        let writer = Writer::<K>::new(dyntype.clone());
        let reader = writer.as_reader();
        let mut trigger_selector = stream::SelectAll::new();
        let self_watcher = trigger_self(
            reflector(writer, watcher(main_api, wc)).applied_objects(),
            dyntype.clone(),
        )
        .boxed();
        trigger_selector.push(self_watcher);
        Self {
            trigger_selector,
            trigger_backoff: Box::<DefaultBackoff>::default(),
            graceful_shutdown_selector: vec![
                // Fallback future, ensuring that we never terminate if no additional futures are added to the selector
                future::pending().boxed(),
            ],
            forceful_shutdown_selector: vec![
                // Fallback future, ensuring that we never terminate if no additional futures are added to the selector
                future::pending().boxed(),
            ],
            dyntype,
            reader,
            config: Default::default(),
        }
    }

    /// Create a Controller for a resource `K` from a stream of `K` objects
    ///
    /// Same as [`Controller::new`], but instead of an `Api`, a stream of resources is used.
    /// This allows for customized and pre-filtered watch streams to be used as a trigger,
    /// as well as sharing input streams between multiple controllers.
    ///
    /// **NB**: This is constructor requires an [`unstable`](https://github.com/kube-rs/kube/blob/main/kube-runtime/Cargo.toml#L17-L21) feature.
    ///
    /// # Example:
    ///
    /// ```no_run
    /// # use futures::StreamExt;
    /// # use k8s_openapi::api::apps::v1::Deployment;
    /// # use kube::runtime::controller::{Action, Controller};
    /// # use kube::runtime::{predicates, watcher, reflector, WatchStreamExt};
    /// # use kube::{Api, Client, Error, ResourceExt};
    /// # use std::sync::Arc;
    /// # async fn reconcile(_: Arc<Deployment>, _: Arc<()>) -> Result<Action, Error> { Ok(Action::await_change()) }
    /// # fn error_policy(_: Arc<Deployment>, _: &kube::Error, _: Arc<()>) -> Action { Action::await_change() }
    /// # async fn doc(client: kube::Client) {
    /// let api: Api<Deployment> = Api::default_namespaced(client);
    /// let (reader, writer) = reflector::store();
    /// let deploys = watcher(api, watcher::Config::default())
    ///     .default_backoff()
    ///     .reflect(writer)
    ///     .applied_objects()
    ///     .predicate_filter(predicates::generation);
    ///
    /// Controller::for_stream(deploys, reader)
    ///     .run(reconcile, error_policy, Arc::new(()))
    ///     .for_each(|_| std::future::ready(()))
    ///     .await;
    /// # }
    /// ```
    ///
    /// Prefer [`Controller::new`] if you do not need to share the stream, or do not need pre-filtering.
    #[cfg(feature = "unstable-runtime-stream-control")]
    pub fn for_stream(
        trigger: impl Stream<Item = Result<K, watcher::Error>> + Send + 'static,
        reader: Store<K>,
    ) -> Self
    where
        K::DynamicType: Default,
    {
        Self::for_stream_with(trigger, reader, Default::default())
    }

    /// Create a Controller for a resource `K` from a stream of `K` objects
    ///
    /// Same as [`Controller::new`], but instead of an `Api`, a stream of resources is used.
    /// This allows for customized and pre-filtered watch streams to be used as a trigger,
    /// as well as sharing input streams between multiple controllers.
    ///
    /// **NB**: This is constructor requires an [`unstable`](https://github.com/kube-rs/kube/blob/main/kube-runtime/Cargo.toml#L17-L21) feature.
    ///
    /// Prefer [`Controller::new`] if you do not need to share the stream, or do not need pre-filtering.
    ///
    /// This variant constructor is for [`dynamic`] types found through discovery. Prefer [`Controller::for_stream`] for static types.
    ///
    /// [`dynamic`]: kube_client::core::dynamic
    #[cfg(feature = "unstable-runtime-stream-control")]
    pub fn for_stream_with(
        trigger: impl Stream<Item = Result<K, watcher::Error>> + Send + 'static,
        reader: Store<K>,
        dyntype: K::DynamicType,
    ) -> Self {
        let mut trigger_selector = stream::SelectAll::new();
        let self_watcher = trigger_self(trigger, dyntype.clone()).boxed();
        trigger_selector.push(self_watcher);
        Self {
            trigger_selector,
            trigger_backoff: Box::<DefaultBackoff>::default(),
            graceful_shutdown_selector: vec![
                // Fallback future, ensuring that we never terminate if no additional futures are added to the selector
                future::pending().boxed(),
            ],
            forceful_shutdown_selector: vec![
                // Fallback future, ensuring that we never terminate if no additional futures are added to the selector
                future::pending().boxed(),
            ],
            dyntype,
            reader,
            config: Default::default(),
        }
    }

    /// This is the same as [`Controller::for_stream`]. Instead of taking an
    /// `Api` (e.g. [`Controller::new`]), a stream of resources is used. Shared
    /// streams can be created out-of-band by subscribing on a store `Writer`.
    /// Through this interface, multiple controllers can use the same root
    /// (shared) input stream of resources to keep memory overheads smaller.
    ///
    /// **N.B**: This constructor requires an
    /// [`unstable`](https://github.com/kube-rs/kube/blob/main/kube-runtime/Cargo.toml#L17-L21)
    /// feature.
    ///
    /// Prefer [`Controller::new`] or [`Controller::for_stream`] if you do not
    /// need to share the stream.
    ///
    /// ## Warning:
    ///
    /// You **must** ensure the root stream (i.e. stream created through a `reflector()`)
    /// is driven to readiness independently of this controller to ensure the
    /// watcher never deadlocks.
    ///
    /// # Example:
    ///
    /// ```no_run
    /// # use futures::StreamExt;
    /// # use k8s_openapi::api::apps::v1::Deployment;
    /// # use kube::runtime::controller::{Action, Controller};
    /// # use kube::runtime::{predicates, watcher, reflector, WatchStreamExt};
    /// # use kube::{Api, Client, Error, ResourceExt};
    /// # use std::sync::Arc;
    /// # async fn reconcile(_: Arc<Deployment>, _: Arc<()>) -> Result<Action, Error> { Ok(Action::await_change()) }
    /// # fn error_policy(_: Arc<Deployment>, _: &kube::Error, _: Arc<()>) -> Action { Action::await_change() }
    /// # async fn doc(client: kube::Client) {
    /// let api: Api<Deployment> = Api::default_namespaced(client);
    /// let (reader, writer) = reflector::store_shared(128);
    /// let subscriber = writer
    ///     .subscribe()
    ///     .expect("subscribers can only be created from shared stores");
    /// let deploys = watcher(api, watcher::Config::default())
    ///     .default_backoff()
    ///     .reflect(writer)
    ///     .applied_objects()
    ///     .for_each(|ev| async move {
    ///         match ev {
    ///             Ok(obj) => tracing::info!("got obj {obj:?}"),
    ///             Err(error) => tracing::error!(%error, "received error")
    ///         }
    ///     });
    ///
    /// let controller = Controller::for_shared_stream(subscriber, reader)
    ///     .run(reconcile, error_policy, Arc::new(()))
    ///     .for_each(|ev| async move {
    ///         tracing::info!("reconciled {ev:?}")
    ///     });
    ///
    /// // Drive streams using a select statement
    /// tokio::select! {
    ///   _ = deploys => {},
    ///   _ = controller => {},
    /// }
    /// # }
    #[cfg(feature = "unstable-runtime-subscribe")]
    pub fn for_shared_stream(trigger: impl Stream<Item = Arc<K>> + Send + 'static, reader: Store<K>) -> Self
    where
        K::DynamicType: Default,
    {
        Self::for_shared_stream_with(trigger, reader, Default::default())
    }

    /// This is the same as [`Controller::for_stream`]. Instead of taking an
    /// `Api` (e.g. [`Controller::new`]), a stream of resources is used. Shared
    /// streams can be created out-of-band by subscribing on a store `Writer`.
    /// Through this interface, multiple controllers can use the same root
    /// (shared) input stream of resources to keep memory overheads smaller.
    ///
    /// **N.B**: This constructor requires an
    /// [`unstable`](https://github.com/kube-rs/kube/blob/main/kube-runtime/Cargo.toml#L17-L21)
    /// feature.
    ///
    /// Prefer [`Controller::new`] or [`Controller::for_stream`] if you do not
    /// need to share the stream.
    ///
    /// This variant constructor is used for [`dynamic`] types found through
    /// discovery. Prefer [`Controller::for_shared_stream`] for static types (i.e.
    /// known at compile time).
    ///
    /// [`dynamic`]: kube_client::core::dynamic
    #[cfg(feature = "unstable-runtime-subscribe")]
    pub fn for_shared_stream_with(
        trigger: impl Stream<Item = Arc<K>> + Send + 'static,
        reader: Store<K>,
        dyntype: K::DynamicType,
    ) -> Self {
        let mut trigger_selector = stream::SelectAll::new();
        let self_watcher = trigger_self_shared(trigger.map(Ok), dyntype.clone()).boxed();
        trigger_selector.push(self_watcher);
        Self {
            trigger_selector,
            trigger_backoff: Box::<DefaultBackoff>::default(),
            graceful_shutdown_selector: vec![
                // Fallback future, ensuring that we never terminate if no additional futures are added to the selector
                future::pending().boxed(),
            ],
            forceful_shutdown_selector: vec![
                // Fallback future, ensuring that we never terminate if no additional futures are added to the selector
                future::pending().boxed(),
            ],
            dyntype,
            reader,
            config: Default::default(),
        }
    }

    /// Specify the configuration for the controller's behavior.
    #[must_use]
    pub fn with_config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Specify the backoff policy for "trigger" watches
    ///
    /// This includes the core watch, as well as auxilary watches introduced by [`Self::owns`] and [`Self::watches`].
    ///
    /// The [`default_backoff`](crate::watcher::default_backoff) follows client-go conventions,
    /// but can be overridden by calling this method.
    #[must_use]
    pub fn trigger_backoff(mut self, backoff: impl Backoff + 'static) -> Self {
        self.trigger_backoff = Box::new(backoff);
        self
    }

    /// Retrieve a copy of the reader before starting the controller
    pub fn store(&self) -> Store<K> {
        self.reader.clone()
    }

    /// Specify `Child` objects which `K` owns and should be watched
    ///
    /// Takes an [`Api`] object that determines how the `Controller` listens for changes to the `Child`.
    /// All owned `Child` objects **must** contain an [`OwnerReference`] pointing back to a `K`.
    ///
    /// The [`watcher::Config`] controls the subset of `Child` objects that you want the [`Api`]
    /// to watch - in the Api's configured scope - and receive reconcile events for.
    /// To watch the full set of `Child` objects in the given `Api` scope, you can use [`watcher::Config::default`].
    ///
    /// [`OwnerReference`]: k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference
    #[must_use]
    pub fn owns<Child: Clone + Resource<DynamicType = ()> + DeserializeOwned + Debug + Send + 'static>(
        self,
        api: Api<Child>,
        wc: watcher::Config,
    ) -> Self {
        self.owns_with(api, (), wc)
    }

    /// Specify `Child` objects which `K` owns and should be watched
    ///
    /// Same as [`Controller::owns`], but accepts a `DynamicType` so it can be used with dynamic resources.
    #[must_use]
    pub fn owns_with<Child: Clone + Resource + DeserializeOwned + Debug + Send + 'static>(
        mut self,
        api: Api<Child>,
        dyntype: Child::DynamicType,
        wc: watcher::Config,
    ) -> Self
    where
        Child::DynamicType: Debug + Eq + Hash + Clone,
    {
        // TODO: call owns_stream_with when it's stable
        let child_watcher = trigger_owners(
            metadata_watcher(api, wc).touched_objects(),
            self.dyntype.clone(),
            dyntype,
        );
        self.trigger_selector.push(child_watcher.boxed());
        self
    }

    /// Trigger the reconciliation process for a stream of `Child` objects of the owner `K`
    ///
    /// Same as [`Controller::owns`], but instead of an `Api`, a stream of resources is used.
    /// This allows for customized and pre-filtered watch streams to be used as a trigger,
    /// as well as sharing input streams between multiple controllers.
    ///
    /// **NB**: This is constructor requires an [`unstable`](https://github.com/kube-rs/kube/blob/main/kube-runtime/Cargo.toml#L17-L21) feature.
    ///
    /// Watcher streams passed in here should be filtered first through `touched_objects`.
    ///
    /// # Example:
    ///
    /// ```no_run
    /// # use futures::StreamExt;
    /// # use k8s_openapi::api::core::v1::ConfigMap;
    /// # use k8s_openapi::api::apps::v1::StatefulSet;
    /// # use kube::runtime::controller::Action;
    /// # use kube::runtime::{predicates, metadata_watcher, watcher, Controller, WatchStreamExt};
    /// # use kube::{Api, Client, Error, ResourceExt};
    /// # use std::sync::Arc;
    /// # type CustomResource = ConfigMap;
    /// # async fn reconcile(_: Arc<CustomResource>, _: Arc<()>) -> Result<Action, Error> { Ok(Action::await_change()) }
    /// # fn error_policy(_: Arc<CustomResource>, _: &kube::Error, _: Arc<()>) -> Action { Action::await_change() }
    /// # async fn doc(client: kube::Client) {
    /// let sts_stream = metadata_watcher(Api::<StatefulSet>::all(client.clone()), watcher::Config::default())
    ///     .touched_objects()
    ///     .predicate_filter(predicates::generation);
    ///
    /// Controller::new(Api::<CustomResource>::all(client), watcher::Config::default())
    ///     .owns_stream(sts_stream)
    ///     .run(reconcile, error_policy, Arc::new(()))
    ///     .for_each(|_| std::future::ready(()))
    ///     .await;
    /// # }
    /// ```
    #[cfg(feature = "unstable-runtime-stream-control")]
    #[must_use]
    pub fn owns_stream<Child: Resource<DynamicType = ()> + Send + 'static>(
        self,
        trigger: impl Stream<Item = Result<Child, watcher::Error>> + Send + 'static,
    ) -> Self {
        self.owns_stream_with(trigger, ())
    }

    /// Trigger the reconciliation process for a stream of `Child` objects of the owner `K`
    ///
    /// Same as [`Controller::owns`], but instead of an `Api`, a stream of resources is used.
    /// This allows for customized and pre-filtered watch streams to be used as a trigger,
    /// as well as sharing input streams between multiple controllers.
    ///
    /// **NB**: This is constructor requires an [`unstable`](https://github.com/kube-rs/kube/blob/main/kube-runtime/Cargo.toml#L17-L21) feature.
    ///
    /// Same as [`Controller::owns_stream`], but accepts a `DynamicType` so it can be used with dynamic resources.
    #[cfg(feature = "unstable-runtime-stream-control")]
    #[must_use]
    pub fn owns_stream_with<Child: Resource + Send + 'static>(
        mut self,
        trigger: impl Stream<Item = Result<Child, watcher::Error>> + Send + 'static,
        dyntype: Child::DynamicType,
    ) -> Self
    where
        Child::DynamicType: Debug + Eq + Hash + Clone,
    {
        let child_watcher = trigger_owners(trigger, self.dyntype.clone(), dyntype);
        self.trigger_selector.push(child_watcher.boxed());
        self
    }

    /// This is the same as [`Controller::for_stream`]. Instead of taking an
    /// `Api` (e.g. [`Controller::new`]), a stream of resources is used. Shared
    /// streams can be created out-of-band by subscribing on a store `Writer`.
    /// Through this interface, multiple controllers can use the same root
    /// (shared) input stream of resources to keep memory overheads smaller.
    ///
    /// **N.B**: This constructor requires an
    /// [`unstable`](https://github.com/kube-rs/kube/blob/main/kube-runtime/Cargo.toml#L17-L21)
    /// feature.
    ///
    /// Prefer [`Controller::new`] or [`Controller::for_stream`] if you do not
    /// need to share the stream.
    ///
    /// ## Warning:
    ///
    /// You **must** ensure the root stream (i.e. stream created through a `reflector()`)
    /// is driven to readiness independently of this controller to ensure the
    /// watcher never deadlocks.
    ///
    ///
    /// Trigger the reconciliation process for a shared stream of `Child`
    /// objects of the owner `K`
    ///
    /// Conceptually the same as [`Controller::owns`], but a stream is used
    /// instead of an `Api`. This interface behaves similarly to its non-shared
    /// counterpart [`Controller::owns_stream`].
    ///
    /// **NB**: This is constructor requires an [`unstable`](https://github.com/kube-rs/kube/blob/main/kube-runtime/Cargo.toml#L17-L21) feature.
    ///
    /// # Example:
    ///
    /// ```no_run
    /// # use futures::StreamExt;
    /// # use k8s_openapi::api::{apps::v1::Deployment, core::v1::Pod};
    /// # use kube::runtime::controller::{Action, Controller};
    /// # use kube::runtime::{predicates, watcher, reflector, WatchStreamExt};
    /// # use kube::{Api, Client, Error, ResourceExt};
    /// # use std::sync::Arc;
    /// # async fn reconcile(_: Arc<Deployment>, _: Arc<()>) -> Result<Action, Error> { Ok(Action::await_change()) }
    /// # fn error_policy(_: Arc<Deployment>, _: &kube::Error, _: Arc<()>) -> Action { Action::await_change() }
    /// # async fn doc(client: kube::Client) {
    /// let deploys: Api<Deployment> = Api::default_namespaced(client.clone());
    /// let pod_api: Api<Pod> = Api::default_namespaced(client);
    ///
    /// let (reader, writer) = reflector::store_shared(128);
    /// let subscriber = writer
    ///     .subscribe()
    ///     .expect("subscribers can only be created from shared stores");
    /// let pods = watcher(pod_api, watcher::Config::default())
    ///     .default_backoff()
    ///     .reflect(writer)
    ///     .applied_objects()
    ///     .for_each(|ev| async move {
    ///         match ev {
    ///             Ok(obj) => tracing::info!("got obj {obj:?}"),
    ///             Err(error) => tracing::error!(%error, "received error")
    ///         }
    ///     });
    ///
    /// let controller = Controller::new(deploys, Default::default())
    ///     .owns_shared_stream(subscriber)
    ///     .run(reconcile, error_policy, Arc::new(()))
    ///     .for_each(|ev| async move {
    ///         tracing::info!("reconciled {ev:?}")
    ///     });
    ///
    /// // Drive streams using a select statement
    /// tokio::select! {
    ///   _ = pods => {},
    ///   _ = controller => {},
    /// }
    /// # }
    #[cfg(feature = "unstable-runtime-subscribe")]
    #[must_use]
    pub fn owns_shared_stream<Child: Resource<DynamicType = ()> + Send + 'static>(
        self,
        trigger: impl Stream<Item = Arc<Child>> + Send + 'static,
    ) -> Self {
        self.owns_shared_stream_with(trigger, ())
    }

    /// Trigger the reconciliation process for a shared stream of `Child` objects of the owner `K`
    ///
    /// Same as [`Controller::owns`], but instead of an `Api`, a shared stream of resources is used.
    /// The source stream can be shared between multiple controllers, optimising
    /// resource usage.
    ///
    /// **NB**: This is constructor requires an [`unstable`](https://github.com/kube-rs/kube/blob/main/kube-runtime/Cargo.toml#L17-L21) feature.
    ///
    /// Same as [`Controller::owns_shared_stream`], but accepts a `DynamicType` so it can be used with dynamic resources.
    #[cfg(feature = "unstable-runtime-subscribe")]
    #[must_use]
    pub fn owns_shared_stream_with<Child: Resource<DynamicType = ()> + Send + 'static>(
        mut self,
        trigger: impl Stream<Item = Arc<Child>> + Send + 'static,
        dyntype: Child::DynamicType,
    ) -> Self
    where
        Child::DynamicType: Debug + Eq + Hash + Clone,
    {
        let child_watcher = trigger_owners_shared(trigger.map(Ok), self.dyntype.clone(), dyntype);
        self.trigger_selector.push(child_watcher.boxed());
        self
    }

    /// Specify `Watched` object which `K` has a custom relation to and should be watched
    ///
    /// To define the `Watched` relation with `K`, you **must** define a custom relation mapper, which,
    /// when given a `Watched` object, returns an option or iterator of relevant `ObjectRef<K>` to reconcile.
    ///
    /// If the relation `K` has to `Watched` is that `K` owns `Watched`, consider using [`Controller::owns`].
    ///
    /// Takes an [`Api`] object that determines how the `Controller` listens for changes to the `Watched`.
    ///
    /// The [`watcher::Config`] controls the subset of `Watched` objects that you want the [`Api`]
    /// to watch - in the Api's configured scope - and run through the custom mapper.
    /// To watch the full set of `Watched` objects in given the `Api` scope, you can use [`watcher::Config::default`].
    ///
    /// # Example
    ///
    /// Tracking cross cluster references using the [Operator-SDK] annotations.
    ///
    /// ```
    /// # use kube::runtime::{Controller, controller::Action, reflector::ObjectRef, watcher};
    /// # use kube::{Api, ResourceExt};
    /// # use k8s_openapi::api::core::v1::{ConfigMap, Namespace};
    /// # use futures::StreamExt;
    /// # use std::sync::Arc;
    /// # type WatchedResource = Namespace;
    /// # struct Context;
    /// # async fn reconcile(_: Arc<ConfigMap>, _: Arc<Context>) -> Result<Action, kube::Error> {
    /// #     Ok(Action::await_change())
    /// # };
    /// # fn error_policy(_: Arc<ConfigMap>, _: &kube::Error, _: Arc<Context>) -> Action {
    /// #     Action::await_change()
    /// # }
    /// # async fn doc(client: kube::Client) -> Result<(), Box<dyn std::error::Error>> {
    /// # let memcached = Api::<ConfigMap>::all(client.clone());
    /// # let context = Arc::new(Context);
    /// Controller::new(memcached, watcher::Config::default())
    ///     .watches(
    ///         Api::<WatchedResource>::all(client.clone()),
    ///         watcher::Config::default(),
    ///         |ar| {
    ///             let prt = ar
    ///                 .annotations()
    ///                 .get("operator-sdk/primary-resource-type")
    ///                 .map(String::as_str);
    ///
    ///             if prt != Some("Memcached.cache.example.com") {
    ///                 return None;
    ///             }
    ///
    ///             let (namespace, name) = ar
    ///                 .annotations()
    ///                 .get("operator-sdk/primary-resource")?
    ///                 .split_once('/')?;
    ///
    ///             Some(ObjectRef::new(name).within(namespace))
    ///         }
    ///     )
    ///     .run(reconcile, error_policy, context)
    ///     .for_each(|_| futures::future::ready(()))
    ///     .await;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [Operator-SDK]: https://sdk.operatorframework.io/docs/building-operators/ansible/reference/retroactively-owned-resources/
    #[must_use]
    pub fn watches<Other, I>(
        self,
        api: Api<Other>,
        wc: watcher::Config,
        mapper: impl Fn(Other) -> I + Sync + Send + 'static,
    ) -> Self
    where
        Other: Clone + Resource + DeserializeOwned + Debug + Send + 'static,
        Other::DynamicType: Default + Debug + Clone + Eq + Hash,
        I: 'static + IntoIterator<Item = ObjectRef<K>>,
        I::IntoIter: Send,
    {
        self.watches_with(api, Default::default(), wc, mapper)
    }

    /// Specify `Watched` object which `K` has a custom relation to and should be watched
    ///
    /// Same as [`Controller::watches`], but accepts a `DynamicType` so it can be used with dynamic resources.
    #[must_use]
    pub fn watches_with<Other, I>(
        mut self,
        api: Api<Other>,
        dyntype: Other::DynamicType,
        wc: watcher::Config,
        mapper: impl Fn(Other) -> I + Sync + Send + 'static,
    ) -> Self
    where
        Other: Clone + Resource + DeserializeOwned + Debug + Send + 'static,
        I: 'static + IntoIterator<Item = ObjectRef<K>>,
        I::IntoIter: Send,
        Other::DynamicType: Debug + Clone + Eq + Hash,
    {
        let other_watcher = trigger_others(watcher(api, wc).touched_objects(), mapper, dyntype);
        self.trigger_selector.push(other_watcher.boxed());
        self
    }

    /// Trigger the reconciliation process for a stream of `Other` objects related to a `K`
    ///
    /// Same as [`Controller::watches`], but instead of an `Api`, a stream of resources is used.
    /// This allows for customized and pre-filtered watch streams to be used as a trigger,
    /// as well as sharing input streams between multiple controllers.
    ///
    /// **NB**: This is constructor requires an [`unstable`](https://github.com/kube-rs/kube/blob/main/kube-runtime/Cargo.toml#L17-L21) feature.
    ///
    /// Watcher streams passed in here should be filtered first through `touched_objects`.
    ///
    /// # Example:
    ///
    /// ```no_run
    /// # use futures::StreamExt;
    /// # use k8s_openapi::api::core::v1::ConfigMap;
    /// # use k8s_openapi::api::apps::v1::DaemonSet;
    /// # use kube::runtime::controller::Action;
    /// # use kube::runtime::{predicates, reflector::ObjectRef, watcher, Controller, WatchStreamExt};
    /// # use kube::{Api, Client, Error, ResourceExt};
    /// # use std::sync::Arc;
    /// # type CustomResource = ConfigMap;
    /// # async fn reconcile(_: Arc<CustomResource>, _: Arc<()>) -> Result<Action, Error> { Ok(Action::await_change()) }
    /// # fn error_policy(_: Arc<CustomResource>, _: &kube::Error, _: Arc<()>) -> Action { Action::await_change() }
    /// fn mapper(_: DaemonSet) -> Option<ObjectRef<CustomResource>> { todo!() }
    /// # async fn doc(client: kube::Client) {
    /// let api: Api<DaemonSet> = Api::all(client.clone());
    /// let cr: Api<CustomResource> = Api::all(client.clone());
    /// let daemons = watcher(api, watcher::Config::default())
    ///     .touched_objects()
    ///     .predicate_filter(predicates::generation);
    ///
    /// Controller::new(cr, watcher::Config::default())
    ///     .watches_stream(daemons, mapper)
    ///     .run(reconcile, error_policy, Arc::new(()))
    ///     .for_each(|_| std::future::ready(()))
    ///     .await;
    /// # }
    /// ```
    #[cfg(feature = "unstable-runtime-stream-control")]
    #[must_use]
    pub fn watches_stream<Other, I>(
        self,
        trigger: impl Stream<Item = Result<Other, watcher::Error>> + Send + 'static,
        mapper: impl Fn(Other) -> I + Sync + Send + 'static,
    ) -> Self
    where
        Other: Clone + Resource + DeserializeOwned + Debug + Send + 'static,
        Other::DynamicType: Default + Debug + Clone,
        I: 'static + IntoIterator<Item = ObjectRef<K>>,
        I::IntoIter: Send,
    {
        self.watches_stream_with(trigger, mapper, Default::default())
    }

    /// Trigger the reconciliation process for a stream of `Other` objects related to a `K`
    ///
    /// Same as [`Controller::watches`], but instead of an `Api`, a stream of resources is used.
    /// This allows for customized and pre-filtered watch streams to be used as a trigger,
    /// as well as sharing input streams between multiple controllers.
    ///
    /// **NB**: This is constructor requires an [`unstable`](https://github.com/kube-rs/kube/blob/main/kube-runtime/Cargo.toml#L17-L21) feature.
    ///
    /// Same as [`Controller::watches_stream`], but accepts a `DynamicType` so it can be used with dynamic resources.
    #[cfg(feature = "unstable-runtime-stream-control")]
    #[must_use]
    pub fn watches_stream_with<Other, I>(
        mut self,
        trigger: impl Stream<Item = Result<Other, watcher::Error>> + Send + 'static,
        mapper: impl Fn(Other) -> I + Sync + Send + 'static,
        dyntype: Other::DynamicType,
    ) -> Self
    where
        Other: Clone + Resource + DeserializeOwned + Debug + Send + 'static,
        Other::DynamicType: Debug + Clone,
        I: 'static + IntoIterator<Item = ObjectRef<K>>,
        I::IntoIter: Send,
    {
        let other_watcher = trigger_others(trigger, mapper, dyntype);
        self.trigger_selector.push(other_watcher.boxed());
        self
    }

    /// Trigger the reconciliation process for a shared stream of `Other`
    /// objects related to a `K`
    ///
    /// Same as [`Controller::watches`], but instead of an `Api`, a shared
    /// stream of resources is used. This allows for sharing input streams
    /// between multiple controllers.
    ///
    /// **NB**: This is constructor requires an [`unstable`](https://github.com/kube-rs/kube/blob/main/kube-runtime/Cargo.toml#L17-L21) feature.
    ///
    /// Watcher streams passed in here should be filtered first through `touched_objects`.
    ///
    /// # Example:
    ///
    /// ```no_run
    /// # use futures::StreamExt;
    /// # use k8s_openapi::api::core::v1::ConfigMap;
    /// # use k8s_openapi::api::apps::v1::DaemonSet;
    /// # use kube::runtime::controller::Action;
    /// # use kube::runtime::{predicates, reflector::ObjectRef, watcher, Controller, WatchStreamExt};
    /// # use kube::{Api, Client, Error, ResourceExt};
    /// # use std::sync::Arc;
    /// # type CustomResource = ConfigMap;
    /// # async fn reconcile(_: Arc<CustomResource>, _: Arc<()>) -> Result<Action, Error> { Ok(Action::await_change()) }
    /// # fn error_policy(_: Arc<CustomResource>, _: &kube::Error, _: Arc<()>) -> Action { Action::await_change() }
    /// fn mapper(_: Arc<DaemonSet>) -> Option<ObjectRef<CustomResource>> { todo!() }
    /// # async fn doc(client: kube::Client) {
    /// let api: Api<DaemonSet> = Api::all(client.clone());
    /// let cr: Api<CustomResource> = Api::all(client.clone());
    /// let (reader, writer) = kube_runtime::reflector::store_shared(128);
    /// let subscriber = writer
    ///     .subscribe()
    ///     .expect("subscribers can only be created from shared stores");
    /// let daemons = watcher(api, watcher::Config::default())
    ///     .reflect(writer)
    ///     .touched_objects()
    ///     .for_each(|ev| async move {
    ///         match ev {
    ///             Ok(obj) => {},
    ///             Err(error) => tracing::error!(%error, "received err")
    ///         }
    ///     });
    ///
    /// let controller = Controller::new(cr, watcher::Config::default())
    ///     .watches_shared_stream(subscriber, mapper)
    ///     .run(reconcile, error_policy, Arc::new(()))
    ///     .for_each(|_| std::future::ready(()));
    ///
    /// // Drive streams using a select statement
    /// tokio::select! {
    ///   _ = daemons => {},
    ///   _ = controller => {},
    /// }
    /// # }
    /// ```
    #[cfg(feature = "unstable-runtime-subscribe")]
    #[must_use]
    pub fn watches_shared_stream<Other, I>(
        self,
        trigger: impl Stream<Item = Arc<Other>> + Send + 'static,
        mapper: impl Fn(Arc<Other>) -> I + Sync + Send + 'static,
    ) -> Self
    where
        Other: Clone + Resource + DeserializeOwned + Debug + Send + 'static,
        Other::DynamicType: Default + Debug + Clone,
        I: 'static + IntoIterator<Item = ObjectRef<K>>,
        I::IntoIter: Send,
    {
        self.watches_shared_stream_with(trigger, mapper, Default::default())
    }

    /// Trigger the reconciliation process for a shared stream of `Other` objects related to a `K`
    ///
    /// Same as [`Controller::watches`], but instead of an `Api`, a shared
    /// stream of resources is used. This allows for sharing of streams between
    /// multiple controllers.
    ///
    /// **NB**: This is constructor requires an [`unstable`](https://github.com/kube-rs/kube/blob/main/kube-runtime/Cargo.toml#L17-L21) feature.
    ///
    /// Same as [`Controller::watches_shared_stream`], but accepts a `DynamicType` so it can be used with dynamic resources.
    #[cfg(feature = "unstable-runtime-subscribe")]
    #[must_use]
    pub fn watches_shared_stream_with<Other, I>(
        mut self,
        trigger: impl Stream<Item = Arc<Other>> + Send + 'static,
        mapper: impl Fn(Arc<Other>) -> I + Sync + Send + 'static,
        dyntype: Other::DynamicType,
    ) -> Self
    where
        Other: Clone + Resource + DeserializeOwned + Debug + Send + 'static,
        Other::DynamicType: Debug + Clone,
        I: 'static + IntoIterator<Item = ObjectRef<K>>,
        I::IntoIter: Send,
    {
        let other_watcher = trigger_others_shared(trigger.map(Ok), mapper, dyntype);
        self.trigger_selector.push(other_watcher.boxed());
        self
    }

    /// Trigger a reconciliation for all managed objects whenever `trigger` emits a value
    ///
    /// For example, this can be used to reconcile all objects whenever the controller's configuration changes.
    ///
    /// To reconcile all objects when a new line is entered:
    ///
    /// ```
    /// # async {
    /// use futures::stream::StreamExt;
    /// use k8s_openapi::api::core::v1::ConfigMap;
    /// use kube::{
    ///     Client,
    ///     api::{Api, ResourceExt},
    ///     runtime::{
    ///         controller::{Controller, Action},
    ///         watcher,
    ///     },
    /// };
    /// use std::{convert::Infallible, io::BufRead, sync::Arc};
    /// let (mut reload_tx, reload_rx) = futures::channel::mpsc::channel(0);
    /// // Using a regular background thread since tokio::io::stdin() doesn't allow aborting reads,
    /// // and its worker prevents the Tokio runtime from shutting down.
    /// std::thread::spawn(move || {
    ///     for _ in std::io::BufReader::new(std::io::stdin()).lines() {
    ///         let _ = reload_tx.try_send(());
    ///     }
    /// });
    /// Controller::new(
    ///     Api::<ConfigMap>::all(Client::try_default().await.unwrap()),
    ///     watcher::Config::default(),
    /// )
    /// .reconcile_all_on(reload_rx.map(|_| ()))
    /// .run(
    ///     |o, _| async move {
    ///         println!("Reconciling {}", o.name_any());
    ///         Ok(Action::await_change())
    ///     },
    ///     |_object: Arc<ConfigMap>, err: &Infallible, _| Err(err).unwrap(),
    ///     Arc::new(()),
    /// );
    /// # };
    /// ```
    ///
    /// This can be called multiple times, in which case they are additive; reconciles are scheduled whenever *any* [`Stream`] emits a new item.
    ///
    /// If a [`Stream`] is terminated (by emitting [`None`]) then the [`Controller`] keeps running, but the [`Stream`] stops being polled.
    #[must_use]
    pub fn reconcile_all_on(mut self, trigger: impl Stream<Item = ()> + Send + Sync + 'static) -> Self {
        let store = self.store();
        let dyntype = self.dyntype.clone();
        self.trigger_selector.push(
            trigger
                .flat_map(move |()| {
                    let dyntype = dyntype.clone();
                    stream::iter(store.state().into_iter().map(move |obj| {
                        Ok(ReconcileRequest {
                            obj_ref: ObjectRef::from_obj_with(&*obj, dyntype.clone()),
                            reason: ReconcileReason::BulkReconcile,
                        })
                    }))
                })
                .boxed(),
        );
        self
    }

    /// Trigger the reconciliation process for a managed object `ObjectRef<K>` whenever `trigger` emits a value
    ///
    /// This can be used to inject reconciliations for specific objects from an external resource.
    ///
    /// # Example:
    ///
    /// ```no_run
    /// # async {
    /// # use futures::{StreamExt, Stream, stream, TryStreamExt};
    /// # use k8s_openapi::api::core::v1::{ConfigMap};
    /// # use kube::api::Api;
    /// # use kube::runtime::controller::Action;
    /// # use kube::runtime::reflector::{ObjectRef, Store};
    /// # use kube::runtime::{reflector, watcher, Controller, WatchStreamExt};
    /// # use kube::runtime::watcher::Config;
    /// # use kube::{Client, Error, ResourceExt};
    /// # use std::future;
    /// # use std::sync::Arc;
    /// #
    /// # let client: Client = todo!();
    /// # async fn reconcile(_: Arc<ConfigMap>, _: Arc<()>) -> Result<Action, Error> { Ok(Action::await_change()) }
    /// # fn error_policy(_: Arc<ConfigMap>, _: &kube::Error, _: Arc<()>) -> Action { Action::await_change() }
    /// # fn watch_external_objects() -> impl Stream<Item = ExternalObject> { stream::iter(vec![]) }
    /// # let ns = "controller-ns".to_string();
    /// struct ExternalObject {
    ///     name: String,
    /// }
    /// let external_stream = watch_external_objects().map(|ext| {
    ///     ObjectRef::new(&format!("{}-cm", ext.name)).within(&ns)
    /// });
    ///
    /// Controller::new(Api::<ConfigMap>::namespaced(client, &ns), Config::default())
    ///     .reconcile_on(external_stream)
    ///     .run(reconcile, error_policy, Arc::new(()))
    ///     .for_each(|_| future::ready(()))
    ///     .await;
    /// # };
    /// ```
    #[cfg(feature = "unstable-runtime-reconcile-on")]
    #[must_use]
    pub fn reconcile_on(mut self, trigger: impl Stream<Item = ObjectRef<K>> + Send + 'static) -> Self {
        self.trigger_selector.push(
            trigger
                .map(move |obj| {
                    Ok(ReconcileRequest {
                        obj_ref: obj,
                        reason: ReconcileReason::Unknown,
                    })
                })
                .boxed(),
        );
        self
    }

    /// Start a graceful shutdown when `trigger` resolves. Once a graceful shutdown has been initiated:
    ///
    /// - No new reconciliations are started from the scheduler
    /// - The underlying Kubernetes watch is terminated
    /// - All running reconciliations are allowed to finish
    /// - [`Controller::run`]'s [`Stream`] terminates once all running reconciliations are done.
    ///
    /// For example, to stop the reconciler whenever the user presses Ctrl+C:
    ///
    /// ```rust
    /// # async {
    /// use futures::future::FutureExt;
    /// use k8s_openapi::api::core::v1::ConfigMap;
    /// use kube::{Api, Client, ResourceExt};
    /// use kube_runtime::{
    ///     controller::{Controller, Action},
    ///     watcher,
    /// };
    /// use std::{convert::Infallible, sync::Arc};
    /// Controller::new(
    ///     Api::<ConfigMap>::all(Client::try_default().await.unwrap()),
    ///     watcher::Config::default(),
    /// )
    /// .graceful_shutdown_on(tokio::signal::ctrl_c().map(|_| ()))
    /// .run(
    ///     |o, _| async move {
    ///         println!("Reconciling {}", o.name_any());
    ///         Ok(Action::await_change())
    ///     },
    ///     |_, err: &Infallible, _| Err(err).unwrap(),
    ///     Arc::new(()),
    /// );
    /// # };
    /// ```
    ///
    /// This can be called multiple times, in which case they are additive; the [`Controller`] starts to terminate
    /// as soon as *any* [`Future`] resolves.
    #[must_use]
    pub fn graceful_shutdown_on(mut self, trigger: impl Future<Output = ()> + Send + Sync + 'static) -> Self {
        self.graceful_shutdown_selector.push(trigger.boxed());
        self
    }

    /// Initiate graceful shutdown on Ctrl+C or SIGTERM (on Unix), waiting for all reconcilers to finish.
    ///
    /// Once a graceful shutdown has been initiated, Ctrl+C (or SIGTERM) can be sent again
    /// to request a forceful shutdown (requesting that all reconcilers abort on the next yield point).
    ///
    /// NOTE: On Unix this leaves the default handlers for SIGINT and SIGTERM disabled after the [`Controller`] has
    /// terminated. If you run this in a process containing more tasks than just the [`Controller`], ensure that
    /// all other tasks either terminate when the [`Controller`] does, that they have their own signal handlers,
    /// or use [`Controller::graceful_shutdown_on`] to manage your own shutdown strategy.
    ///
    /// NOTE: If developing a Windows service then you need to listen to its lifecycle events instead, and hook that into
    /// [`Controller::graceful_shutdown_on`].
    ///
    /// NOTE: [`Controller::run`] terminates as soon as a forceful shutdown is requested, but leaves the reconcilers running
    /// in the background while they terminate. This will block [`tokio::runtime::Runtime`] termination until they actually terminate,
    /// unless you run [`std::process::exit`] afterwards.
    #[must_use]
    pub fn shutdown_on_signal(mut self) -> Self {
        async fn shutdown_signal() {
            futures::future::select(
                tokio::signal::ctrl_c().map(|_| ()).boxed(),
                #[cfg(unix)]
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .unwrap()
                    .recv()
                    .map(|_| ())
                    .boxed(),
                // Assume that ctrl_c is enough on non-Unix platforms (such as Windows)
                #[cfg(not(unix))]
                futures::future::pending::<()>(),
            )
            .await;
        }

        let (graceful_tx, graceful_rx) = channel::oneshot::channel();
        self.graceful_shutdown_selector
            .push(graceful_rx.map(|_| ()).boxed());
        self.forceful_shutdown_selector.push(
            async {
                tracing::info!("press ctrl+c to shut down gracefully");
                shutdown_signal().await;
                if let Ok(()) = graceful_tx.send(()) {
                    tracing::info!("graceful shutdown requested, press ctrl+c again to force shutdown");
                } else {
                    tracing::info!(
                        "graceful shutdown already requested, press ctrl+c again to force shutdown"
                    );
                }
                shutdown_signal().await;
                tracing::info!("forced shutdown requested");
            }
            .boxed(),
        );
        self
    }

    /// Consume all the parameters of the Controller and start the applier stream
    ///
    /// This creates a stream from all builder calls and starts an applier with
    /// a specified `reconciler` and `error_policy` callbacks. Each of these will be called
    /// with a configurable `context`.
    pub fn run<ReconcilerFut, Ctx>(
        self,
        mut reconciler: impl FnMut(Arc<K>, Arc<Ctx>) -> ReconcilerFut,
        error_policy: impl Fn(Arc<K>, &ReconcilerFut::Error, Arc<Ctx>) -> Action,
        context: Arc<Ctx>,
    ) -> impl Stream<Item = Result<(ObjectRef<K>, Action), Error<ReconcilerFut::Error, watcher::Error>>>
    where
        K::DynamicType: Debug + Unpin,
        ReconcilerFut: TryFuture<Ok = Action> + Send + 'static,
        ReconcilerFut::Error: std::error::Error + Send + 'static,
    {
        applier(
            move |obj, ctx| {
                CancelableJoinHandle::spawn(
                    reconciler(obj, ctx).into_future().in_current_span(),
                    &Handle::current(),
                )
            },
            error_policy,
            context,
            self.reader,
            StreamBackoff::new(self.trigger_selector, self.trigger_backoff)
                .take_until(future::select_all(self.graceful_shutdown_selector)),
            self.config,
        )
        .take_until(futures::future::select_all(self.forceful_shutdown_selector))
    }
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, pin::pin, sync::Arc, time::Duration};

    use super::{Action, APPLIER_REQUEUE_BUF_SIZE};
    use crate::{
        applier,
        reflector::{self, ObjectRef},
        watcher::{self, metadata_watcher, watcher, Event},
        Config, Controller,
    };
    use futures::{Stream, StreamExt, TryStreamExt};
    use k8s_openapi::api::core::v1::ConfigMap;
    use kube_client::{core::ObjectMeta, Api, Resource};
    use serde::de::DeserializeOwned;
    use tokio::time::timeout;

    fn assert_send<T: Send>(x: T) -> T {
        x
    }

    // Used to typecheck that a type T is a generic type that implements Stream
    // and returns a WatchEvent generic over a resource `K`
    fn assert_stream<T, K>(x: T) -> T
    where
        T: Stream<Item = watcher::Result<Event<K>>> + Send,
        K: Resource + Clone + DeserializeOwned + std::fmt::Debug + Send + 'static,
    {
        x
    }

    fn mock_type<T>() -> T {
        unimplemented!(
            "mock_type is not supposed to be called, only used for filling holes in type assertions"
        )
    }

    // not #[test] because we don't want to actually run it, we just want to assert that it typechecks
    #[allow(dead_code, unused_must_use)]
    fn test_controller_should_be_send() {
        assert_send(
            Controller::new(mock_type::<Api<ConfigMap>>(), Default::default()).run(
                |_, _| async { Ok(mock_type::<Action>()) },
                |_: Arc<ConfigMap>, _: &std::io::Error, _| mock_type::<Action>(),
                Arc::new(()),
            ),
        );
    }

    // not #[test] because we don't want to actually run it, we just want to
    // assert that it typechecks
    //
    // will check return types for `watcher` and `watch_metadata` do not drift
    // given an arbitrary K that implements `Resource` (e.g ConfigMap)
    #[allow(dead_code, unused_must_use)]
    fn test_watcher_stream_type_drift() {
        assert_stream(watcher(mock_type::<Api<ConfigMap>>(), Default::default()));
        assert_stream(metadata_watcher(
            mock_type::<Api<ConfigMap>>(),
            Default::default(),
        ));
    }

    #[tokio::test]
    async fn applier_must_not_deadlock_if_reschedule_buffer_fills() {
        // This tests that `applier` handles reschedule queue backpressure correctly, by trying to flood it with no-op reconciles
        // This is intended to avoid regressing on https://github.com/kube-rs/kube/issues/926

        // Assume that we can keep APPLIER_REQUEUE_BUF_SIZE flooded if we have 100x the number of objects "in rotation"
        // On my (@nightkr)'s 3900X I can reliably trigger this with 10x, but let's have some safety margin to avoid false negatives
        let items = APPLIER_REQUEUE_BUF_SIZE * 50;
        // Assume that everything's OK if we can reconcile every object 3 times on average
        let reconciles = items * 3;

        let (queue_tx, queue_rx) = futures::channel::mpsc::unbounded::<ObjectRef<ConfigMap>>();
        let (store_rx, mut store_tx) = reflector::store();
        let mut applier = pin!(applier(
            |_obj, _| {
                Box::pin(async move {
                    // Try to flood the rescheduling buffer buffer by just putting it back in the queue immediately
                    //println!("reconciling {:?}", obj.metadata.name);
                    Ok(Action::requeue(Duration::ZERO))
                })
            },
            |_: Arc<ConfigMap>, _: &Infallible, _| todo!(),
            Arc::new(()),
            store_rx,
            queue_rx.map(Result::<_, Infallible>::Ok),
            Config::default(),
        ));
        store_tx.apply_watcher_event(&watcher::Event::InitDone);
        for i in 0..items {
            let obj = ConfigMap {
                metadata: ObjectMeta {
                    name: Some(format!("cm-{i}")),
                    namespace: Some("default".to_string()),
                    ..Default::default()
                },
                ..Default::default()
            };
            store_tx.apply_watcher_event(&watcher::Event::Apply(obj.clone()));
            queue_tx.unbounded_send(ObjectRef::from_obj(&obj)).unwrap();
        }

        timeout(
            Duration::from_secs(10),
            applier
                .as_mut()
                .take(reconciles)
                .try_for_each(|_| async { Ok(()) }),
        )
        .await
        .expect("test timeout expired, applier likely deadlocked")
        .unwrap();

        // Do an orderly shutdown to ensure that no individual reconcilers are stuck
        drop(queue_tx);
        timeout(
            Duration::from_secs(10),
            applier.try_for_each(|_| async { Ok(()) }),
        )
        .await
        .expect("applier cleanup timeout expired, individual reconciler likely deadlocked?")
        .unwrap();
    }
}
