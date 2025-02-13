//! Caches objects in memory

mod dispatcher;
mod object_ref;
pub mod store;

pub use self::{
    dispatcher::ReflectHandle,
    object_ref::{Extra as ObjectRefExtra, Lookup, ObjectRef},
};
use crate::watcher;
use async_stream::stream;
use futures::{Stream, StreamExt};
use store::CacheWriter as _;
use std::hash::Hash;
#[cfg(feature = "unstable-runtime-subscribe")] pub use store::store_shared;
pub use store::{store, Store};

/// Cache objects from a [`watcher()`] stream into a local [`Store`]
///
/// Observes the raw `Stream` of [`watcher::Event`] objects, and modifies the cache.
/// It passes the raw [`watcher()`] stream through unmodified.
///
/// ## Usage
/// Create a [`Store`] through e.g. [`store::store()`]. The `writer` part is not-clonable,
/// and must be moved into the reflector. The `reader` part is the [`Store`] interface
/// that you can send to other parts of your program as state.
///
/// The cache contains the last-seen state of objects,
/// which may lag slightly behind the actual state.
///
/// ## Example
///
/// Infinite watch of [`Node`](k8s_openapi::api::core::v1::Node) resources with a certain label.
///
/// The `reader` part being passed around to a webserver is omitted.
/// For examples see [version-rs](https://github.com/kube-rs/version-rs) for integration with [axum](https://github.com/tokio-rs/axum),
/// or [controller-rs](https://github.com/kube-rs/controller-rs) for the similar controller integration with [actix-web](https://actix.rs/).
///
/// ```no_run
/// use std::future::ready;
/// use k8s_openapi::api::core::v1::Node;
/// use kube::runtime::{reflector, watcher, WatchStreamExt, watcher::Config};
/// use futures::StreamExt;
/// # use kube::api::Api;
/// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
/// # let client: kube::Client = todo!();
///
/// let nodes: Api<Node> = Api::all(client);
/// let node_filter = Config::default().labels("kubernetes.io/arch=amd64");
/// let (reader, writer) = reflector::store();
///
/// // Create the infinite reflector stream
/// let rf = reflector(writer, watcher(nodes, node_filter));
///
/// // !!! pass reader to your webserver/manager as state !!!
///
/// // Poll the stream (needed to keep the store up-to-date)
/// let infinite_watch = rf.applied_objects().for_each(|o| { ready(()) });
/// infinite_watch.await;
/// # Ok(())
/// # }
/// ```
///
///
/// ## Memory Usage
///
/// A reflector often constitutes one of the biggest components of a controller's memory use.
/// Given a ~2000 pods cluster, a reflector saving everything (including injected sidecars, managed fields)
/// can quickly consume a couple of hundred megabytes or more, depending on how much of this you are storing.
///
/// While generally acceptable, there are techniques you can leverage to reduce the memory usage
/// depending on your use case.
///
/// 1. Reflect a [`PartialObjectMeta<K>`](kube_client::core::PartialObjectMeta) stream rather than a stream of `K`
///
/// You can send in a [`metadata_watcher()`](crate::watcher::metadata_watcher()) for a type rather than a [`watcher()`],
/// and this can drop your memory usage by more than a factor of two,
/// depending on the size of `K`. 60% reduction seen for `Pod`. Usage is otherwise identical.
///
/// 2. Use `modify` the raw [`watcher::Event`] object stream to clear unneeded properties
///
/// For instance, managed fields typically constitutes around half the size of `ObjectMeta` and can often be dropped:
///
/// ```no_run
/// # use futures::TryStreamExt;
/// # use kube::{ResourceExt, Api, runtime::watcher};
/// # let api: Api<k8s_openapi::api::core::v1::Node> = todo!();
/// let stream = watcher(api, Default::default()).map_ok(|ev| {
///     ev.modify(|pod| {
///         pod.managed_fields_mut().clear();
///         pod.annotations_mut().clear();
///         pod.status = None;
///     })
/// });
/// ```
/// The `stream` can then be passed to `reflector` causing smaller objects to be written to its store.
/// Note that you **cannot drop everything**; you minimally need the spec properties your app relies on.
/// Additionally, only `labels`, `annotations` and `managed_fields` are safe to drop from `ObjectMeta`.
///
/// For more information check out: <https://kube.rs/controllers/optimization/> for graphs and techniques.
///
/// ## Stream sharing
///
/// `reflector()` as an interface may optionally create a stream that can be
/// shared with other components to help with resource usage.
///
/// To share a stream, the `Writer<K>` consumed by `reflector()` must be
/// created through an interface that allows a store to be subscribed on, such
/// as [`store_shared()`]. When the store supports being subscribed on, it will
/// broadcast an event to all active listeners after caching any object
/// contained in the event.
///
/// Creating subscribers requires an
/// [`unstable`](https://github.com/kube-rs/kube/blob/main/kube-runtime/Cargo.toml#L17-L21)
/// feature
pub fn reflector<K, W>(mut writer: store::Writer<K>, stream: W) -> impl Stream<Item = W::Item>
where
    K: Lookup + Clone,
    K::DynamicType: Eq + Hash + Clone,
    W: Stream<Item = watcher::Result<watcher::Event<K>>>,
{
    let mut stream = Box::pin(stream);
    stream! {
        while let Some(event) = stream.next().await {
            match event {
                Ok(ev) => {
                    writer.apply_watcher_event(&ev);
                    writer.dispatch_event(&ev).await;
                    yield Ok(ev);
                },
                Err(ev) => yield Err(ev)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{reflector, store, ObjectRef};
    use crate::watcher;
    use futures::{stream, StreamExt, TryStreamExt};
    use k8s_openapi::{api::core::v1::ConfigMap, apimachinery::pkg::apis::meta::v1::ObjectMeta};
    use rand::{
        distr::{Bernoulli, Uniform},
        Rng,
    };
    use std::collections::{BTreeMap, HashMap};

    #[tokio::test]
    async fn reflector_applied_should_add_object() {
        let store_w = store::Writer::default();
        let store = store_w.as_reader();
        let cm = ConfigMap {
            metadata: ObjectMeta {
                name: Some("a".to_string()),
                ..ObjectMeta::default()
            },
            ..ConfigMap::default()
        };
        reflector(store_w, stream::iter(vec![Ok(watcher::Event::Apply(cm.clone()))]))
            .map(|_| ())
            .collect::<()>()
            .await;
        assert_eq!(store.get(&ObjectRef::from_obj(&cm)).as_deref(), Some(&cm));
    }

    #[tokio::test]
    async fn reflector_applied_should_update_object() {
        let store_w = store::Writer::default();
        let store = store_w.as_reader();
        let cm = ConfigMap {
            metadata: ObjectMeta {
                name: Some("a".to_string()),
                ..ObjectMeta::default()
            },
            ..ConfigMap::default()
        };
        let updated_cm = ConfigMap {
            data: Some({
                let mut data = BTreeMap::new();
                data.insert("data".to_string(), "present!".to_string());
                data
            }),
            ..cm.clone()
        };
        reflector(
            store_w,
            stream::iter(vec![
                Ok(watcher::Event::Apply(cm.clone())),
                Ok(watcher::Event::Apply(updated_cm.clone())),
            ]),
        )
        .map(|_| ())
        .collect::<()>()
        .await;
        assert_eq!(store.get(&ObjectRef::from_obj(&cm)).as_deref(), Some(&updated_cm));
    }

    #[tokio::test]
    async fn reflector_deleted_should_remove_object() {
        let store_w = store::Writer::default();
        let store = store_w.as_reader();
        let cm = ConfigMap {
            metadata: ObjectMeta {
                name: Some("a".to_string()),
                ..ObjectMeta::default()
            },
            ..ConfigMap::default()
        };
        reflector(
            store_w,
            stream::iter(vec![
                Ok(watcher::Event::Apply(cm.clone())),
                Ok(watcher::Event::Delete(cm.clone())),
            ]),
        )
        .map(|_| ())
        .collect::<()>()
        .await;
        assert_eq!(store.get(&ObjectRef::from_obj(&cm)), None);
    }

    #[tokio::test]
    async fn reflector_restarted_should_clear_objects() {
        let store_w = store::Writer::default();
        let store = store_w.as_reader();
        let cm_a = ConfigMap {
            metadata: ObjectMeta {
                name: Some("a".to_string()),
                ..ObjectMeta::default()
            },
            ..ConfigMap::default()
        };
        let cm_b = ConfigMap {
            metadata: ObjectMeta {
                name: Some("b".to_string()),
                ..ObjectMeta::default()
            },
            ..ConfigMap::default()
        };
        reflector(
            store_w,
            stream::iter(vec![
                Ok(watcher::Event::Apply(cm_a.clone())),
                Ok(watcher::Event::Init),
                Ok(watcher::Event::InitApply(cm_b.clone())),
                Ok(watcher::Event::InitDone),
            ]),
        )
        .map(|_| ())
        .collect::<()>()
        .await;
        assert_eq!(store.get(&ObjectRef::from_obj(&cm_a)), None);
        assert_eq!(store.get(&ObjectRef::from_obj(&cm_b)).as_deref(), Some(&cm_b));
    }

    #[tokio::test]
    async fn reflector_store_should_not_contain_duplicates() {
        let mut rng = rand::rng();
        let item_dist = Uniform::new(0_u8, 100).unwrap();
        let deleted_dist = Bernoulli::new(0.40).unwrap();
        let store_w = store::Writer::default();
        let store = store_w.as_reader();
        reflector(
            store_w,
            stream::iter((0_u32..100_000).map(|gen| {
                let item = rng.sample(item_dist);
                let deleted = rng.sample(deleted_dist);
                let obj = ConfigMap {
                    metadata: ObjectMeta {
                        name: Some(item.to_string()),
                        resource_version: Some(gen.to_string()),
                        ..ObjectMeta::default()
                    },
                    ..ConfigMap::default()
                };
                Ok(if deleted {
                    watcher::Event::Delete(obj)
                } else {
                    watcher::Event::Apply(obj)
                })
            })),
        )
        .map_ok(|_| ())
        .try_collect::<()>()
        .await
        .unwrap();

        let mut seen_objects = HashMap::new();
        for obj in store.state() {
            assert_eq!(seen_objects.get(obj.metadata.name.as_ref().unwrap()), None);
            seen_objects.insert(obj.metadata.name.clone().unwrap(), obj);
        }
    }
}
