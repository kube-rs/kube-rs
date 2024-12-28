use super::{dispatcher::Dispatcher, Lookup, ObjectRef};
#[cfg(feature = "unstable-runtime-subscribe")]
use crate::reflector::ReflectHandle;
use crate::{
    utils::delayed_init::{self, DelayedInit},
    watcher,
};
use ahash::AHashMap;
use educe::Educe;
use parking_lot::RwLock;
use std::{fmt::Debug, hash::Hash, sync::Arc};
use thiserror::Error;

type Cache<K> = Arc<RwLock<AHashMap<ObjectRef<K>, Arc<K>>>>;

/// A writable Store handle
///
/// This is exclusive since it's not safe to share a single `Store` between multiple reflectors.
/// In particular, `Restarted` events will clobber the state of other connected reflectors.
#[derive(Debug)]
pub struct Writer<K: 'static + Lookup + Clone>
where
    K::DynamicType: Eq + Hash + Clone,
{
    store: Cache<K>,
    buffer: AHashMap<ObjectRef<K>, Arc<K>>,
    dyntype: K::DynamicType,
    ready_tx: Option<delayed_init::Initializer<()>>,
    ready_rx: Arc<DelayedInit<()>>,
    dispatcher: Option<Dispatcher<K>>,
}

impl<K: 'static + Lookup + Clone> Writer<K>
where
    K::DynamicType: Eq + Hash + Clone,
{
    /// Creates a new Writer with the specified dynamic type.
    ///
    /// If the dynamic type is default-able (for example when writer is used with
    /// `k8s_openapi` types) you can use `Default` instead.
    pub fn new(dyntype: K::DynamicType) -> Self {
        let (ready_tx, ready_rx) = DelayedInit::new();
        Writer {
            store: Default::default(),
            buffer: Default::default(),
            dyntype,
            ready_tx: Some(ready_tx),
            ready_rx: Arc::new(ready_rx),
            dispatcher: None,
        }
    }

    /// Creates a new Writer with the specified dynamic type and buffer size.
    ///
    /// When the Writer is created through `new_shared`, it will be able to
    /// be subscribed. Stored objects will be propagated to all subscribers. The
    /// buffer size is used for the underlying channel. An object is cleared
    /// from the buffer only when all subscribers have seen it.
    ///
    /// If the dynamic type is default-able (for example when writer is used with
    /// `k8s_openapi` types) you can use `Default` instead.
    #[cfg(feature = "unstable-runtime-subscribe")]
    pub fn new_shared(buf_size: usize, dyntype: K::DynamicType) -> Self {
        let (ready_tx, ready_rx) = DelayedInit::new();
        Writer {
            store: Default::default(),
            buffer: Default::default(),
            dyntype,
            ready_tx: Some(ready_tx),
            ready_rx: Arc::new(ready_rx),
            dispatcher: Some(Dispatcher::new(buf_size)),
        }
    }

    /// Return a read handle to the store
    ///
    /// Multiple read handles may be obtained, by either calling `as_reader` multiple times,
    /// or by calling `Store::clone()` afterwards.
    #[must_use]
    pub fn as_reader(&self) -> Store<K> {
        Store {
            store: self.store.clone(),
            ready_rx: self.ready_rx.clone(),
        }
    }

    /// Return a handle to a subscriber
    ///
    /// Multiple subscribe handles may be obtained, by either calling
    /// `subscribe` multiple times, or by calling `clone()`
    ///
    /// This function returns a `Some` when the [`Writer`] is constructed through
    /// [`Writer::new_shared`] or [`store_shared`], and a `None` otherwise.
    #[cfg(feature = "unstable-runtime-subscribe")]
    pub fn subscribe(&self) -> Option<ReflectHandle<K>> {
        self.dispatcher
            .as_ref()
            .map(|dispatcher| dispatcher.subscribe(self.as_reader()))
    }

    /// Applies a single watcher event to the store
    pub fn apply_watcher_event(&mut self, event: &watcher::Event<K>) {
        match event {
            watcher::Event::Apply(obj) => {
                let key = obj.to_object_ref(self.dyntype.clone());
                let obj = Arc::new(obj.clone());
                self.store.write().insert(key, obj);
            }
            watcher::Event::Delete(obj) => {
                let mut key = obj.to_object_ref(self.dyntype.clone());
                let mut store = self.store.write();
                store.remove(&key);
                if self.dispatcher.is_some() {
                    // Re-insert the entry with updated key, as insert on its own doesnt modify the key
                    key.extra.remaining_lookups = self.dispatcher.as_ref().map(Dispatcher::subscribers);
                    store.insert(key, Arc::new(obj.clone()));
                }
            }
            watcher::Event::Init => {
                self.buffer = AHashMap::new();
            }
            watcher::Event::InitApply(obj) => {
                let key = obj.to_object_ref(self.dyntype.clone());
                let obj = Arc::new(obj.clone());
                self.buffer.insert(key, obj);
            }
            watcher::Event::InitDone => {
                let mut store = self.store.write();

                // Swap the buffer into the store
                std::mem::swap(&mut *store, &mut self.buffer);

                // Clear the buffer
                // This is preferred over self.buffer.clear(), as clear() will keep the allocated memory for reuse.
                // This way, the old buffer is dropped.
                self.buffer = AHashMap::new();

                // Mark as ready after the Restart, "releasing" any calls to Store::wait_until_ready()
                if let Some(ready_tx) = self.ready_tx.take() {
                    ready_tx.init(())
                }
            }
        }
    }

    /// Broadcast an event to any downstream listeners subscribed on the store
    pub(crate) async fn dispatch_event(&mut self, event: &watcher::Event<K>) {
        if let Some(ref mut dispatcher) = self.dispatcher {
            match event {
                watcher::Event::Apply(obj) => {
                    let obj_ref = obj.to_object_ref(self.dyntype.clone());
                    // TODO (matei): should this take a timeout to log when backpressure has
                    // been applied for too long, e.g. 10s
                    dispatcher.broadcast(obj_ref).await;
                }

                watcher::Event::InitDone => {
                    let obj_refs: Vec<_> = {
                        let store = self.store.read();
                        store.keys().cloned().collect()
                    };

                    for obj_ref in obj_refs {
                        dispatcher.broadcast(obj_ref).await;
                    }
                }

                watcher::Event::Delete(obj) => {
                    let mut obj_ref = obj.to_object_ref(self.dyntype.clone());
                    obj_ref.extra.remaining_lookups = Some(dispatcher.subscribers());
                    dispatcher.broadcast(obj_ref).await;
                }

                _ => {}
            }
        }
    }
}

impl<K> Default for Writer<K>
where
    K: Lookup + Clone + 'static,
    K::DynamicType: Default + Eq + Hash + Clone,
{
    fn default() -> Self {
        Self::new(K::DynamicType::default())
    }
}

/// A readable cache of Kubernetes objects of kind `K`
///
/// Cloning will produce a new reference to the same backing store.
///
/// Cannot be constructed directly since one writer handle is required,
/// use `Writer::as_reader()` instead.
#[derive(Educe)]
#[educe(Debug(bound("K: Debug, K::DynamicType: Debug")), Clone)]
pub struct Store<K: 'static + Lookup>
where
    K::DynamicType: Hash + Eq,
{
    store: Cache<K>,
    ready_rx: Arc<DelayedInit<()>>,
}

#[derive(Debug, Error)]
#[error("writer was dropped before store became ready")]
pub struct WriterDropped(delayed_init::InitDropped);

impl<K: 'static + Clone + Lookup> Store<K>
where
    K::DynamicType: Eq + Hash + Clone,
{
    /// Wait for the store to be populated by Kubernetes.
    ///
    /// Note that polling this will _not_ await the source of the stream that populates the [`Writer`].
    /// The [`reflector`](crate::reflector()) stream must be awaited separately.
    ///
    /// # Errors
    /// Returns an error if the [`Writer`] was dropped before any value was written.
    pub async fn wait_until_ready(&self) -> Result<(), WriterDropped> {
        self.ready_rx.get().await.map_err(WriterDropped)
    }

    /// Retrieve a `clone()` of the entry referred to by `key`, if it is in the cache.
    ///
    /// `key.namespace` is ignored for cluster-scoped resources.
    ///
    /// Note that this is a cache and may be stale. Deleted objects may still exist in the cache
    /// despite having been deleted in the cluster, and new objects may not yet exist in the cache.
    /// If any of these are a problem for you then you should abort your reconciler and retry later.
    /// If you use `kube_rt::controller` then you can do this by returning an error and specifying a
    /// reasonable `error_policy`.
    #[must_use]
    pub fn get(&self, key: &ObjectRef<K>) -> Option<Arc<K>> {
        let store = self.store.read();
        store
            .get(key)
            // Try to erase the namespace and try again, in case the object is cluster-scoped
            .or_else(|| {
                store.get(&{
                    let mut cluster_key = key.clone();
                    cluster_key.namespace = None;
                    cluster_key
                })
            })
            // Clone to let go of the entry lock ASAP
            .cloned()
    }

    #[must_use]
    pub fn remove(&self, key: &ObjectRef<K>) -> Option<Arc<K>> {
        let mut store = self.store.write();
        store.remove_entry(key).map(|(mut key, obj)| {
            match key.extra.remaining_lookups {
                Some(..=1) | None => (),
                Some(lookups) => {
                    key.extra.remaining_lookups = Some(lookups - 1);
                    store.insert(key, obj.clone());
                }
            };

            obj
        })
    }

    /// Return a full snapshot of the current values
    #[must_use]
    pub fn state(&self) -> Vec<Arc<K>> {
        let s = self.store.read();
        s.values().cloned().collect()
    }

    /// Retrieve a `clone()` of the entry found by the given predicate
    #[must_use]
    pub fn find<P>(&self, predicate: P) -> Option<Arc<K>>
    where
        P: Fn(&K) -> bool,
    {
        self.store
            .read()
            .iter()
            .map(|(_, k)| k)
            .find(|k| predicate(k.as_ref()))
            .cloned()
    }

    /// Return the number of elements in the store
    #[must_use]
    pub fn len(&self) -> usize {
        self.store.read().len()
    }

    /// Return whether the store is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.store.read().is_empty()
    }
}

/// Create a (Reader, Writer) for a `Store<K>` for a typed resource `K`
///
/// The `Writer` should be passed to a [`reflector`](crate::reflector()),
/// and the [`Store`] is a read-only handle.
#[must_use]
pub fn store<K>() -> (Store<K>, Writer<K>)
where
    K: Lookup + Clone + 'static,
    K::DynamicType: Eq + Hash + Clone + Default,
{
    let w = Writer::<K>::default();
    let r = w.as_reader();
    (r, w)
}

/// Create a (Reader, Writer) for a `Store<K>` for a typed resource `K`
///
/// The resulting `Writer` can be subscribed on in order to fan out events from
/// a watcher. The `Writer` should be passed to a [`reflector`](crate::reflector()),
/// and the [`Store`] is a read-only handle.
///
/// A buffer size is used for the underlying message channel. When the buffer is
/// full, backpressure will be applied by waiting for capacity.
#[must_use]
#[allow(clippy::module_name_repetitions)]
#[cfg(feature = "unstable-runtime-subscribe")]
pub fn store_shared<K>(buf_size: usize) -> (Store<K>, Writer<K>)
where
    K: Lookup + Clone + 'static,
    K::DynamicType: Eq + Hash + Clone + Default,
{
    let w = Writer::<K>::new_shared(buf_size, Default::default());
    let r = w.as_reader();
    (r, w)
}

#[cfg(test)]
mod tests {
    use super::{store, Writer};
    use crate::{reflector::ObjectRef, watcher};
    use k8s_openapi::api::core::v1::ConfigMap;
    use kube_client::api::ObjectMeta;

    #[test]
    fn should_allow_getting_namespaced_object_by_namespaced_ref() {
        let cm = ConfigMap {
            metadata: ObjectMeta {
                name: Some("obj".to_string()),
                namespace: Some("ns".to_string()),
                ..ObjectMeta::default()
            },
            ..ConfigMap::default()
        };
        let mut store_w = Writer::default();
        store_w.apply_watcher_event(&watcher::Event::Apply(cm.clone()));
        let store = store_w.as_reader();
        assert_eq!(store.get(&ObjectRef::from_obj(&cm)).as_deref(), Some(&cm));
    }

    #[test]
    fn should_not_allow_getting_namespaced_object_by_clusterscoped_ref() {
        let cm = ConfigMap {
            metadata: ObjectMeta {
                name: Some("obj".to_string()),
                namespace: Some("ns".to_string()),
                ..ObjectMeta::default()
            },
            ..ConfigMap::default()
        };
        let mut cluster_cm = cm.clone();
        cluster_cm.metadata.namespace = None;
        let mut store_w = Writer::default();
        store_w.apply_watcher_event(&watcher::Event::Apply(cm));
        let store = store_w.as_reader();
        assert_eq!(store.get(&ObjectRef::from_obj(&cluster_cm)), None);
    }

    #[test]
    fn should_allow_getting_clusterscoped_object_by_clusterscoped_ref() {
        let cm = ConfigMap {
            metadata: ObjectMeta {
                name: Some("obj".to_string()),
                namespace: None,
                ..ObjectMeta::default()
            },
            ..ConfigMap::default()
        };
        let (store, mut writer) = store();
        writer.apply_watcher_event(&watcher::Event::Apply(cm.clone()));
        assert_eq!(store.get(&ObjectRef::from_obj(&cm)).as_deref(), Some(&cm));
    }

    #[test]
    fn should_allow_getting_clusterscoped_object_by_namespaced_ref() {
        let cm = ConfigMap {
            metadata: ObjectMeta {
                name: Some("obj".to_string()),
                namespace: None,
                ..ObjectMeta::default()
            },
            ..ConfigMap::default()
        };
        #[allow(clippy::redundant_clone)] // false positive
        let mut nsed_cm = cm.clone();
        nsed_cm.metadata.namespace = Some("ns".to_string());
        let mut store_w = Writer::default();
        store_w.apply_watcher_event(&watcher::Event::Apply(cm.clone()));
        let store = store_w.as_reader();
        assert_eq!(store.get(&ObjectRef::from_obj(&nsed_cm)).as_deref(), Some(&cm));
    }

    #[test]
    fn find_element_in_store() {
        let cm = ConfigMap {
            metadata: ObjectMeta {
                name: Some("obj".to_string()),
                namespace: None,
                ..ObjectMeta::default()
            },
            ..ConfigMap::default()
        };
        let mut target_cm = cm.clone();

        let (reader, mut writer) = store::<ConfigMap>();
        assert!(reader.is_empty());
        writer.apply_watcher_event(&watcher::Event::Apply(cm));

        assert_eq!(reader.len(), 1);
        assert!(reader.find(|k| k.metadata.generation == Some(1234)).is_none());

        target_cm.metadata.name = Some("obj1".to_string());
        target_cm.metadata.generation = Some(1234);
        writer.apply_watcher_event(&watcher::Event::Apply(target_cm.clone()));
        assert!(!reader.is_empty());
        assert_eq!(reader.len(), 2);
        let found = reader.find(|k| k.metadata.generation == Some(1234));
        assert_eq!(found.as_deref(), Some(&target_cm));
    }
}
