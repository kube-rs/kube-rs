use futures::{future, stream, StreamExt};
use k8s_openapi::api::{
    apps::v1::Deployment,
    core::v1::{ConfigMap, Secret},
};
use kube::{
    api::{ApiResource, DynamicObject, GroupVersionKind},
    core::TypedResource,
    runtime::{reflector::store::CacheWriter, watcher, WatchStreamExt},
    Api, Client, Resource,
};
use parking_lot::RwLock;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use tracing::*;

use std::collections::HashMap;

type Cache = Arc<RwLock<HashMap<LookupKey, Arc<DynamicObject>>>>;

#[derive(Default, Clone, Hash, PartialEq, Eq, Debug)]
struct LookupKey {
    gvk: GroupVersionKind,
    name: Option<String>,
    namespace: Option<String>,
}

impl LookupKey {
    fn new<R: TypedResource>(resource: &R) -> LookupKey {
        let meta = resource.meta();
        LookupKey {
            gvk: resource.gvk(),
            name: meta.name.clone(),
            namespace: meta.namespace.clone(),
        }
    }
}

#[derive(Default, Clone)]
struct MultiCache {
    store: Cache,
}

impl MultiCache {
    fn get<K: Resource<DynamicType = impl Default> + DeserializeOwned + Clone>(
        &self,
        name: &str,
        ns: &str,
    ) -> Option<Arc<K>> {
        let obj = self
            .store
            .read()
            .get(&LookupKey {
                gvk: K::gvk(&Default::default()),
                name: Some(name.into()),
                namespace: if !ns.is_empty() { Some(ns.into()) } else { None },
            })?
            .as_ref()
            .clone();
        obj.try_parse().ok().map(Arc::new)
    }
}

impl CacheWriter<DynamicObject> for MultiCache {
    /// Applies a single watcher event to the store
    fn apply_watcher_event(&mut self, event: &watcher::Event<DynamicObject>) {
        match event {
            watcher::Event::Init | watcher::Event::InitDone => {}
            watcher::Event::Delete(obj) => {
                self.store.write().remove(&LookupKey::new(obj));
            }
            watcher::Event::InitApply(obj) | watcher::Event::Apply(obj) => {
                self.store
                    .write()
                    .insert(LookupKey::new(obj), Arc::new(obj.clone()));
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let client = Client::try_default().await?;

    // multistore
    let mut combo_stream = stream::select_all(vec![]);
    combo_stream.push(
        watcher::watcher(
            Api::all_with(client.clone(), &ApiResource::erase::<Deployment>(&())),
            Default::default(),
        )
        .boxed(),
    );
    combo_stream.push(
        watcher::watcher(
            Api::all_with(client.clone(), &ApiResource::erase::<ConfigMap>(&())),
            Default::default(),
        )
        .boxed(),
    );

    // // Duplicate streams with narrowed down selection
    combo_stream.push(
        watcher::watcher(
            Api::default_namespaced_with(client.clone(), &ApiResource::erase::<Secret>(&())),
            Default::default(),
        )
        .boxed(),
    );
    combo_stream.push(
        watcher::watcher(
            Api::all_with(client.clone(), &ApiResource::erase::<Secret>(&())),
            Default::default(),
        )
        .boxed(),
    );

    let multi_writer = MultiCache::default();
    let watcher = combo_stream
        .reflect(multi_writer.clone())
        .applied_objects()
        .for_each(|_| future::ready(()));

    // simulate doing stuff with the stores from some other thread
    tokio::spawn(async move {
        // can use helper accessors
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            info!("cache content: {:?}", multi_writer.store.read().keys());
            info!(
                "common cm: {:?}",
                multi_writer.get::<ConfigMap>("kube-root-ca.crt", "kube-system")
            );
            // access individual sub stores
            info!("Current objects count: {}", multi_writer.store.read().len());
        }
    });
    info!("long watches starting");
    tokio::select! {
        r = watcher => println!("watcher exit: {r:?}"),
    }

    Ok(())
}
