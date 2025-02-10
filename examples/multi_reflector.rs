use futures::{future, StreamExt};
use k8s_openapi::api::{
    apps::v1::Deployment,
    core::v1::{ConfigMap, Secret},
};
use kube::{
    runtime::{
        reflector,
        reflector::{ObjectRef, Store},
        watcher, WatchStreamExt,
    },
    Api, Client,
};
use std::sync::Arc;
use tracing::*;

// This does not work because Resource trait is not dyn safe.
/*
use std::any::TypeId;
use std::collections::HashMap;
use k8s_openapi::NamespaceResourceScope;
use kube::api::{Resource, ResourceExt};
struct MultiStore {
     stores: HashMap<TypeId, Store<dyn Resource<DynamicType = (), Scope = NamespaceResourceScope>>>,
 }
impl MultiStore {
    fn get<K: Resource<DynamicType = ()>>(&self, name: &str, ns: &str) -> Option<Arc<K>> {
        let oref = ObjectRef::<K>::new(name).within(ns);
        if let Some(store) = self.stores.get(&TypeId::of::<K>()) {
            store.get(oref)
        } else {
            None
        }
    }
}*/

// explicit store can work
struct MultiStore {
    deploys: Store<Deployment>,
    cms: Store<ConfigMap>,
    secs: Store<Secret>,
}
// but using generics to help out won't because the K needs to be concretised
/*
impl MultiStore {
    fn get<K: Resource<DynamicType = ()>>(&self, name: &str, ns: &str) -> Option<Arc<Option<K>>> {
        let oref = ObjectRef::<K>::new(name).within(ns);
        let kind = K::kind(&()).to_owned();
        match kind.as_ref() {
            "Deployment" => self.deploys.get(&ObjectRef::new(name).within(ns)),
            "ConfigMap" => self.cms.get(&ObjectRef::new(name).within(ns)),
            "Secret" => self.secs.get(&ObjectRef::new(name).within(ns)),
            _ => None,
        }
        None
    }
}
*/
// so left with this

impl MultiStore {
    fn get_deploy(&self, name: &str, ns: &str) -> Option<Arc<Deployment>> {
        self.deploys.get(&ObjectRef::<Deployment>::new(name).within(ns))
    }

    fn get_secret(&self, name: &str, ns: &str) -> Option<Arc<Secret>> {
        self.secs.get(&ObjectRef::<Secret>::new(name).within(ns))
    }

    fn get_cm(&self, name: &str, ns: &str) -> Option<Arc<ConfigMap>> {
        self.cms.get(&ObjectRef::<ConfigMap>::new(name).within(ns))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let client = Client::try_default().await?;

    let deploys: Api<Deployment> = Api::default_namespaced(client.clone());
    let cms: Api<ConfigMap> = Api::default_namespaced(client.clone());
    let secret: Api<Secret> = Api::default_namespaced(client.clone());

    let (dep_reader, dep_writer) = reflector::store::<Deployment>();
    let (cm_reader, cm_writer) = reflector::store::<ConfigMap>();
    let (sec_reader, sec_writer) = reflector::store::<Secret>();

    let cfg = watcher::Config::default();
    let dep_watcher = watcher(deploys, cfg.clone())
        .reflect(dep_writer)
        .applied_objects()
        .for_each(|_| future::ready(()));
    let cm_watcher = watcher(cms, cfg.clone())
        .reflect(cm_writer)
        .applied_objects()
        .for_each(|_| future::ready(()));
    let sec_watcher = watcher(secret, cfg)
        .reflect(sec_writer)
        .applied_objects()
        .for_each(|_| future::ready(()));
    // poll these forever

    // multistore
    let stores = MultiStore {
        deploys: dep_reader,
        cms: cm_reader,
        secs: sec_reader,
    };

    // simulate doing stuff with the stores from some other thread
    tokio::spawn(async move {
        // Show state every 5 seconds of watching
        info!("waiting for them to be ready");
        stores.deploys.wait_until_ready().await.unwrap();
        stores.cms.wait_until_ready().await.unwrap();
        stores.secs.wait_until_ready().await.unwrap();
        info!("stores initialised");
        // can use helper accessors
        info!(
            "common cm: {:?}",
            stores.get_cm("kube-root-ca.crt", "kube-system").unwrap()
        );
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            // access individual sub stores
            info!("Current deploys count: {}", stores.deploys.state().len());
        }
    });
    // info!("long watches starting");
    tokio::select! {
        r = dep_watcher => println!("dep watcher exit: {r:?}"),
        r = cm_watcher => println!("cm watcher exit: {r:?}"),
        r = sec_watcher => println!("sec watcher exit: {r:?}"),
    }

    Ok(())
}
