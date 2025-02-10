//! Kube is an umbrella-crate for interacting with [Kubernetes](http://kubernetes.io) in Rust.
//!
//! # Overview
//!
//! Kube contains a Kubernetes client, a controller runtime, a custom resource derive, and various tooling
//! required for building applications or controllers that interact with Kubernetes.
//!
//! The main modules are:
//!
//! - [`client`] with the Kubernetes [`Client`] and its layers
//! - [`config`] for cluster [`Config`]
//! - [`api`] with the generic Kubernetes [`Api`]
//! - [`derive`](kube_derive) with the [`CustomResource`] / [`Resource`](kube_derive::Resource) derive for building controllers types
//! - [`runtime`] with a [`Controller`](crate::runtime::Controller) / [`watcher`](crate::runtime::watcher()) / [`reflector`](crate::runtime::reflector::reflector) / [`Store`](crate::runtime::reflector::Store)
//! - [`core`] with generics from `apimachinery`
//!
//! You can use each of these as you need with the help of the [exported features](https://kube.rs/features/).
//!
//! # Using the Client
//! ```no_run
//! use futures::{StreamExt, TryStreamExt};
//! use kube::{Client, api::{Api, ResourceExt, ListParams, PostParams}};
//! use k8s_openapi::api::core::v1::Pod;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Infer the runtime environment and try to create a Kubernetes Client
//!     let client = Client::try_default().await?;
//!
//!     // Read pods in the configured namespace into the typed interface from k8s-openapi
//!     let pods: Api<Pod> = Api::default_namespaced(client);
//!     for p in pods.list(&ListParams::default()).await? {
//!         println!("found pod {}", p.name_any());
//!     }
//!     Ok(())
//! }
//! ```
//!
//! For details, see:
//!
//! - [`Client`](crate::client) for the extensible Kubernetes client
//! - [`Api`] for the generic api methods available on Kubernetes resources
//! - [k8s-openapi](https://docs.rs/k8s-openapi/*/k8s_openapi/) for documentation about the generated Kubernetes types
//!
//! # Using the Runtime with the Derive macro
//!
//! ```no_run
//! use schemars::JsonSchema;
//! use serde::{Deserialize, Serialize};
//! use serde_json::json;
//! use futures::{StreamExt, TryStreamExt};
//! use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
//! use kube::{
//!     api::{Api, DeleteParams, PatchParams, Patch, ResourceExt},
//!     core::CustomResourceExt,
//!     Client, CustomResource,
//!     runtime::{watcher, WatchStreamExt, wait::{conditions, await_condition}},
//! };
//!
//! // Our custom resource
//! #[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
//! #[kube(group = "clux.dev", version = "v1", kind = "Foo", namespaced)]
//! pub struct FooSpec {
//!     info: String,
//!     #[schemars(length(min = 3))]
//!     name: String,
//!     replicas: i32,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = Client::try_default().await?;
//!     let crds: Api<CustomResourceDefinition> = Api::all(client.clone());
//!
//!     // Apply the CRD so users can create Foo instances in Kubernetes
//!     crds.patch("foos.clux.dev",
//!         &PatchParams::apply("my_manager"),
//!         &Patch::Apply(Foo::crd())
//!     ).await?;
//!
//!     // Wait for the CRD to be ready
//!     tokio::time::timeout(
//!         std::time::Duration::from_secs(10),
//!         await_condition(crds, "foos.clux.dev", conditions::is_crd_established())
//!     ).await?;
//!
//!     // Watch for changes to foos in the configured namespace
//!     let foos: Api<Foo> = Api::default_namespaced(client.clone());
//!     let wc = watcher::Config::default();
//!     let mut apply_stream = watcher(foos, wc).applied_objects().boxed();
//!     while let Some(f) = apply_stream.try_next().await? {
//!         println!("saw apply to {}", f.name_any());
//!     }
//!     Ok(())
//! }
//! ```
//!
//! For details, see:
//!
//! - [`CustomResource`] for documentation how to configure custom resources
//! - [`runtime::watcher`](crate::runtime::watcher()) for how to long-running watches work and why you want to use this over [`Api::watch`](crate::Api::watch)
//! - [`runtime`] for abstractions that help with more complicated Kubernetes application
//!
//! # Examples
//! A large list of complete, runnable examples with explainations are available in the [examples folder](https://github.com/kube-rs/kube/tree/main/examples).
#![cfg_attr(docsrs, feature(doc_cfg))]

macro_rules! cfg_client {
    ($($item:item)*) => {
        $(
            #[cfg_attr(docsrs, doc(cfg(feature = "client")))]
            #[cfg(feature = "client")]
            $item
        )*
    }
}
macro_rules! cfg_config {
    ($($item:item)*) => {
        $(
            #[cfg_attr(docsrs, doc(cfg(feature = "config")))]
            #[cfg(feature = "config")]
            $item
        )*
    }
}

macro_rules! cfg_error {
    ($($item:item)*) => {
        $(
            #[cfg_attr(docsrs, doc(cfg(any(feature = "config", feature = "client"))))]
            #[cfg(any(feature = "config", feature = "client"))]
            $item
        )*
    }
}

cfg_client! {
    pub use kube_client::api;
    pub use kube_client::discovery;
    pub use kube_client::client;

    #[doc(inline)]
    pub use api::Api;
    #[doc(inline)]
    pub use client::Client;
    #[doc(inline)]
    pub use discovery::Discovery;
}

cfg_config! {
    pub use kube_client::config;
    #[doc(inline)]
    pub use config::Config;
}

cfg_error! {
    pub use kube_client::error;
    #[doc(inline)] pub use error::Error;
    /// Convient alias for `Result<T, Error>`
    pub type Result<T, E = Error> = std::result::Result<T, E>;
}

#[cfg(feature = "derive")]
#[cfg_attr(docsrs, doc(cfg(feature = "derive")))]
pub use kube_derive::CustomResource;

#[cfg(feature = "derive")]
#[cfg_attr(docsrs, doc(cfg(feature = "derive")))]
pub use kube_derive::Resource;

#[cfg(feature = "derive")]
#[cfg_attr(docsrs, doc(cfg(feature = "derive")))]
pub use kube_derive::CELSchema;

#[cfg(feature = "runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "runtime")))]
#[doc(inline)]
pub use kube_runtime as runtime;

pub use crate::core::{CustomResourceExt, Resource, ResourceExt};
#[doc(inline)] pub use kube_core as core;

// Mock tests for the runtime
#[cfg(test)]
#[cfg(all(feature = "derive", feature = "runtime"))]
mod mock_tests;

pub mod prelude {
    //! A prelude for kube. Reduces the number of duplicated imports.
    //!
    //! This prelude is similar to the standard library's prelude in that you'll
    //! almost always want to import its entire contents, but unlike the
    //! standard library's prelude you'll have to do so manually:
    //!
    //! ```
    //! use kube::prelude::*;
    //! ```
    //!
    //! The prelude may grow over time as additional items see ubiquitous use.

    #[cfg(feature = "client")]
    #[allow(unreachable_pub)]
    pub use crate::client::ConfigExt as _;

    #[cfg(feature = "unstable-client")] pub use crate::client::scope::NamespacedRef;

    #[allow(unreachable_pub)] pub use crate::core::PartialObjectMetaExt as _;
    #[allow(unreachable_pub)] pub use crate::core::SelectorExt as _;
    pub use crate::{core::crd::CustomResourceExt as _, Resource as _, ResourceExt as _};

    #[cfg(feature = "runtime")] pub use crate::runtime::utils::WatchStreamExt as _;
}

// Tests that require a cluster and the complete feature set
// Can be run with `cargo test -p kube --lib --features=runtime,derive -- --ignored`
#[cfg(test)]
#[cfg(all(feature = "derive", feature = "client"))]
mod test {
    use crate::{
        api::{DeleteParams, Patch, PatchParams},
        Api, Client, CustomResourceExt, Resource, ResourceExt,
    };
    use kube_derive::CustomResource;
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    #[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
    #[kube(group = "clux.dev", version = "v1", kind = "Foo", namespaced)]
    #[kube(status = "FooStatus")]
    #[kube(scale(
        spec_replicas_path = ".spec.replicas",
        status_replicas_path = ".status.replicas"
    ))]
    #[kube(crates(kube_core = "crate::core"))] // for dev-dep test structure
    pub struct FooSpec {
        name: String,
        info: Option<String>,
        replicas: isize,
    }

    #[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
    pub struct FooStatus {
        is_bad: bool,
        replicas: isize,
    }

    #[tokio::test]
    #[ignore = "needs kubeconfig"]
    async fn custom_resource_generates_correct_core_structs() {
        use crate::core::{ApiResource, DynamicObject, GroupVersionKind};
        let client = Client::try_default().await.unwrap();

        let gvk = GroupVersionKind::gvk("clux.dev", "v1", "Foo");
        let api_resource = ApiResource::from_gvk(&gvk);
        let a1: Api<DynamicObject> = Api::namespaced_with(client.clone(), "myns", &api_resource);
        let a2: Api<Foo> = Api::namespaced(client, "myns");

        // make sure they return the same url_path through their impls
        assert_eq!(a1.resource_url(), a2.resource_url());
    }

    use k8s_openapi::{
        api::core::v1::ConfigMap,
        apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition,
    };
    #[tokio::test]
    #[ignore = "needs cluster (creates + patches foo crd)"]
    #[cfg(all(feature = "derive", feature = "runtime"))]
    async fn derived_resource_queriable_and_has_subresources() -> Result<(), Box<dyn std::error::Error>> {
        use crate::runtime::wait::{await_condition, conditions};

        use serde_json::json;
        let client = Client::try_default().await?;
        let ssapply = PatchParams::apply("kube").force();
        let crds: Api<CustomResourceDefinition> = Api::all(client.clone());
        // Server-side apply CRD and wait for it to get ready
        crds.patch("foos.clux.dev", &ssapply, &Patch::Apply(Foo::crd()))
            .await?;
        let establish = await_condition(crds.clone(), "foos.clux.dev", conditions::is_crd_established());
        let _ = tokio::time::timeout(std::time::Duration::from_secs(10), establish).await?;
        // Use it
        let foos: Api<Foo> = Api::default_namespaced(client.clone());
        // Apply from generated struct
        {
            let foo = Foo::new("baz", FooSpec {
                name: "baz".into(),
                info: Some("old baz".into()),
                replicas: 1,
            });
            let o = foos.patch("baz", &ssapply, &Patch::Apply(&foo)).await?;
            assert_eq!(o.spec.name, "baz");
            let oref = o.object_ref(&());
            assert_eq!(oref.name.unwrap(), "baz");
            assert_eq!(oref.uid, o.uid());
        }
        // Apply from partial json!
        {
            let patch = json!({
                "apiVersion": "clux.dev/v1",
                "kind": "Foo",
                "spec": {
                    "name": "foo",
                    "replicas": 2
                }
            });
            let o = foos.patch("baz", &ssapply, &Patch::Apply(patch)).await?;
            assert_eq!(o.spec.replicas, 2, "patching spec updated spec.replicas");
        }
        // check subresource
        {
            let scale = foos.get_scale("baz").await?;
            assert_eq!(scale.spec.unwrap().replicas, Some(2));
            let status = foos.get_status("baz").await?;
            assert!(status.status.is_none(), "nothing has set status");
        }
        // set status subresource
        {
            let fs = serde_json::json!({"status": FooStatus { is_bad: false, replicas: 1 }});
            let o = foos
                .patch_status("baz", &Default::default(), &Patch::Merge(&fs))
                .await?;
            assert!(o.status.is_some(), "status set after patch_status");
        }
        // set scale subresource
        {
            let fs = serde_json::json!({"spec": { "replicas": 3 }});
            let o = foos
                .patch_scale("baz", &Default::default(), &Patch::Merge(&fs))
                .await?;
            assert_eq!(o.status.unwrap().replicas, 1, "scale replicas got patched");
            let linked_replicas = o.spec.unwrap().replicas.unwrap();
            assert_eq!(linked_replicas, 3, "patch_scale updates linked spec.replicas");
        }

        // cleanup
        foos.delete_collection(&DeleteParams::default(), &Default::default())
            .await?;
        crds.delete("foos.clux.dev", &DeleteParams::default()).await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "needs cluster (lists pods)"]
    async fn custom_serialized_objects_are_queryable_and_iterable() -> Result<(), Box<dyn std::error::Error>>
    {
        use crate::core::{
            object::{HasSpec, HasStatus, NotUsed, Object},
            ApiResource,
        };
        use k8s_openapi::api::core::v1::Pod;
        #[derive(Clone, Deserialize, Debug)]
        struct PodSpecSimple {
            containers: Vec<ContainerSimple>,
        }
        #[derive(Clone, Deserialize, Debug)]
        struct ContainerSimple {
            #[allow(dead_code)]
            image: String,
        }
        type PodSimple = Object<PodSpecSimple, NotUsed>;

        // use known type information from pod (can also use discovery for this)
        let ar = ApiResource::erase::<Pod>(&());

        let client = Client::try_default().await?;
        let api: Api<PodSimple> = Api::default_namespaced_with(client, &ar);
        let mut list = api.list(&Default::default()).await?;
        // check we can mutably iterate over ObjectList
        for pod in &mut list {
            pod.spec_mut().containers = vec![];
            *pod.status_mut() = None;
            pod.annotations_mut()
                .entry("kube-seen".to_string())
                .or_insert_with(|| "yes".to_string());
            pod.labels_mut()
                .entry("kube.rs".to_string())
                .or_insert_with(|| "hello".to_string());
            pod.finalizers_mut().push("kube-finalizer".to_string());
            pod.managed_fields_mut().clear();
            // NB: we are **not** pushing these back upstream - (Api::apply or Api::replace needed for it)
        }
        // check we can iterate over ObjectList normally - and check the mutations worked
        for pod in list {
            assert!(pod.annotations().get("kube-seen").is_some());
            assert!(pod.labels().get("kube.rs").is_some());
            assert!(pod.finalizers().contains(&"kube-finalizer".to_string()));
            assert!(pod.spec().containers.is_empty());
            assert!(pod.managed_fields().is_empty());
        }
        Ok(())
    }

    #[tokio::test]
    #[ignore = "needs cluster (fetches api resources, and lists all)"]
    #[cfg(feature = "derive")]
    async fn derived_resources_discoverable() -> Result<(), Box<dyn std::error::Error>> {
        use crate::{
            core::{DynamicObject, GroupVersion, GroupVersionKind},
            discovery::{self, verbs, ApiGroup, Discovery, Scope},
            runtime::wait::{await_condition, conditions, Condition},
        };

        #[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
        #[kube(group = "kube.rs", version = "v1", kind = "TestCr", namespaced)]
        #[kube(crates(kube_core = "crate::core"))] // for dev-dep test structure
        struct TestCrSpec {}

        let client = Client::try_default().await?;

        // install crd is installed
        let crds: Api<CustomResourceDefinition> = Api::all(client.clone());
        let ssapply = PatchParams::apply("kube").force();
        crds.patch("testcrs.kube.rs", &ssapply, &Patch::Apply(TestCr::crd()))
            .await?;
        let establish = await_condition(crds.clone(), "testcrs.kube.rs", conditions::is_crd_established());
        let crd = tokio::time::timeout(std::time::Duration::from_secs(10), establish).await??;
        assert!(conditions::is_crd_established().matches_object(crd.as_ref()));
        tokio::time::sleep(std::time::Duration::from_secs(2)).await; // Established condition is actually not enough for api discovery :(

        // create partial information for it to discover
        let gvk = GroupVersionKind::gvk("kube.rs", "v1", "TestCr");
        let gv = GroupVersion::gv("kube.rs", "v1");

        // discover by both (recommended kind on groupversion) and (pinned gvk) and they should equal
        let apigroup = discovery::oneshot::pinned_group(&client, &gv).await?;
        let (ar1, caps1) = apigroup.recommended_kind("TestCr").unwrap();
        let (ar2, caps2) = discovery::pinned_kind(&client, &gvk).await?;
        assert_eq!(caps1.operations.len(), caps2.operations.len(), "unequal caps");
        assert_eq!(ar1, ar2, "unequal apiresource");
        assert_eq!(DynamicObject::api_version(&ar2), "kube.rs/v1", "unequal dynver");

        // run (almost) full discovery
        let discovery = Discovery::new(client.clone())
            // skip something in discovery (clux.dev crd being mutated in other tests)
            .exclude(&["rbac.authorization.k8s.io", "clux.dev"])
            .run()
            .await?;

        // check our custom resource first by resolving within groups
        assert!(discovery.has_group("kube.rs"), "missing group kube.rs");
        let (ar, _caps) = discovery.resolve_gvk(&gvk).unwrap();
        assert_eq!(ar.group, gvk.group, "unexpected discovered group");
        assert_eq!(ar.version, gvk.version, "unexcepted discovered ver");
        assert_eq!(ar.kind, gvk.kind, "unexpected discovered kind");

        // check all non-excluded groups that are iterable
        let mut groups = discovery.groups_alphabetical().into_iter();
        let firstgroup = groups.next().unwrap();
        assert_eq!(firstgroup.name(), ApiGroup::CORE_GROUP, "core not first");
        for group in groups {
            for (ar, caps) in group.recommended_resources() {
                if !caps.supports_operation(verbs::LIST) {
                    continue;
                }
                let api: Api<DynamicObject> = if caps.scope == Scope::Namespaced {
                    Api::default_namespaced_with(client.clone(), &ar)
                } else {
                    Api::all_with(client.clone(), &ar)
                };
                api.list(&Default::default()).await?;
            }
        }

        // cleanup
        crds.delete("testcrs.kube.rs", &DeleteParams::default()).await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "needs cluster (will create await a pod)"]
    #[cfg(feature = "runtime")]
    async fn pod_can_await_conditions() -> Result<(), Box<dyn std::error::Error>> {
        use crate::{
            api::{DeleteParams, PostParams},
            runtime::wait::{await_condition, conditions, delete::delete_and_finalize, Condition},
            Api, Client,
        };
        use k8s_openapi::api::core::v1::Pod;
        use std::time::Duration;
        use tokio::time::timeout;

        let client = Client::try_default().await?;
        let pods: Api<Pod> = Api::default_namespaced(client);

        // create busybox pod that's alive for at most 20s
        let data: Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "busybox-kube4",
                "labels": { "app": "kube-rs-test" },
            },
            "spec": {
                "terminationGracePeriodSeconds": 1,
                "restartPolicy": "Never",
                "containers": [{
                  "name": "busybox",
                  "image": "busybox:1.34.1",
                  "command": ["sh", "-c", "sleep 20"],
                }],
            }
        }))?;

        let pp = PostParams::default();
        assert_eq!(
            data.name_unchecked(),
            pods.create(&pp, &data).await?.name_unchecked()
        );

        // Watch it phase for a few seconds
        let is_running = await_condition(pods.clone(), "busybox-kube4", conditions::is_pod_running());
        let _ = timeout(Duration::from_secs(15), is_running).await?;

        // Verify we can get it
        let pod = pods.get("busybox-kube4").await?;
        assert_eq!(pod.spec.as_ref().unwrap().containers[0].name, "busybox");

        // Wait for a more complicated condition: ContainersReady AND Initialized
        // TODO: remove these once we can write these functions generically
        fn is_each_container_ready() -> impl Condition<Pod> {
            |obj: Option<&Pod>| {
                if let Some(o) = obj {
                    if let Some(s) = &o.status {
                        if let Some(conds) = &s.conditions {
                            if let Some(pcond) = conds.iter().find(|c| c.type_ == "ContainersReady") {
                                return pcond.status == "True";
                            }
                        }
                    }
                }
                false
            }
        }
        let is_fully_ready = await_condition(
            pods.clone(),
            "busybox-kube4",
            conditions::is_pod_running().and(is_each_container_ready()),
        );
        let _ = timeout(Duration::from_secs(10), is_fully_ready).await?;

        // Delete it - and wait for deletion to complete
        let dp = DeleteParams::default();
        delete_and_finalize(pods.clone(), "busybox-kube4", &dp).await?;

        // verify it is properly gone
        assert!(pods.get("busybox-kube4").await.is_err());

        Ok(())
    }

    #[tokio::test]
    #[ignore = "needs cluster (lists cms)"]
    async fn api_get_opt_handles_404() -> Result<(), Box<dyn std::error::Error>> {
        let client = Client::try_default().await?;
        let api = Api::<ConfigMap>::default_namespaced(client);
        assert_eq!(
            api.get_opt("this-cm-does-not-exist-ajklisdhfqkljwhreq").await?,
            None
        );
        Ok(())
    }
}
