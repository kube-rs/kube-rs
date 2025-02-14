//! Crate for interacting with the Kubernetes API
//!
//! This crate includes the tools for manipulating Kubernetes resources as
//! well as keeping track of those resources as they change over time
//!
//! # Example
//!
//! The following example will create a [`Pod`](k8s_openapi::api::core::v1::Pod)
//! and then watch for it to become available using a manual [`Api::watch`] call.
//!
//! ```rust,no_run
//! use futures::{StreamExt, TryStreamExt};
//! use kube_client::api::{Api, ResourceExt, ListParams, PatchParams, Patch};
//! use kube_client::Client;
//! use k8s_openapi::api::core::v1::Pod;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Read the environment to find config for kube client.
//!     // Note that this tries an in-cluster configuration first,
//!     // then falls back on a kubeconfig file.
//!     let client = Client::try_default().await?;
//!
//!     // Interact with pods in the configured namespace with the typed interface from k8s-openapi
//!     let pods: Api<Pod> = Api::default_namespaced(client);
//!
//!     // Create a Pod (cheating here with json, but it has to validate against the type):
//!     let patch: Pod = serde_json::from_value(serde_json::json!({
//!         "apiVersion": "v1",
//!         "kind": "Pod",
//!         "metadata": {
//!             "name": "my-pod"
//!         },
//!         "spec": {
//!             "containers": [
//!                 {
//!                     "name": "my-container",
//!                     "image": "myregistry.azurecr.io/hello-world:v1",
//!                 },
//!             ],
//!         }
//!     }))?;
//!
//!     // Apply the Pod via server-side apply
//!     let params = PatchParams::apply("myapp");
//!     let result = pods.patch("my-pod", &params, &Patch::Apply(&patch)).await?;
//!
//!     // List pods in the configured namespace
//!     for p in pods.list(&ListParams::default()).await? {
//!         println!("found pod {}", p.name_any());
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! For more details, see:
//!
//! - [`Client`](crate::client) for the extensible Kubernetes client
//! - [`Config`](crate::config) for the Kubernetes config abstraction
//! - [`Api`](crate::Api) for the generic api methods available on Kubernetes resources
//! - [k8s-openapi](https://docs.rs/k8s-openapi) for how to create typed kubernetes objects directly
#![cfg_attr(docsrs, feature(doc_cfg))]
// Nightly clippy (0.1.64) considers Drop a side effect, see https://github.com/rust-lang/rust-clippy/issues/9608
#![allow(clippy::unnecessary_lazy_evaluations)]

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
    pub mod api;
    pub mod discovery;
    pub mod client;

    #[doc(inline)]
    pub use api::Api;
    #[doc(inline)]
    pub use client::Client;
    #[doc(inline)]
    pub use discovery::Discovery;
}

cfg_config! {
    pub mod config;
    #[doc(inline)]
    pub use config::Config;
}

cfg_error! {
    pub mod error;
    #[doc(inline)] pub use error::Error;
    /// Convient alias for `Result<T, Error>`
    pub type Result<T, E = Error> = std::result::Result<T, E>;
}

pub use crate::core::{CustomResourceExt, Resource, ResourceExt};
/// Re-exports from kube_core
pub use kube_core as core;

// Tests that require a cluster and the complete feature set
// Can be run with `cargo test -p kube-client --lib features=rustls-tls,ws -- --ignored`
#[cfg(all(feature = "client", feature = "config"))]
#[cfg(test)]
#[allow(unused_imports)] // varying test imports depending on feature
mod test {
    use crate::{
        api::{AttachParams, AttachedProcess},
        client::ConfigExt,
        Api, Client, Config, ResourceExt,
    };
    use futures::{AsyncBufRead, AsyncBufReadExt, StreamExt, TryStreamExt};
    use hyper::Uri;
    use k8s_openapi::api::core::v1::{EphemeralContainer, Pod, PodSpec};
    use kube_core::{
        params::{DeleteParams, Patch, PatchParams, PostParams, WatchParams},
        response::StatusSummary,
    };
    use serde_json::json;
    use tower::ServiceBuilder;

    // hard disabled test atm due to k3d rustls issues: https://github.com/kube-rs/kube/issues?q=is%3Aopen+is%3Aissue+label%3Arustls
    #[allow(dead_code)]
    // #[tokio::test]
    #[ignore = "needs cluster (lists pods)"]
    #[cfg(feature = "rustls-tls")]
    async fn custom_client_rustls_configuration() -> Result<(), Box<dyn std::error::Error>> {
        use hyper_util::rt::TokioExecutor;

        let config = Config::infer().await?;
        let https = config.rustls_https_connector(None)?;
        let service = ServiceBuilder::new()
            .layer(config.base_uri_layer())
            .service(hyper_util::client::legacy::Client::builder(TokioExecutor::new()).build(https));
        let client = Client::new(service, config.default_namespace, None);
        let pods: Api<Pod> = Api::default_namespaced(client);
        pods.list(&Default::default()).await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "needs cluster (lists pods)"]
    #[cfg(feature = "openssl-tls")]
    async fn custom_client_openssl_tls_configuration() -> Result<(), Box<dyn std::error::Error>> {
        use hyper_util::rt::TokioExecutor;

        let config = Config::infer().await?;
        let https = config.openssl_https_connector()?;
        let service = ServiceBuilder::new()
            .layer(config.base_uri_layer())
            .service(hyper_util::client::legacy::Client::builder(TokioExecutor::new()).build(https));
        let client = Client::new(service, config.default_namespace);
        let pods: Api<Pod> = Api::default_namespaced(client);
        pods.list(&Default::default()).await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "needs cluster (lists api resources)"]
    #[cfg(feature = "client")]
    async fn group_discovery_oneshot() -> Result<(), Box<dyn std::error::Error>> {
        use crate::{core::DynamicObject, discovery};
        let client = Client::try_default().await?;
        let apigroup = discovery::group(&client, "apiregistration.k8s.io").await?;
        let (ar, _caps) = apigroup.recommended_kind("APIService").unwrap();
        let api: Api<DynamicObject> = Api::all_with(client.clone(), &ar);
        api.list(&Default::default()).await?;

        Ok(())
    }

    #[tokio::test]
    #[ignore = "needs cluster (will create and edit a pod)"]
    async fn pod_can_use_core_apis() -> Result<(), Box<dyn std::error::Error>> {
        use kube::api::{DeleteParams, ListParams, Patch, PatchParams, PostParams, WatchEvent};

        let client = Client::try_default().await?;
        let pods: Api<Pod> = Api::default_namespaced(client);

        // create busybox pod that's alive for at most 30s
        let p: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "busybox-kube1",
                "labels": { "app": "kube-rs-test" },
            },
            "spec": {
                "terminationGracePeriodSeconds": 1,
                "restartPolicy": "Never",
                "containers": [{
                  "name": "busybox",
                  "image": "busybox:1.34.1",
                  "command": ["sh", "-c", "sleep 30"],
                }],
            }
        }))?;

        let pp = PostParams::default();
        match pods.create(&pp, &p).await {
            Ok(o) => assert_eq!(p.name_unchecked(), o.name_unchecked()),
            Err(crate::Error::Api(ae)) => assert_eq!(ae.code, 409), // if we failed to clean-up
            Err(e) => return Err(e.into()),                         // any other case if a failure
        }

        // Manual watch-api for it to become ready
        // NB: don't do this; using conditions (see pod_api example) is easier and less error prone
        let wp = WatchParams::default()
            .fields(&format!("metadata.name={}", "busybox-kube1"))
            .timeout(15);
        let mut stream = pods.watch(&wp, "0").await?.boxed();
        while let Some(ev) = stream.try_next().await? {
            // can debug format watch event
            let _ = format!("we: {ev:?}");
            match ev {
                WatchEvent::Modified(o) => {
                    let s = o.status.as_ref().expect("status exists on pod");
                    let phase = s.phase.clone().unwrap_or_default();
                    if phase == "Running" {
                        break;
                    }
                }
                WatchEvent::Error(e) => panic!("watch error: {e}"),
                _ => {}
            }
        }

        // Verify we can get it
        let mut pod = pods.get("busybox-kube1").await?;
        assert_eq!(p.spec.as_ref().unwrap().containers[0].name, "busybox");

        // verify replace with explicit resource version
        // NB: don't do this; use server side apply
        {
            assert!(pod.resource_version().is_some());
            pod.spec.as_mut().unwrap().active_deadline_seconds = Some(5);

            let pp = PostParams::default();
            let patched_pod = pods.replace("busybox-kube1", &pp, &pod).await?;
            assert_eq!(patched_pod.spec.unwrap().active_deadline_seconds, Some(5));
        }

        // Delete it
        let dp = DeleteParams::default();
        pods.delete("busybox-kube1", &dp).await?.map_left(|pdel| {
            assert_eq!(pdel.name_unchecked(), "busybox-kube1");
        });

        Ok(())
    }

    #[tokio::test]
    #[ignore = "needs cluster (will create and attach to a pod)"]
    #[cfg(feature = "ws")]
    async fn pod_can_exec_and_write_to_stdin() -> Result<(), Box<dyn std::error::Error>> {
        use crate::api::{DeleteParams, ListParams, Patch, PatchParams, WatchEvent};

        let client = Client::try_default().await?;
        let pods: Api<Pod> = Api::default_namespaced(client);

        // create busybox pod that's alive for at most 30s
        let p: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "busybox-kube2",
                "labels": { "app": "kube-rs-test" },
            },
            "spec": {
                "terminationGracePeriodSeconds": 1,
                "restartPolicy": "Never",
                "containers": [{
                  "name": "busybox",
                  "image": "busybox:1.34.1",
                  "command": ["sh", "-c", "sleep 30"],
                }],
            }
        }))?;

        match pods.create(&Default::default(), &p).await {
            Ok(o) => assert_eq!(p.name_unchecked(), o.name_unchecked()),
            Err(crate::Error::Api(ae)) => assert_eq!(ae.code, 409), // if we failed to clean-up
            Err(e) => return Err(e.into()),                         // any other case if a failure
        }

        // Manual watch-api for it to become ready
        // NB: don't do this; using conditions (see pod_api example) is easier and less error prone
        let wp = WatchParams::default()
            .fields(&format!("metadata.name={}", "busybox-kube2"))
            .timeout(15);
        let mut stream = pods.watch(&wp, "0").await?.boxed();
        while let Some(ev) = stream.try_next().await? {
            match ev {
                WatchEvent::Modified(o) => {
                    let s = o.status.as_ref().expect("status exists on pod");
                    let phase = s.phase.clone().unwrap_or_default();
                    if phase == "Running" {
                        break;
                    }
                }
                WatchEvent::Error(e) => panic!("watch error: {e}"),
                _ => {}
            }
        }

        // Verify exec works and we can get the output
        {
            let mut attached = pods
                .exec(
                    "busybox-kube2",
                    vec!["sh", "-c", "for i in $(seq 1 3); do echo $i; done"],
                    &AttachParams::default().stderr(false),
                )
                .await?;
            let stdout = tokio_util::io::ReaderStream::new(attached.stdout().unwrap());
            let out = stdout
                .filter_map(|r| async { r.ok().and_then(|v| String::from_utf8(v.to_vec()).ok()) })
                .collect::<Vec<_>>()
                .await
                .join("");
            attached.join().await.unwrap();
            assert_eq!(out.lines().count(), 3);
            assert_eq!(out, "1\n2\n3\n");
        }

        // Verify we can write to Stdin
        {
            use tokio::io::AsyncWriteExt;
            let mut attached = pods
                .exec(
                    "busybox-kube2",
                    vec!["sh"],
                    &AttachParams::default().stdin(true).stderr(false),
                )
                .await?;
            let mut stdin_writer = attached.stdin().unwrap();
            let mut stdout_stream = tokio_util::io::ReaderStream::new(attached.stdout().unwrap());
            let next_stdout = stdout_stream.next();
            stdin_writer.write_all(b"echo test string 1\n").await?;
            let stdout = String::from_utf8(next_stdout.await.unwrap().unwrap().to_vec()).unwrap();
            println!("{stdout}");
            assert_eq!(stdout, "test string 1\n");

            // AttachedProcess resolves with status object.
            // Send `exit 1` to get a failure status.
            stdin_writer.write_all(b"exit 1\n").await?;
            let status = attached.take_status().unwrap();
            if let Some(status) = status.await {
                println!("{status:?}");
                assert_eq!(status.status, Some("Failure".to_owned()));
                assert_eq!(status.reason, Some("NonZeroExitCode".to_owned()));
            }
        }

        // Delete it
        let dp = DeleteParams::default();
        pods.delete("busybox-kube2", &dp).await?.map_left(|pdel| {
            assert_eq!(pdel.name_unchecked(), "busybox-kube2");
        });

        Ok(())
    }

    #[tokio::test]
    #[ignore = "needs cluster (will create and tail logs from a pod)"]
    async fn can_get_pod_logs_and_evict() -> Result<(), Box<dyn std::error::Error>> {
        use crate::{
            api::{DeleteParams, EvictParams, ListParams, Patch, PatchParams, WatchEvent},
            core::subresource::LogParams,
        };

        let client = Client::try_default().await?;
        let pods: Api<Pod> = Api::default_namespaced(client);

        // create busybox pod that's alive for at most 30s
        let p: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "busybox-kube3",
                "labels": { "app": "kube-rs-test" },
            },
            "spec": {
                "terminationGracePeriodSeconds": 1,
                "restartPolicy": "Never",
                "containers": [{
                  "name": "busybox",
                  "image": "busybox:1.34.1",
                  "command": ["sh", "-c", "for i in $(seq 1 5); do echo kube $i; sleep 0.1; done"],
                }],
            }
        }))?;

        match pods.create(&Default::default(), &p).await {
            Ok(o) => assert_eq!(p.name_unchecked(), o.name_unchecked()),
            Err(crate::Error::Api(ae)) => assert_eq!(ae.code, 409), // if we failed to clean-up
            Err(e) => return Err(e.into()),                         // any other case if a failure
        }

        // Manual watch-api for it to become ready
        // NB: don't do this; using conditions (see pod_api example) is easier and less error prone
        let wp = WatchParams::default()
            .fields(&format!("metadata.name={}", "busybox-kube3"))
            .timeout(15);
        let mut stream = pods.watch(&wp, "0").await?.boxed();
        while let Some(ev) = stream.try_next().await? {
            match ev {
                WatchEvent::Modified(o) => {
                    let s = o.status.as_ref().expect("status exists on pod");
                    let phase = s.phase.clone().unwrap_or_default();
                    if phase == "Running" {
                        break;
                    }
                }
                WatchEvent::Error(e) => panic!("watch error: {e}"),
                _ => {}
            }
        }

        // Get current list of logs
        let lp = LogParams {
            follow: true,
            ..LogParams::default()
        };
        let mut logs_stream = pods.log_stream("busybox-kube3", &lp).await?.lines();

        // wait for container to finish
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let all_logs = pods.logs("busybox-kube3", &Default::default()).await?;
        assert_eq!(all_logs, "kube 1\nkube 2\nkube 3\nkube 4\nkube 5\n");

        // individual logs may or may not buffer
        let mut output = vec![];
        while let Some(line) = logs_stream.try_next().await? {
            output.push(line);
        }
        assert_eq!(output, vec!["kube 1", "kube 2", "kube 3", "kube 4", "kube 5"]);

        // evict the pod
        let ep = EvictParams::default();
        let eres = pods.evict("busybox-kube3", &ep).await?;
        assert_eq!(eres.code, 201); // created
        assert!(eres.is_success());

        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a cluster"]
    async fn can_operate_on_pod_metadata() -> Result<(), Box<dyn std::error::Error>> {
        use crate::{
            api::{DeleteParams, EvictParams, ListParams, Patch, PatchParams, WatchEvent},
            core::subresource::LogParams,
        };
        use kube_core::{ObjectList, ObjectMeta, PartialObjectMeta, PartialObjectMetaExt};

        let client = Client::try_default().await?;
        let pods: Api<Pod> = Api::default_namespaced(client);

        // create busybox pod that's alive for at most 30s
        let p: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "busybox-kube-meta",
                "labels": { "app": "kube-rs-test" },
            },
            "spec": {
                "terminationGracePeriodSeconds": 1,
                "restartPolicy": "Never",
                "containers": [{
                  "name": "busybox",
                  "image": "busybox:1.34.1",
                  "command": ["sh", "-c", "sleep 30s"],
                }],
            }
        }))?;

        match pods.create(&Default::default(), &p).await {
            Ok(o) => assert_eq!(p.name_unchecked(), o.name_unchecked()),
            Err(crate::Error::Api(ae)) => assert_eq!(ae.code, 409), // if we failed to clean-up
            Err(e) => return Err(e.into()),                         // any other case if a failure
        }

        // Test we can get a pod as a PartialObjectMeta and convert to
        // ObjectMeta
        let pod_metadata = pods.get_metadata("busybox-kube-meta").await?;
        assert_eq!("busybox-kube-meta", pod_metadata.name_any());
        assert_eq!(
            Some((&"app".to_string(), &"kube-rs-test".to_string())),
            pod_metadata.labels().get_key_value("app")
        );

        // Test we can get a list of PartialObjectMeta for pods
        let p_list = pods.list_metadata(&ListParams::default()).await?;

        // Find only pod we are concerned with in this test and fail eagerly if
        // name doesn't exist
        let pod_metadata = p_list
            .items
            .into_iter()
            .find(|p| p.name_any() == "busybox-kube-meta")
            .unwrap();
        assert_eq!(
            pod_metadata.labels().get("app"),
            Some(&"kube-rs-test".to_string())
        );

        // Attempt to patch pod metadata
        let patch = ObjectMeta {
            annotations: Some([("test".to_string(), "123".to_string())].into()),
            ..Default::default()
        }
        .into_request_partial::<Pod>();

        let patchparams = PatchParams::default();
        let p_patched = pods
            .patch_metadata("busybox-kube-meta", &patchparams, &Patch::Merge(&patch))
            .await?;
        assert_eq!(p_patched.annotations().get("test"), Some(&"123".to_string()));
        assert_eq!(p_patched.types.as_ref().unwrap().kind, "PartialObjectMetadata");
        assert_eq!(p_patched.types.as_ref().unwrap().api_version, "meta.k8s.io/v1");

        // Clean-up
        let dp = DeleteParams::default();
        pods.delete("busybox-kube-meta", &dp).await?.map_left(|pdel| {
            assert_eq!(pdel.name_any(), "busybox-kube-meta");
        });

        Ok(())
    }
    #[tokio::test]
    #[ignore = "needs cluster (will create a CertificateSigningRequest)"]
    async fn csr_can_be_approved() -> Result<(), Box<dyn std::error::Error>> {
        use crate::api::PostParams;
        use k8s_openapi::api::certificates::v1::{
            CertificateSigningRequest, CertificateSigningRequestCondition, CertificateSigningRequestStatus,
        };

        let csr_name = "fake";
        let dummy_csr: CertificateSigningRequest = serde_json::from_value(json!({
            "apiVersion": "certificates.k8s.io/v1",
            "kind": "CertificateSigningRequest",
            "metadata": { "name": csr_name },
            "spec": {
                "request": "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURSBSRVFVRVNULS0tLS0KTUlJQ1ZqQ0NBVDRDQVFBd0VURVBNQTBHQTFVRUF3d0dZVzVuWld4aE1JSUJJakFOQmdrcWhraUc5dzBCQVFFRgpBQU9DQVE4QU1JSUJDZ0tDQVFFQTByczhJTHRHdTYxakx2dHhWTTJSVlRWMDNHWlJTWWw0dWluVWo4RElaWjBOCnR2MUZtRVFSd3VoaUZsOFEzcWl0Qm0wMUFSMkNJVXBGd2ZzSjZ4MXF3ckJzVkhZbGlBNVhwRVpZM3ExcGswSDQKM3Z3aGJlK1o2MVNrVHF5SVBYUUwrTWM5T1Nsbm0xb0R2N0NtSkZNMUlMRVI3QTVGZnZKOEdFRjJ6dHBoaUlFMwpub1dtdHNZb3JuT2wzc2lHQ2ZGZzR4Zmd4eW8ybmlneFNVekl1bXNnVm9PM2ttT0x1RVF6cXpkakJ3TFJXbWlECklmMXBMWnoyalVnald4UkhCM1gyWnVVV1d1T09PZnpXM01LaE8ybHEvZi9DdS8wYk83c0x0MCt3U2ZMSU91TFcKcW90blZtRmxMMytqTy82WDNDKzBERHk5aUtwbXJjVDBnWGZLemE1dHJRSURBUUFCb0FBd0RRWUpLb1pJaHZjTgpBUUVMQlFBRGdnRUJBR05WdmVIOGR4ZzNvK21VeVRkbmFjVmQ1N24zSkExdnZEU1JWREkyQTZ1eXN3ZFp1L1BVCkkwZXpZWFV0RVNnSk1IRmQycVVNMjNuNVJsSXJ3R0xuUXFISUh5VStWWHhsdnZsRnpNOVpEWllSTmU3QlJvYXgKQVlEdUI5STZXT3FYbkFvczFqRmxNUG5NbFpqdU5kSGxpT1BjTU1oNndLaTZzZFhpVStHYTJ2RUVLY01jSVUyRgpvU2djUWdMYTk0aEpacGk3ZnNMdm1OQUxoT045UHdNMGM1dVJVejV4T0dGMUtCbWRSeEgvbUNOS2JKYjFRQm1HCkkwYitEUEdaTktXTU0xMzhIQXdoV0tkNjVoVHdYOWl4V3ZHMkh4TG1WQzg0L1BHT0tWQW9FNkpsYWFHdTlQVmkKdjlOSjVaZlZrcXdCd0hKbzZXdk9xVlA3SVFjZmg3d0drWm89Ci0tLS0tRU5EIENFUlRJRklDQVRFIFJFUVVFU1QtLS0tLQo=",
                "signerName": "kubernetes.io/kube-apiserver-client",
                "expirationSeconds": 86400,
                "usages": ["client auth"]
            }
        }))?;

        let client = Client::try_default().await?;
        let csr: Api<CertificateSigningRequest> = Api::all(client.clone());
        assert!(csr.create(&PostParams::default(), &dummy_csr).await.is_ok());

        // Patch the approval and approve the CSR
        let approval_type = "ApprovedFake";
        let csr_status: CertificateSigningRequestStatus = CertificateSigningRequestStatus {
            certificate: None,
            conditions: Some(vec![CertificateSigningRequestCondition {
                type_: approval_type.to_string(),
                last_update_time: None,
                last_transition_time: None,
                message: Some(format!("{} {}", approval_type, "by kube-rs client")),
                reason: Some("kube-rsClient".to_string()),
                status: "True".to_string(),
            }]),
        };
        let csr_status_patch = Patch::Merge(serde_json::json!({ "status": csr_status }));
        let _ = csr
            .patch_approval(csr_name, &Default::default(), &csr_status_patch)
            .await?;
        let csr_after_approval = csr.get_approval(csr_name).await?;

        assert_eq!(
            csr_after_approval
                .status
                .as_ref()
                .unwrap()
                .conditions
                .as_ref()
                .unwrap()[0]
                .type_,
            approval_type.to_string()
        );
        csr.delete(csr_name, &DeleteParams::default()).await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "needs cluster for ephemeral containers operations"]
    async fn can_operate_on_ephemeral_containers() -> Result<(), Box<dyn std::error::Error>> {
        let client = Client::try_default().await?;

        // Ephemeral containers were stabilized in Kubernetes v1.25.
        // This test therefore exits early if the current cluster version is older than v1.25.
        let api_version = client.apiserver_version().await?;
        if api_version.major.parse::<i32>()? < 1 || api_version.minor.parse::<i32>()? < 25 {
            return Ok(());
        }

        let pod: Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "ephemeral-container-test",
                "labels": { "app": "kube-rs-test" },
            },
            "spec": {
                "restartPolicy": "Never",
                "containers": [{
                  "name": "busybox",
                  "image": "busybox:1.34.1",
                  "command": ["sh", "-c", "sleep 2"],
                }],
            }
        }))?;

        let pod_name = pod.name_any();
        let pods = Api::<Pod>::default_namespaced(client);

        // If cleanup failed and a pod already exists, we attempt to remove it
        // before proceeding. This is important as ephemeral containers can't
        // be removed from a Pod's spec. Therefore this test must start with a fresh
        // Pod every time.
        let _ = pods
            .delete(&pod.name_any(), &DeleteParams::default())
            .await
            .map(|v| v.map_left(|pdel| assert_eq!(pdel.name_any(), pod.name_any())));

        // Ephemeral containes can only be applied to a running pod, so one must
        // be created before any operations are tested.
        match pods.create(&Default::default(), &pod).await {
            Ok(o) => assert_eq!(pod.name_unchecked(), o.name_unchecked()),
            Err(e) => return Err(e.into()), // any other case if a failure
        }

        let current_ephemeral_containers = pods
            .get_ephemeral_containers(&pod.name_any())
            .await?
            .spec
            .unwrap()
            .ephemeral_containers;

        // We expect no ephemeral containers initially, get_ephemeral_containers should
        // reflect that.
        assert_eq!(current_ephemeral_containers, None);

        let mut busybox_eph: EphemeralContainer = serde_json::from_value(json!(
            {
                "name": "myephemeralcontainer1",
                "image": "busybox:1.34.1",
                "command": ["sh", "-c", "sleep 2"],
            }
        ))?;

        // Attempt to replace ephemeral containers.

        let patch: Pod = serde_json::from_value(json!({
            "metadata": { "name": pod_name },
            "spec":{ "ephemeralContainers": [ busybox_eph ] }
        }))?;

        let current_containers = pods
            .replace_ephemeral_containers(&pod_name, &PostParams::default(), &patch)
            .await?
            .spec
            .unwrap()
            .ephemeral_containers
            .expect("could find ephemeral container");

        // Note that we can't compare the whole ephemeral containers object, as some fields
        // are set by the cluster. We therefore compare the fields specified in the patch.
        assert_eq!(current_containers.len(), 1);
        assert_eq!(current_containers[0].name, busybox_eph.name);
        assert_eq!(current_containers[0].image, busybox_eph.image);
        assert_eq!(current_containers[0].command, busybox_eph.command);

        // Attempt to patch ephemeral containers.

        // The new ephemeral container will have different values from the
        // first to ensure we can test for its presence.
        busybox_eph = serde_json::from_value(json!(
            {
                "name": "myephemeralcontainer2",
                "image": "busybox:1.35.0",
                "command": ["sh", "-c", "sleep 1"],
            }
        ))?;

        let patch: Pod =
            serde_json::from_value(json!({ "spec": { "ephemeralContainers": [ busybox_eph ] }}))?;

        let current_containers = pods
            .patch_ephemeral_containers(&pod_name, &PatchParams::default(), &Patch::Strategic(patch))
            .await?
            .spec
            .unwrap()
            .ephemeral_containers
            .expect("could find ephemeral container");

        // There should only be 2 ephemeral containers at this point,
        // one from each patch
        assert_eq!(current_containers.len(), 2);

        let new_container = current_containers
            .iter()
            .find(|c| c.name == busybox_eph.name)
            .expect("could find myephemeralcontainer2");

        // Note that we can't compare the whole ephemeral container object, as some fields
        // get set in the cluster. We therefore compare the fields specified in the patch.
        assert_eq!(new_container.image, busybox_eph.image);
        assert_eq!(new_container.command, busybox_eph.command);

        // Attempt to get ephemeral containers.

        let expected_containers = current_containers;

        let current_containers = pods
            .get_ephemeral_containers(&pod.name_any())
            .await?
            .spec
            .unwrap()
            .ephemeral_containers
            .unwrap();

        assert_eq!(current_containers, expected_containers);

        pods.delete(&pod.name_any(), &DeleteParams::default())
            .await?
            .map_left(|pdel| {
                assert_eq!(pdel.name_any(), pod.name_any());
            });

        Ok(())
    }

    #[tokio::test]
    #[ignore = "needs kubelet debug methods"]
    #[cfg(feature = "kubelet-debug")]
    async fn pod_can_exec_and_write_to_stdin_from_node_proxy() -> Result<(), Box<dyn std::error::Error>> {
        use crate::{
            api::{DeleteParams, ListParams, Patch, PatchParams, WatchEvent},
            core::kubelet_debug::KubeletDebugParams,
        };

        let client = Client::try_default().await?;
        let pods: Api<Pod> = Api::default_namespaced(client);

        // create busybox pod that's alive for at most 30s
        let p: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "busybox-kube2",
                "labels": { "app": "kube-rs-test" },
            },
            "spec": {
                "terminationGracePeriodSeconds": 1,
                "restartPolicy": "Never",
                "containers": [{
                  "name": "busybox",
                  "image": "busybox:1.34.1",
                  "command": ["sh", "-c", "sleep 30"],
                }],
            }
        }))?;

        match pods.create(&Default::default(), &p).await {
            Ok(o) => assert_eq!(p.name_unchecked(), o.name_unchecked()),
            Err(crate::Error::Api(ae)) => assert_eq!(ae.code, 409), // if we failed to clean-up
            Err(e) => return Err(e.into()),                         // any other case if a failure
        }

        // Manual watch-api for it to become ready
        // NB: don't do this; using conditions (see pod_api example) is easier and less error prone
        let wp = WatchParams::default()
            .fields(&format!("metadata.name={}", "busybox-kube2"))
            .timeout(15);
        let mut stream = pods.watch(&wp, "0").await?.boxed();
        while let Some(ev) = stream.try_next().await? {
            match ev {
                WatchEvent::Modified(o) => {
                    let s = o.status.as_ref().expect("status exists on pod");
                    let phase = s.phase.clone().unwrap_or_default();
                    if phase == "Running" {
                        break;
                    }
                }
                WatchEvent::Error(e) => panic!("watch error: {e}"),
                _ => {}
            }
        }

        let mut config = Config::infer().await?;
        config.accept_invalid_certs = true;
        config.cluster_url = "https://localhost:10250".to_string().parse::<Uri>().unwrap();
        let kubelet_client: Client = config.try_into()?;

        // Verify exec works and we can get the output
        {
            let mut attached = kubelet_client
                .kubelet_node_exec(
                    &KubeletDebugParams {
                        name: "busybox-kube2",
                        namespace: "default",
                        ..Default::default()
                    },
                    "busybox",
                    vec!["sh", "-c", "for i in $(seq 1 3); do echo $i; done"],
                    &AttachParams::default().stderr(false),
                )
                .await?;
            let stdout = tokio_util::io::ReaderStream::new(attached.stdout().unwrap());
            let out = stdout
                .filter_map(|r| async { r.ok().and_then(|v| String::from_utf8(v.to_vec()).ok()) })
                .collect::<Vec<_>>()
                .await
                .join("");
            attached.join().await.unwrap();
            assert_eq!(out.lines().count(), 3);
            assert_eq!(out, "1\n2\n3\n");
        }

        // Verify we can write to Stdin
        {
            use tokio::io::AsyncWriteExt;
            let mut attached = kubelet_client
                .kubelet_node_exec(
                    &KubeletDebugParams {
                        name: "busybox-kube2",
                        namespace: "default",
                        ..Default::default()
                    },
                    "busybox",
                    vec!["sh"],
                    &AttachParams::default().stdin(true).stderr(false),
                )
                .await?;
            let mut stdin_writer = attached.stdin().unwrap();
            let mut stdout_stream = tokio_util::io::ReaderStream::new(attached.stdout().unwrap());
            let next_stdout = stdout_stream.next();
            stdin_writer.write_all(b"echo test string 1\n").await?;
            let stdout = String::from_utf8(next_stdout.await.unwrap().unwrap().to_vec()).unwrap();
            println!("{stdout}");
            assert_eq!(stdout, "test string 1\n");

            // AttachedProcess resolves with status object.
            // Send `exit 1` to get a failure status.
            stdin_writer.write_all(b"exit 1\n").await?;
            let status = attached.take_status().unwrap();
            if let Some(status) = status.await {
                println!("{status:?}");
                assert_eq!(status.status, Some("Failure".to_owned()));
                assert_eq!(status.reason, Some("NonZeroExitCode".to_owned()));
            }
        }

        // Delete it
        let dp = DeleteParams::default();
        pods.delete("busybox-kube2", &dp).await?.map_left(|pdel| {
            assert_eq!(pdel.name_unchecked(), "busybox-kube2");
        });

        Ok(())
    }
}
