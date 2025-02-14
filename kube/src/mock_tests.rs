use crate::{
    runtime::{
        watcher::{watcher, Config},
        WatchStreamExt,
    },
    Api, Client,
};
use anyhow::Result;
use futures::{poll, StreamExt, TryStreamExt};
use http::{Request, Response};
use kube_client::client::Body;
use kube_derive::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(group = "kube.rs", version = "v1", kind = "Hack")]
#[kube(crates(kube_core = "crate::core"))] // for dev-dep test structure
struct HackSpec {
    num: u32,
}
impl Hack {
    fn test(num: u32) -> Self {
        Hack::new("h{num}", HackSpec { num })
    }
}

#[tokio::test]
async fn watchers_respect_pagination_limits() {
    let (client, fakeserver) = testcontext();
    // NB: page scenario which responds to 3 paginated list calls with 3 object (one per page).
    // This ensures the watcher internal paging mechanism is not bypassed
    // and that each page is actually drained before starting the long watch.
    let mocksrv = fakeserver.run(Scenario::PaginatedList);

    let api: Api<Hack> = Api::all(client);
    let cfg = Config::default().page_size(1);
    let mut stream = watcher(api, cfg).applied_objects().boxed();
    let first: Hack = stream.try_next().await.unwrap().unwrap();
    assert_eq!(first.spec.num, 1);
    let second: Hack = stream.try_next().await.unwrap().unwrap();
    assert_eq!(second.spec.num, 2);
    let third: Hack = stream.try_next().await.unwrap().unwrap();
    assert_eq!(third.spec.num, 3);
    assert!(poll!(stream.next()).is_pending());
    timeout_after_1s(mocksrv).await;
}

// ------------------------------------------------------------------------
// mock test setup cruft
// ------------------------------------------------------------------------

// We wrap tower_test::mock::Handle
type ApiServerHandle = tower_test::mock::Handle<Request<Body>, Response<Body>>;
struct ApiServerVerifier(ApiServerHandle);

async fn timeout_after_1s(handle: tokio::task::JoinHandle<()>) {
    tokio::time::timeout(std::time::Duration::from_secs(1), handle)
        .await
        .expect("timeout on mock apiserver")
        .expect("scenario succeeded")
}

/// Scenarios we test for in ApiServerVerifier above
enum Scenario {
    PaginatedList,
    #[allow(dead_code)] // remove when/if we start doing better mock tests that use this
    RadioSilence,
}

impl ApiServerVerifier {
    /// Tests only get to run specific scenarios that has matching handlers
    ///
    /// NB: If the test is cauysing more calls than we are handling in the scenario,
    /// you then typically see a `KubeError(Service(Closed(())))` from the test.
    ///
    /// You should await the `JoinHandle` (with a timeout) from this function to ensure that the
    /// scenario runs to completion (i.e. all expected calls were responded to),
    /// using the timeout to catch missing api calls to Kubernetes.
    fn run(self, scenario: Scenario) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            // moving self => one scenario per test
            match scenario {
                Scenario::PaginatedList => self.handle_paged_lists().await,
                Scenario::RadioSilence => Ok(self),
            }
            .expect("scenario completed without errors");
        })
    }

    // chainable scenario handlers

    async fn handle_paged_lists(mut self) -> Result<Self> {
        {
            let (request, send) = self.0.next_request().await.expect("service not called 1");
            // We expect a json patch to the specified document adding our finalizer
            assert_eq!(request.method(), http::Method::GET);
            let req_uri = request.uri().to_string();
            assert!(req_uri.contains("limit="));
            assert!(!req_uri.contains("continue=")); // first list has no continue

            let respdata = json!({
                "kind": "HackList",
                "apiVersion": "kube.rs/v1",
                "metadata": {
                  "continue": "first",
                },
                "items": [Hack::test(1)]
            });
            let response = serde_json::to_vec(&respdata).unwrap(); // respond as the apiserver would have
            send.send_response(Response::builder().body(Body::from(response)).unwrap());
        }
        {
            // we expect another list GET because we included a continue token
            let (request, send) = self.0.next_request().await.expect("service not called 2");
            assert_eq!(request.method(), http::Method::GET);
            let req_uri = request.uri().to_string();
            assert!(req_uri.contains("&continue=first"));
            let respdata = json!({
                "kind": "HackList",
                "apiVersion": "kube.rs/v1",
                "metadata": {
                    "continue": "second",
                    "resourceVersion": "2"
                },
                "items": [Hack::test(2)]
            });
            let response = serde_json::to_vec(&respdata).unwrap(); // respond as the apiserver would have
            send.send_response(Response::builder().body(Body::from(response)).unwrap());
        }
        {
            // we expect a final list GET because we included a continue token
            let (request, send) = self.0.next_request().await.expect("service not called 3");
            assert_eq!(request.method(), http::Method::GET);
            let req_uri = request.uri().to_string();
            assert!(req_uri.contains("&continue=second"));
            let respdata = json!({
                "kind": "HackList",
                "apiVersion": "kube.rs/v1",
                "metadata": {
                    "continue": "",
                    "resourceVersion": "2"
                },
                "items": [Hack::test(3)]
            });
            let response = serde_json::to_vec(&respdata).unwrap(); // respond as the apiserver would have
            send.send_response(Response::builder().body(Body::from(response)).unwrap());
        }
        Ok(self)
    }
}

// Create a test context with a mocked kube client
fn testcontext() -> (Client, ApiServerVerifier) {
    let (mock_service, handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let mock_client = Client::new(mock_service, "default", None);
    (mock_client, ApiServerVerifier(handle))
}
