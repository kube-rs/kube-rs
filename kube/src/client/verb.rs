//! Operations supported by kube

use std::{str::FromStr, time::Duration};

use futures::TryFuture;
use http::{Request, Response, Uri};
use hyper::Body;
use kube_core::{
    object::ObjectList,
    params::{self, ListParams},
    Resource, WatchEvent,
};
use serde::{de::DeserializeOwned, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};

use crate::client::{
    decoder::{DecodeSingle, DecodeStream},
    scope::{self, NativeScope},
};

#[derive(Snafu, Debug)]
#[allow(missing_docs)]
/// Failed to create a [`Request`] for a given [`Verb`]
pub enum Error {
    /// Verb tried to create invalid HTTP request
    #[snafu(display("verb created invalid http request: {}", source))]
    BuildRequestFailed { source: http::Error },
    /// Failed to serialize object
    #[snafu(display("failed to serialize object: {}", source))]
    SerializeFailed { source: serde_json::Error },
    // Object has no name
    #[snafu(display("object has no name"))]
    UnnamedObject,
}
type Result<T, E = Error> = std::result::Result<T, E>;

/// An action that Kube can take
pub trait Verb {
    /// Decodes the response given from the server
    /// Will typically be [`DecodeSingle`]
    type ResponseDecoder: TryFuture + From<Response<Body>>;

    /// Prepare a HTTP request that takes the action
    ///
    /// Should include request-specific options, but not global options (such as the base URI or authentication tokens)
    fn to_http_request(&self) -> Result<Request<Body>>;
}

/// Get a single object
pub struct Get<'a, Kind: Resource, Scope> {
    /// The name of the object
    pub name: &'a str,
    /// The scope that the object will be queried from
    pub scope: &'a Scope,
    /// The type of the object
    pub dyn_type: &'a Kind::DynamicType,
}
impl<'a, Kind: Resource + DeserializeOwned, Scope: NativeScope<Kind>> Verb for Get<'a, Kind, Scope> {
    type ResponseDecoder = DecodeSingle<Kind>;

    fn to_http_request(&self) -> Result<Request<Body>> {
        Request::get(format!(
            "{}/{}",
            Kind::url_path(&self.dyn_type, self.scope.namespace()),
            self.name
        ))
        .body(Body::empty())
        .context(BuildRequestFailed)
    }
}

/// List all objects of a resource type
pub struct List<'a, Kind: Resource, Scope> {
    /// The scope that the objects will be queried from
    pub scope: &'a Scope,
    /// The type of the objects
    pub dyn_type: &'a Kind::DynamicType,
    /// The query to filter the objects by
    pub query: &'a Query<'a>,

    /// Limit the number of results.
    ///
    /// If there are more results, the server will respond with a continue token which can be used to fetch another page
    /// of results. See the [Kubernetes API docs](https://kubernetes.io/docs/reference/using-api/api-concepts/#retrieving-large-results-sets-in-chunks)
    /// for pagination details.
    pub limit: Option<u32>,

    /// Fetch a second page of results.
    ///
    /// After listing results with a `limit`, a continue token can be used to fetch another page of results.
    pub continue_token: Option<&'a str>,
}
impl<'a, Kind: Resource + DeserializeOwned, Scope: scope::Scope> Verb for List<'a, Kind, Scope> {
    type ResponseDecoder = DecodeSingle<ObjectList<Kind>>;

    fn to_http_request(&self) -> Result<Request<Body>> {
        let mut url = format!("{}?", Kind::url_path(&self.dyn_type, self.scope.namespace()));
        let mut qp = form_urlencoded::Serializer::new(&mut url);
        self.query.populate_qp(&mut qp);
        if let Some(limit) = self.limit {
            qp.append_pair("limit", &limit.to_string());
        }
        if let Some(cont) = self.continue_token {
            qp.append_pair("continue", cont);
        }
        Request::get(url).body(Body::empty()).context(BuildRequestFailed)
    }
}
/// Common query parameters used to select multiple objects
#[derive(Default)]
pub struct Query<'a> {
    /// A selector to restrict the list of returned objects by their labels.
    ///
    /// Defaults to everything if `None`.
    pub label_selector: Option<&'a str>,
    /// A selector to restrict the list of returned objects by their fields.
    ///
    /// Defaults to everything if `None`.
    pub field_selector: Option<&'a str>,
}
impl<'a> Query<'a> {
    fn populate_qp(&self, qp: &mut form_urlencoded::Serializer<&mut String>) {
        if let Some(labels) = self.label_selector {
            qp.append_pair("labelSelector", labels);
        }
        if let Some(fields) = self.field_selector {
            qp.append_pair("fieldSelector", fields);
        }
    }

    pub(crate) fn from_list_params(lp: &'a ListParams) -> Self {
        Self {
            label_selector: lp.label_selector.as_deref(),
            field_selector: lp.field_selector.as_deref(),
        }
    }
}

/// Watch all objects of a resource type for modifications
pub struct Watch<'a, Kind: Resource, Scope> {
    /// The scope that the objects will be queried from
    pub scope: &'a Scope,
    /// The type of the objects
    pub dyn_type: &'a Kind::DynamicType,
    /// The query to filter the objects by
    pub query: &'a Query<'a>,
    /// The `resourceVersion` to report events newer than
    pub version: &'a str,
    /// Upper bound on how long the watch should be active for, rounded down to the nearest second
    pub timeout: Option<Duration>,
}
impl<'a, Kind: Resource, Scope: scope::Scope> Verb for Watch<'a, Kind, Scope> {
    type ResponseDecoder = DecodeStream<WatchEvent<Kind>>;

    fn to_http_request(&self) -> Result<Request<Body>> {
        let mut url = format!("{}?", Kind::url_path(&self.dyn_type, self.scope.namespace()),);
        let mut qp = form_urlencoded::Serializer::new(&mut url);
        qp.append_pair("watch", "1");
        qp.append_pair("resourceVersion", self.version);
        qp.append_pair("allowWatchBookmarks", "1");
        if let Some(timeout) = self.timeout {
            qp.append_pair("timeoutSeconds", &timeout.as_secs().to_string());
        }
        Request::get(url).body(Body::empty()).context(BuildRequestFailed)
    }
}

/// Create a new object
pub struct Create<'a, Kind: Resource, Scope> {
    /// The object to be created
    pub object: &'a Kind,
    /// The scope for the object to be created in
    pub scope: &'a Scope,
    /// The type of the object
    pub dyn_type: &'a Kind::DynamicType,
}
impl<'a, Kind: Resource + Serialize + DeserializeOwned, Scope: scope::Scope> Verb
    for Create<'a, Kind, Scope>
{
    type ResponseDecoder = DecodeSingle<Kind>;

    fn to_http_request(&self) -> Result<Request<Body>> {
        Request::post(format!(
            "{}/{}",
            Kind::url_path(&self.dyn_type, self.scope.namespace()),
            self.object.meta().name.as_ref().context(UnnamedObject)?
        ))
        .body(Body::from(
            serde_json::to_vec(self.object).context(SerializeFailed)?,
        ))
        .context(BuildRequestFailed)
    }
}

/// Delete a named object
pub struct Delete<'a, Kind: Resource, Scope> {
    /// The name of the object to be deleted
    pub name: &'a str,
    /// The scope for the object to be deleted from
    pub scope: &'a Scope,
    /// The type of the object
    pub dyn_type: &'a Kind::DynamicType,
}
impl<'a, Kind: Resource + DeserializeOwned, Scope: scope::Scope> Verb for Delete<'a, Kind, Scope> {
    type ResponseDecoder = DecodeSingle<Kind>;

    fn to_http_request(&self) -> Result<Request<Body>> {
        Request::delete(format!(
            "{}/{}",
            Kind::url_path(&self.dyn_type, self.scope.namespace()),
            self.name
        ))
        .body(Body::empty())
        .context(BuildRequestFailed)
    }
}

/// Delete objects matching a search query
pub struct DeleteCollection<'a, Kind: Resource, Scope> {
    /// The scope for the object to be deleted from
    pub scope: &'a Scope,
    /// The type of the objects
    pub dyn_type: &'a Kind::DynamicType,
}
impl<'a, Kind: Resource + DeserializeOwned, Scope: scope::Scope> Verb for DeleteCollection<'a, Kind, Scope> {
    type ResponseDecoder = DecodeSingle<ObjectList<Kind>>;

    fn to_http_request(&self) -> Result<Request<Body>> {
        Request::delete(Kind::url_path(&self.dyn_type, self.scope.namespace()))
            .body(Body::empty())
            .context(BuildRequestFailed)
    }
}

/// Patch a named object
pub struct Patch<'a, Kind: Resource + Serialize, Scope> {
    /// The name of the object to patch
    pub name: &'a str,
    /// The scope of the object
    pub scope: &'a Scope,
    /// The type of the objects
    pub dyn_type: &'a Kind::DynamicType,
    /// The patch to be applied
    pub patch: &'a params::Patch<Kind>,
}
impl<'a, Kind: Resource + Serialize + DeserializeOwned, Scope: scope::Scope> Verb for Patch<'a, Kind, Scope>
where
    params::Patch<Kind>: Serialize,
{
    type ResponseDecoder = DecodeSingle<Kind>;

    fn to_http_request(&self) -> Result<Request<Body>> {
        Request::patch(format!(
            "{}/{}",
            Kind::url_path(&self.dyn_type, self.scope.namespace()),
            self.name
        ))
        .body(Body::from(
            serde_json::to_vec(self.patch).context(SerializeFailed)?,
        ))
        .context(BuildRequestFailed)
    }
}

/// Replace a named existing object
pub struct Replace<'a, Kind: Resource, Scope> {
    /// The object to replace
    /// Requires `metadata.resourceVersion` to be `Some`
    pub object: &'a Kind,
    /// The scope of the object
    pub scope: &'a Scope,
    /// The type of the objects
    pub dyn_type: &'a Kind::DynamicType,
}
impl<'a, Kind: Resource + Serialize + DeserializeOwned, Scope: scope::Scope> Verb
    for Replace<'a, Kind, Scope>
{
    type ResponseDecoder = DecodeSingle<Kind>;

    fn to_http_request(&self) -> Result<Request<Body>> {
        Request::patch(format!(
            "{}/{}",
            Kind::url_path(&self.dyn_type, self.scope.namespace()),
            self.object.meta().name.as_ref().context(UnnamedObject)?
        ))
        .body(Body::from(
            serde_json::to_vec(self.object).context(SerializeFailed)?,
        ))
        .context(BuildRequestFailed)
    }
}

/// Get the API server's version
pub struct GetApiserverVersion;
impl Verb for GetApiserverVersion {
    type ResponseDecoder = DecodeSingle<k8s_openapi::apimachinery::pkg::version::Info>;

    fn to_http_request(&self) -> Result<Request<Body>> {
        Request::get("/version")
            .body(Body::empty())
            .context(BuildRequestFailed)
    }
}

/// Get all API groups supported by the API server
pub struct ListApiGroups;
impl Verb for ListApiGroups {
    type ResponseDecoder = DecodeSingle<k8s_openapi::apimachinery::pkg::apis::meta::v1::APIGroupList>;

    fn to_http_request(&self) -> Result<Request<Body>> {
        Request::get("/apis")
            .body(Body::empty())
            .context(BuildRequestFailed)
    }
}

/// Get all supported versions of the legacy core API group
pub struct ListCoreApiVersions;
impl Verb for ListCoreApiVersions {
    type ResponseDecoder = DecodeSingle<k8s_openapi::apimachinery::pkg::apis::meta::v1::APIVersions>;

    fn to_http_request(&self) -> Result<Request<Body>> {
        Request::get("/api")
            .body(Body::empty())
            .context(BuildRequestFailed)
    }
}

/// Get all resources supported by the API server for a given API group and version
pub struct ListApiGroupResources<'a> {
    /// The API group, use `""` for the legacy core group
    pub group: &'a str,
    /// THe API version
    pub version: &'a str,
}
impl<'a> Verb for ListApiGroupResources<'a> {
    type ResponseDecoder = DecodeSingle<k8s_openapi::apimachinery::pkg::apis::meta::v1::APIResourceList>;

    fn to_http_request(&self) -> Result<Request<Body>> {
        let path = match self.group {
            "" => format!("/api/{}", self.version),
            group => format!("/apis/{}/{}", group, self.version),
        };
        Request::get(path).body(Body::empty()).context(BuildRequestFailed)
    }
}
