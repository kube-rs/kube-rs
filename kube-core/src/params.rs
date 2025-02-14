//! A port of request parameter *Optionals from apimachinery/types.go
use crate::{request::Error, Selector};
use serde::Serialize;

/// Controls how the resource version parameter is applied for list calls
///
/// Not specifying a `VersionMatch` strategy will give you different semantics
/// depending on what `resource_version`, `limit`, `continue_token` you include with the list request.
///
/// See <https://kubernetes.io/docs/reference/using-api/api-concepts/#semantics-for-get-and-list> for details.
#[derive(Clone, Debug, PartialEq)]
pub enum VersionMatch {
    /// Returns data at least as new as the provided resource version.
    ///
    /// The newest available data is preferred, but any data not older than the provided resource version may be served.
    /// This guarantees that the collection's resource version is not older than the requested resource version,
    /// but does not make any guarantee about the resource version of any of the items in that collection.
    ///
    /// ### Any Version
    /// A degenerate, but common sub-case of `NotOlderThan` is when used together with `resource_version` "0".
    ///
    /// It is possible for a "0" resource version request to return data at a much older resource version
    /// than the client has previously observed, particularly in HA configurations, due to partitions or stale caches.
    /// Clients that cannot tolerate this should not use this semantic.
    NotOlderThan,

    /// Return data at the exact resource version provided.
    ///
    /// If the provided resource version  is unavailable, the server responds with HTTP 410 "Gone".
    /// For list requests to servers that honor the resource version Match parameter, this guarantees that the collection's
    /// resource version  is the same as the resource version  you requested in the query string.
    /// That guarantee does not apply to the resource version  of any items within that collection.
    ///
    /// Note that `Exact` cannot be used with resource version "0". For the most up-to-date list; use `Unset`.
    Exact,
}

/// Common query parameters used in list/delete calls on collections
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ListParams {
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
    pub timeout: Option<u32>,

    /// Limit the number of results.
    ///
    /// If there are more results, the server will respond with a continue token which can be used to fetch another page
    /// of results. See the [Kubernetes API docs](https://kubernetes.io/docs/reference/using-api/api-concepts/#retrieving-large-results-sets-in-chunks)
    /// for pagination details.
    pub limit: Option<u32>,

    /// Fetch a second page of results.
    ///
    /// After listing results with a limit, a continue token can be used to fetch another page of results.
    pub continue_token: Option<String>,

    /// Determines how resourceVersion is matched applied to list calls.
    pub version_match: Option<VersionMatch>,

    /// An explicit resourceVersion using the given `VersionMatch` strategy
    ///
    /// See <https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions> for details.
    pub resource_version: Option<String>,
}

impl ListParams {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if let Some(rv) = &self.resource_version {
            if self.version_match == Some(VersionMatch::Exact) && rv == "0" {
                return Err(Error::Validation(
                    "A non-zero resource_version is required when using an Exact match".into(),
                ));
            }
        } else if self.version_match.is_some() {
            return Err(Error::Validation(
                "A resource_version is required when using an explicit match".into(),
            ));
        }
        Ok(())
    }

    // Partially populate query parameters (needs resourceVersion out of band)
    pub(crate) fn populate_qp(&self, qp: &mut form_urlencoded::Serializer<String>) {
        if let Some(fields) = &self.field_selector {
            qp.append_pair("fieldSelector", fields);
        }
        if let Some(labels) = &self.label_selector {
            qp.append_pair("labelSelector", labels);
        }
        if let Some(limit) = &self.limit {
            qp.append_pair("limit", &limit.to_string());
        }
        if let Some(continue_token) = &self.continue_token {
            qp.append_pair("continue", continue_token);
        } else {
            // When there's a continue token, we don't want to set resourceVersion
            if let Some(rv) = &self.resource_version {
                if rv != "0" || self.limit.is_none() {
                    qp.append_pair("resourceVersion", rv.as_str());

                    match &self.version_match {
                        None => {}
                        Some(VersionMatch::NotOlderThan) => {
                            qp.append_pair("resourceVersionMatch", "NotOlderThan");
                        }
                        Some(VersionMatch::Exact) => {
                            qp.append_pair("resourceVersionMatch", "Exact");
                        }
                    }
                }
            }
        }
    }
}

/// Builder interface to ListParams
///
/// Usage:
/// ```
/// use kube::api::ListParams;
/// let lp = ListParams::default()
///     .match_any()
///     .timeout(60)
///     .labels("kubernetes.io/lifecycle=spot");
/// ```
impl ListParams {
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
    /// Configure typed selectors from [`Selector`](crate::Selector) and [`Expression`](crate::Expression) lists.
    ///
    /// ```
    /// use kube::core::{Expression, Selector, ParseExpressionError};
    /// # use kube::core::params::ListParams;
    /// use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
    ///
    /// // From expressions
    /// let selector: Selector = Expression::In("env".into(), ["development".into(), "sandbox".into()].into()).into();
    /// let lp = ListParams::default().labels_from(&selector);
    /// let lp = ListParams::default().labels_from(&Expression::Exists("foo".into()).into());
    ///
    /// // Native LabelSelector
    /// let selector: Selector = LabelSelector::default().try_into()?;
    /// let lp = ListParams::default().labels_from(&selector);
    /// # Ok::<(), ParseExpressionError>(())
    ///```
    #[must_use]
    pub fn labels_from(mut self, selector: &Selector) -> Self {
        self.label_selector = Some(selector.to_string());
        self
    }

    /// Sets a result limit.
    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Sets a continue token.
    #[must_use]
    pub fn continue_token(mut self, token: &str) -> Self {
        self.continue_token = Some(token.to_string());
        self
    }

    /// Sets the resource version
    #[must_use]
    pub fn at(mut self, resource_version: &str) -> Self {
        self.resource_version = Some(resource_version.into());
        self
    }

    /// Sets an arbitary resource version match strategy
    ///
    /// A non-default strategy such as `VersionMatch::Exact` or `VersionMatch::NotOlderThan`
    /// requires an explicit `resource_version` set to pass request validation.
    #[must_use]
    pub fn matching(mut self, version_match: VersionMatch) -> Self {
        self.version_match = Some(version_match);
        self
    }

    /// Use the semantic "any" resource version strategy
    ///
    /// This is a less taxing variant of the default list, returning data at any resource version.
    /// It will prefer the newest avialable resource version, but strong consistency is not required;
    /// data at any resource version may be served.
    /// It is possible for the request to return data at a much older resource version than the client
    /// has previously observed, particularly in high availability configurations, due to partitions or stale caches.
    /// Clients that cannot tolerate this should not use this semantic.
    #[must_use]
    pub fn match_any(self) -> Self {
        self.matching(VersionMatch::NotOlderThan).at("0")
    }
}

/// Common query parameters used in get calls
#[derive(Clone, Debug, Default, PartialEq)]
pub struct GetParams {
    /// An explicit resourceVersion with implicit version matching strategies
    ///
    /// Default (unset) gives the most recent version. "0" gives a less
    /// consistent, but more performant "Any" version. Specifing a version is
    /// like providing a `VersionMatch::NotOlderThan`.
    /// See <https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions> for details.
    pub resource_version: Option<String>,
}

/// Helper interface to GetParams
///
/// Usage:
/// ```
/// use kube::api::GetParams;
/// let gp = GetParams::at("6664");
/// ```
impl GetParams {
    /// Sets the resource version, implicitly applying a 'NotOlderThan' match
    #[must_use]
    pub fn at(resource_version: &str) -> Self {
        Self {
            resource_version: Some(resource_version.into()),
        }
    }

    /// Sets the resource version to "0"
    #[must_use]
    pub fn any() -> Self {
        Self::at("0")
    }
}

/// The validation directive to use for `fieldValidation` when using server-side apply.
#[derive(Clone, Debug)]
pub enum ValidationDirective {
    /// Strict mode will fail any invalid manifests.
    ///
    /// This will fail the request with a BadRequest error if any unknown fields would be dropped from the
    /// object, or if any duplicate fields are present. The error returned from the server will contain
    /// all unknown and duplicate fields encountered.
    Strict,
    /// Warn mode will return a warning for invalid manifests.
    ///
    /// This will send a warning via the standard warning response header for each unknown field that
    /// is dropped from the object, and for each duplicate field that is encountered. The request will
    /// still succeed if there are no other errors, and will only persist the last of any duplicate fields.
    Warn,
    /// Ignore mode will silently ignore any problems.
    ///
    /// This will ignore any unknown fields that are silently dropped from the object, and will ignore
    /// all but the last duplicate field that the decoder encounters.
    Ignore,
}

impl ValidationDirective {
    /// Returns the string format of the directive
    pub fn as_str(&self) -> &str {
        match self {
            Self::Strict => "Strict",
            Self::Warn => "Warn",
            Self::Ignore => "Ignore",
        }
    }
}

/// Common query parameters used in watch calls on collections
#[derive(Clone, Debug, PartialEq)]
pub struct WatchParams {
    /// A selector to restrict returned objects by their labels.
    ///
    /// Defaults to everything if `None`.
    pub label_selector: Option<String>,

    /// A selector to restrict returned objects by their fields.
    ///
    /// Defaults to everything if `None`.
    pub field_selector: Option<String>,

    /// Timeout for the watch call.
    ///
    /// This limits the duration of the call, regardless of any activity or inactivity.
    /// If unset for a watch call, we will use 290s.
    /// We limit this to 295s due to [inherent watch limitations](https://github.com/kubernetes/kubernetes/issues/6513).
    pub timeout: Option<u32>,

    /// Enables watch events with type "BOOKMARK".
    ///
    /// Servers that do not implement bookmarks ignore this flag and
    /// bookmarks are sent at the server's discretion. Clients should not
    /// assume bookmarks are returned at any specific interval, nor may they
    /// assume the server will send any BOOKMARK event during a session.
    /// If this is not a watch, this field is ignored.
    /// If the feature gate WatchBookmarks is not enabled in apiserver,
    /// this field is ignored.
    pub bookmarks: bool,

    /// Kubernetes 1.27 Streaming Lists
    /// `sendInitialEvents=true` may be set together with `watch=true`.
    /// In that case, the watch stream will begin with synthetic events to
    /// produce the current state of objects in the collection. Once all such
    /// events have been sent, a synthetic "Bookmark" event  will be sent.
    /// The bookmark will report the ResourceVersion (RV) corresponding to the
    /// set of objects, and be marked with `"k8s.io/initial-events-end": "true"` annotation.
    /// Afterwards, the watch stream will proceed as usual, sending watch events
    /// corresponding to changes (subsequent to the RV) to objects watched.
    ///
    /// When `sendInitialEvents` option is set, we require `resourceVersionMatch`
    /// option to also be set. The semantic of the watch request is as following:
    /// - `resourceVersionMatch` = NotOlderThan
    ///   is interpreted as "data at least as new as the provided `resourceVersion`"
    ///   and the bookmark event is send when the state is synced
    ///   to a `resourceVersion` at least as fresh as the one provided by the ListOptions.
    ///   If `resourceVersion` is unset, this is interpreted as "consistent read" and the
    ///   bookmark event is send when the state is synced at least to the moment
    ///   when request started being processed.
    /// - `resourceVersionMatch` set to any other value or unset
    ///   Invalid error is returned.
    pub send_initial_events: bool,
}

impl WatchParams {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if let Some(to) = &self.timeout {
            // https://github.com/kubernetes/kubernetes/issues/6513
            if *to >= 295 {
                return Err(Error::Validation("WatchParams::timeout must be < 295s".into()));
            }
        }
        if self.send_initial_events && !self.bookmarks {
            return Err(Error::Validation(
                "WatchParams::bookmarks must be set when using send_initial_events".into(),
            ));
        }
        Ok(())
    }

    // Partially populate query parameters (needs resourceVersion out of band)
    pub(crate) fn populate_qp(&self, qp: &mut form_urlencoded::Serializer<String>) {
        qp.append_pair("watch", "true");

        // https://github.com/kubernetes/kubernetes/issues/6513
        qp.append_pair("timeoutSeconds", &self.timeout.unwrap_or(290).to_string());

        if let Some(fields) = &self.field_selector {
            qp.append_pair("fieldSelector", fields);
        }
        if let Some(labels) = &self.label_selector {
            qp.append_pair("labelSelector", labels);
        }
        if self.bookmarks {
            qp.append_pair("allowWatchBookmarks", "true");
        }
        if self.send_initial_events {
            qp.append_pair("sendInitialEvents", "true");
            qp.append_pair("resourceVersionMatch", "NotOlderThan");
        }
    }
}

impl Default for WatchParams {
    /// Default `WatchParams` without any constricting selectors
    fn default() -> Self {
        Self {
            // bookmarks stable since 1.17, and backwards compatible
            bookmarks: true,

            label_selector: None,
            field_selector: None,
            timeout: None,
            send_initial_events: false,
        }
    }
}

/// Builder interface to WatchParams
///
/// Usage:
/// ```
/// use kube::api::WatchParams;
/// let lp = WatchParams::default()
///     .timeout(60)
///     .labels("kubernetes.io/lifecycle=spot");
/// ```
impl WatchParams {
    /// Configure the timeout for watch calls
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
    /// Configure typed selectors from [`Selector`](crate::Selector) and [`Expression`](crate::Expression) lists.
    ///
    /// ```
    /// use kube::core::{Expression, Selector, ParseExpressionError};
    /// # use kube::core::params::WatchParams;
    /// use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
    ///
    /// // From expressions
    /// let selector: Selector = Expression::In("env".into(), ["development".into(), "sandbox".into()].into()).into();
    /// let wp = WatchParams::default().labels_from(&selector);
    /// let wp = WatchParams::default().labels_from(&Expression::Exists("foo".into()).into());
    ///
    /// // Native LabelSelector
    /// let selector: Selector = LabelSelector::default().try_into()?;
    /// let wp = WatchParams::default().labels_from(&selector);
    /// # Ok::<(), ParseExpressionError>(())
    ///```
    #[must_use]
    pub fn labels_from(mut self, selector: &Selector) -> Self {
        self.label_selector = Some(selector.to_string());
        self
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

    /// Kubernetes 1.27 Streaming Lists
    /// `sendInitialEvents=true` may be set together with `watch=true`.
    /// In that case, the watch stream will begin with synthetic events to
    /// produce the current state of objects in the collection. Once all such
    /// events have been sent, a synthetic "Bookmark" event  will be sent.
    /// The bookmark will report the ResourceVersion (RV) corresponding to the
    /// set of objects, and be marked with `"k8s.io/initial-events-end": "true"` annotation.
    /// Afterwards, the watch stream will proceed as usual, sending watch events
    /// corresponding to changes (subsequent to the RV) to objects watched.
    ///
    /// When `sendInitialEvents` option is set, we require `resourceVersionMatch`
    /// option to also be set. The semantic of the watch request is as following:
    /// - `resourceVersionMatch` = NotOlderThan
    ///   is interpreted as "data at least as new as the provided `resourceVersion`"
    ///   and the bookmark event is send when the state is synced
    ///   to a `resourceVersion` at least as fresh as the one provided by the ListOptions.
    ///   If `resourceVersion` is unset, this is interpreted as "consistent read" and the
    ///   bookmark event is send when the state is synced at least to the moment
    ///   when request started being processed.
    /// - `resourceVersionMatch` set to any other value or unset
    ///   Invalid error is returned.
    ///
    /// Defaults to true if `resourceVersion=""` or `resourceVersion="0"` (for backward
    /// compatibility reasons) and to false otherwise.
    #[must_use]
    pub fn initial_events(mut self) -> Self {
        self.send_initial_events = true;

        self
    }

    /// Constructor for doing Kubernetes 1.27 Streaming List watches
    ///
    /// Enables [`VersionMatch::NotOlderThan`] semantics and [`WatchParams::send_initial_events`].
    pub fn streaming_lists() -> Self {
        Self {
            send_initial_events: true,
            bookmarks: true, // required
            ..WatchParams::default()
        }
    }
}

/// Common query parameters for put/post calls
#[derive(Default, Clone, Debug, PartialEq)]
pub struct PostParams {
    /// Whether to run this as a dry run
    pub dry_run: bool,
    /// fieldManager is a name of the actor that is making changes
    pub field_manager: Option<String>,
}

impl PostParams {
    pub(crate) fn populate_qp(&self, qp: &mut form_urlencoded::Serializer<String>) {
        if self.dry_run {
            qp.append_pair("dryRun", "All");
        }
        if let Some(ref fm) = self.field_manager {
            qp.append_pair("fieldManager", fm);
        }
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        if let Some(field_manager) = &self.field_manager {
            // Implement the easy part of validation, in future this may be extended to provide validation as in go code
            // For now it's fine, because k8s API server will return an error
            if field_manager.len() > 128 {
                return Err(Error::Validation(
                    "Failed to validate PostParams::field_manager!".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Describes changes that should be applied to a resource
///
/// Takes arbitrary serializable data for all strategies except `Json`.
///
/// We recommend using ([server-side](https://kubernetes.io/blog/2020/04/01/kubernetes-1.18-feature-server-side-apply-beta-2)) `Apply` patches on new kubernetes releases.
///
/// See [kubernetes patch docs](https://kubernetes.io/docs/tasks/run-application/update-api-object-kubectl-patch/#use-a-json-merge-patch-to-update-a-deployment) for the older patch types.
///
/// Note that applying an invalid patch will **not** always return an error. See an example of such a case below.
///
/// Note that patches have different effects on different fields depending on their merge strategies.
/// These strategies are configurable when deriving your [`CustomResource`](https://docs.rs/kube-derive/*/kube_derive/derive.CustomResource.html#customizing-schemas).
///
/// # Creating a patch via serde_json
/// ```
/// use kube::api::Patch;
/// let patch = serde_json::json!({
///     "apiVersion": "v1",
///     "kind": "Pod",
///     "metadata": {
///         "name": "blog"
///     },
///     "spec": {
///         "activeDeadlineSeconds": 5
///     }
/// });
/// let patch = Patch::Apply(&patch);
/// ```
/// # Creating a patch from a type
/// ```
/// use kube::api::Patch;
/// use k8s_openapi::api::rbac::v1::Role;
/// use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
/// let r = Role {
///     metadata: ObjectMeta { name: Some("user".into()), ..ObjectMeta::default() },
///     rules: Some(vec![])
/// };
/// let patch = Patch::Apply(&r);
/// ```
/// # Invalid Patches
///
/// In this example patch contains a `PodSpec` and **not** a complete or partial `Pod`.
/// This patch is invalid as the full structure of a resource is required.
/// The invalid patch will be accepted by the cluster, but no changes will be made.
///
/// Using serve-side style [`Apply`](Patch::Apply) patches mitigates this issue.
///
/// ```no_run
/// use k8s_openapi::api::core::v1::{Pod, PodSpec};
/// use kube::{Api, api::{PatchParams, Patch}};
///
/// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = kube::Client::try_default().await?;
/// let pods: Api<Pod> = Api::namespaced(client, "apps");
/// let pp = PatchParams::default();
///
/// let invalid_patch: PodSpec = serde_json::from_value(serde_json::json!({
///                 "activeDeadlineSeconds": 5
/// }))?;
///
/// // This will have no effect on mypod.
/// pods.patch("mypod", &pp, &Patch::Strategic(invalid_patch)).await?;
///
/// let valid_patch: Pod = serde_json::from_value(serde_json::json!({
///           "spec": {
///                 "activeDeadlineSeconds": 5
///            }
/// }))?;
///
/// // This will set activeDeadlineSeconds to 5.
/// pods.patch("mypod", &pp, &Patch::Strategic(valid_patch)).await?;
///
/// # Ok(())
/// # }
/// ```
///
///
#[non_exhaustive]
#[derive(Debug, PartialEq, Clone)]
pub enum Patch<T: Serialize> {
    /// [Server side apply](https://kubernetes.io/docs/reference/using-api/api-concepts/#server-side-apply)
    ///
    /// Requires kubernetes >= 1.16
    Apply(T),

    /// [JSON patch](https://kubernetes.io/docs/tasks/run-application/update-api-object-kubectl-patch/#use-a-json-merge-patch-to-update-a-deployment)
    ///
    /// Using this variant will require you to explicitly provide a type for `T` at the moment.
    ///
    /// # Example
    ///
    /// ```
    /// use kube::api::Patch;
    /// let json_patch = json_patch::Patch(vec![]);
    /// let patch = Patch::Json::<()>(json_patch);
    /// ```
    #[cfg(feature = "jsonpatch")]
    #[cfg_attr(docsrs, doc(cfg(feature = "jsonpatch")))]
    Json(json_patch::Patch),

    /// [JSON Merge patch](https://kubernetes.io/docs/tasks/run-application/update-api-object-kubectl-patch/#use-a-json-merge-patch-to-update-a-deployment)
    Merge(T),
    /// [Strategic JSON Merge patch](https://kubernetes.io/docs/tasks/run-application/update-api-object-kubectl-patch/#use-a-strategic-merge-patch-to-update-a-deployment)
    Strategic(T),
}

impl<T: Serialize> Patch<T> {
    pub(crate) fn is_apply(&self) -> bool {
        matches!(self, Patch::Apply(_))
    }

    pub(crate) fn content_type(&self) -> &'static str {
        match &self {
            Self::Apply(_) => "application/apply-patch+yaml",
            #[cfg(feature = "jsonpatch")]
            #[cfg_attr(docsrs, doc(cfg(feature = "jsonpatch")))]
            Self::Json(_) => "application/json-patch+json",
            Self::Merge(_) => "application/merge-patch+json",
            Self::Strategic(_) => "application/strategic-merge-patch+json",
        }
    }
}

impl<T: Serialize> Patch<T> {
    pub(crate) fn serialize(&self) -> Result<Vec<u8>, serde_json::Error> {
        match self {
            Self::Apply(p) => serde_json::to_vec(p),
            #[cfg(feature = "jsonpatch")]
            #[cfg_attr(docsrs, doc(cfg(feature = "jsonpatch")))]
            Self::Json(p) => serde_json::to_vec(p),
            Self::Strategic(p) => serde_json::to_vec(p),
            Self::Merge(p) => serde_json::to_vec(p),
        }
    }
}

/// Common query parameters for patch calls
#[derive(Default, Clone, Debug)]
pub struct PatchParams {
    /// Whether to run this as a dry run
    pub dry_run: bool,
    /// force Apply requests. Applicable only to [`Patch::Apply`].
    pub force: bool,
    /// fieldManager is a name of the actor that is making changes. Required for [`Patch::Apply`]
    /// optional for everything else.
    pub field_manager: Option<String>,
    /// The server-side validation directive to use. Applicable only to [`Patch::Apply`].
    pub field_validation: Option<ValidationDirective>,
}

impl PatchParams {
    pub(crate) fn validate<P: Serialize>(&self, patch: &Patch<P>) -> Result<(), Error> {
        if let Some(field_manager) = &self.field_manager {
            // Implement the easy part of validation, in future this may be extended to provide validation as in go code
            // For now it's fine, because k8s API server will return an error
            if field_manager.len() > 128 {
                return Err(Error::Validation(
                    "Failed to validate PatchParams::field_manager!".into(),
                ));
            }
        }
        if self.force && !patch.is_apply() {
            return Err(Error::Validation(
                "PatchParams::force only works with Patch::Apply".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn populate_qp(&self, qp: &mut form_urlencoded::Serializer<String>) {
        if self.dry_run {
            qp.append_pair("dryRun", "All");
        }
        if self.force {
            qp.append_pair("force", "true");
        }
        if let Some(ref fm) = self.field_manager {
            qp.append_pair("fieldManager", fm);
        }
        if let Some(sv) = &self.field_validation {
            qp.append_pair("fieldValidation", sv.as_str());
        }
    }

    /// Construct `PatchParams` for server-side apply
    #[must_use]
    pub fn apply(manager: &str) -> Self {
        Self {
            field_manager: Some(manager.into()),
            ..Self::default()
        }
    }

    /// Force the result through on conflicts
    ///
    /// NB: Force is a concept restricted to the server-side [`Patch::Apply`].
    #[must_use]
    pub fn force(mut self) -> Self {
        self.force = true;
        self
    }

    /// Perform a dryRun only
    #[must_use]
    pub fn dry_run(mut self) -> Self {
        self.dry_run = true;
        self
    }

    /// Set the validation directive for `fieldValidation` during server-side apply.
    pub fn validation(mut self, vd: ValidationDirective) -> Self {
        self.field_validation = Some(vd);
        self
    }

    /// Set the validation directive to `Ignore`
    #[must_use]
    pub fn validation_ignore(self) -> Self {
        self.validation(ValidationDirective::Ignore)
    }

    /// Set the validation directive to `Warn`
    #[must_use]
    pub fn validation_warn(self) -> Self {
        self.validation(ValidationDirective::Warn)
    }

    /// Set the validation directive to `Strict`
    #[must_use]
    pub fn validation_strict(self) -> Self {
        self.validation(ValidationDirective::Strict)
    }
}

/// Common query parameters for delete calls
#[derive(Default, Clone, Serialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DeleteParams {
    /// When present, indicates that modifications should not be persisted.
    #[serde(
        serialize_with = "dry_run_all_ser",
        skip_serializing_if = "std::ops::Not::not"
    )]
    pub dry_run: bool,

    /// The duration in seconds before the object should be deleted.
    ///
    /// Value must be non-negative integer. The value zero indicates delete immediately.
    /// If this value is `None`, the default grace period for the specified type will be used.
    /// Defaults to a per object value if not specified. Zero means delete immediately.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grace_period_seconds: Option<u32>,

    /// Whether or how garbage collection is performed.
    ///
    /// The default policy is decided by the existing finalizer set in
    /// `metadata.finalizers`, and the resource-specific default policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub propagation_policy: Option<PropagationPolicy>,

    /// Condtions that must be fulfilled before a deletion is carried out
    ///
    /// If not possible, a `409 Conflict` status will be returned.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preconditions: Option<Preconditions>,
}

impl DeleteParams {
    /// Construct `DeleteParams` with `PropagationPolicy::Background`.
    ///
    /// This allows the garbage collector to delete the dependents in the background.
    pub fn background() -> Self {
        Self {
            propagation_policy: Some(PropagationPolicy::Background),
            ..Self::default()
        }
    }

    /// Construct `DeleteParams` with `PropagationPolicy::Foreground`.
    ///
    /// This is a cascading policy that deletes all dependents in the foreground.
    pub fn foreground() -> Self {
        Self {
            propagation_policy: Some(PropagationPolicy::Foreground),
            ..Self::default()
        }
    }

    /// Construct `DeleteParams` with `PropagationPolicy::Orphan`.
    ///
    ///
    /// This orpans the dependents.
    pub fn orphan() -> Self {
        Self {
            propagation_policy: Some(PropagationPolicy::Orphan),
            ..Self::default()
        }
    }

    /// Perform a dryRun only
    #[must_use]
    pub fn dry_run(mut self) -> Self {
        self.dry_run = true;
        self
    }

    /// Set the duration in seconds before the object should be deleted.
    #[must_use]
    pub fn grace_period(mut self, secs: u32) -> Self {
        self.grace_period_seconds = Some(secs);
        self
    }

    /// Set the condtions that must be fulfilled before a deletion is carried out.
    #[must_use]
    pub fn preconditions(mut self, preconditions: Preconditions) -> Self {
        self.preconditions = Some(preconditions);
        self
    }

    pub(crate) fn is_default(&self) -> bool {
        !self.dry_run
            && self.grace_period_seconds.is_none()
            && self.propagation_policy.is_none()
            && self.preconditions.is_none()
    }
}

// dryRun serialization differ when used as body parameters and query strings:
// query strings are either true/false
// body params allow only: missing field, or ["All"]
// The latter is a very awkward API causing users to do to
// dp.dry_run = vec!["All".into()];
// just to turn on dry_run..
// so we hide this detail for now.
fn dry_run_all_ser<S>(t: &bool, s: S) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::ser::Serializer,
{
    use serde::ser::SerializeTuple;
    match t {
        true => {
            let mut map = s.serialize_tuple(1)?;
            map.serialize_element("All")?;
            map.end()
        }
        false => s.serialize_none(),
    }
}
#[cfg(test)]
mod test {
    use crate::{params::WatchParams, Expression, Selector};

    use super::{DeleteParams, ListParams, PatchParams};
    #[test]
    fn delete_param_serialize() {
        let mut dp = DeleteParams::default();
        let emptyser = serde_json::to_string(&dp).unwrap();
        //println!("emptyser is: {}", emptyser);
        assert_eq!(emptyser, "{}");

        dp.dry_run = true;
        let ser = serde_json::to_string(&dp).unwrap();
        //println!("ser is: {}", ser);
        assert_eq!(ser, "{\"dryRun\":[\"All\"]}");
    }

    #[test]
    fn delete_param_constructors() {
        let dp_background = DeleteParams::background();
        let ser = serde_json::to_value(dp_background).unwrap();
        assert_eq!(ser, serde_json::json!({"propagationPolicy": "Background"}));

        let dp_foreground = DeleteParams::foreground();
        let ser = serde_json::to_value(dp_foreground).unwrap();
        assert_eq!(ser, serde_json::json!({"propagationPolicy": "Foreground"}));

        let dp_orphan = DeleteParams::orphan();
        let ser = serde_json::to_value(dp_orphan).unwrap();
        assert_eq!(ser, serde_json::json!({"propagationPolicy": "Orphan"}));
    }

    #[test]
    fn patch_param_serializes_field_validation() {
        let pp = PatchParams::default().validation_ignore();
        let mut qp = form_urlencoded::Serializer::new(String::from("some/resource?"));
        pp.populate_qp(&mut qp);
        let urlstr = qp.finish();
        assert_eq!(String::from("some/resource?&fieldValidation=Ignore"), urlstr);

        let pp = PatchParams::default().validation_warn();
        let mut qp = form_urlencoded::Serializer::new(String::from("some/resource?"));
        pp.populate_qp(&mut qp);
        let urlstr = qp.finish();
        assert_eq!(String::from("some/resource?&fieldValidation=Warn"), urlstr);

        let pp = PatchParams::default().validation_strict();
        let mut qp = form_urlencoded::Serializer::new(String::from("some/resource?"));
        pp.populate_qp(&mut qp);
        let urlstr = qp.finish();
        assert_eq!(String::from("some/resource?&fieldValidation=Strict"), urlstr);
    }

    #[test]
    fn list_params_serialize() {
        let selector: Selector =
            Expression::In("env".into(), ["development".into(), "sandbox".into()].into()).into();
        let lp = ListParams::default().labels_from(&selector);
        let labels = lp.label_selector.unwrap();
        assert_eq!(labels, "env in (development,sandbox)");
    }

    #[test]
    fn watch_params_serialize() {
        let selector: Selector =
            Expression::In("env".into(), ["development".into(), "sandbox".into()].into()).into();
        let wp = WatchParams::default().labels_from(&selector);
        let labels = wp.label_selector.unwrap();
        assert_eq!(labels, "env in (development,sandbox)");
    }
}

/// Preconditions must be fulfilled before an operation (update, delete, etc.) is carried out.
#[derive(Default, Clone, Serialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Preconditions {
    /// Specifies the target ResourceVersion
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
    /// Specifies the target UID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
}

/// Propagation policy when deleting single objects
#[derive(Clone, Debug, Serialize, PartialEq)]
pub enum PropagationPolicy {
    /// Orphan dependents
    Orphan,
    /// Allow the garbage collector to delete the dependents in the background
    Background,
    /// A cascading policy that deletes all dependents in the foreground
    Foreground,
}
