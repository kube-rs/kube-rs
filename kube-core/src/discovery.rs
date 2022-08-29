//! Type information structs for API discovery
use crate::{gvk::GroupVersionKind, resource::Resource};
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::{
    CustomResourceDefinition, CustomResourceDefinitionVersion,
};
use serde::{Deserialize, Serialize};

/// Information about a Kubernetes API resource
///
/// Enough information to use it like a `Resource` by passing it to the dynamic `Api`
/// constructors like `Api::all_with` and `Api::namespaced_with`.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct ApiResource {
    /// Resource group, empty for core group.
    pub group: String,
    /// group version
    pub version: String,
    /// apiVersion of the resource (v1 for core group,
    /// groupName/groupVersions for other).
    pub api_version: String,
    /// Singular PascalCase name of the resource
    pub kind: String,
    /// Plural name of the resource
    pub plural: String,
}

impl ApiResource {
    /// Creates an ApiResource by type-erasing a Resource
    pub fn erase<K: Resource>(dt: &K::DynamicType) -> Self {
        ApiResource {
            group: K::group(dt).to_string(),
            version: K::version(dt).to_string(),
            api_version: K::api_version(dt).to_string(),
            kind: K::kind(dt).to_string(),
            plural: K::plural(dt).to_string(),
        }
    }

    /// Creates an ApiResource from group, version, kind and plural name.
    pub fn from_gvk_with_plural(gvk: &GroupVersionKind, plural: &str) -> Self {
        ApiResource {
            api_version: gvk.api_version(),
            group: gvk.group.clone(),
            version: gvk.version.clone(),
            kind: gvk.kind.clone(),
            plural: plural.to_string(),
        }
    }

    /// Creates an ApiResource from group, version and kind.
    ///
    /// # Warning
    /// This function will **guess** the resource plural name.
    /// Usually, this is ok, but for CRDs with complex pluralisations it can fail.
    /// If you are getting your values from `kube_derive` use the generated method for giving you an [`ApiResource`].
    /// Otherwise consider using [`ApiResource::from_gvk_with_plural`](crate::discovery::ApiResource::from_gvk_with_plural)
    /// to explicitly set the plural, or run api discovery on it via `kube::discovery`.
    pub fn from_gvk(gvk: &GroupVersionKind) -> Self {
        ApiResource::from_gvk_with_plural(gvk, &to_plural(&gvk.kind.to_ascii_lowercase()))
    }

    /// Returns [`ApiResource`] instance for the custom resource,
    /// defined by the CRD. You may use this `ApiResource` to manipulate
    /// instances of the CRD.
    /// ```no_run
    /// # use kube::{Api, Client, core::{ApiResource, DynamicObject, ResourceExt, discovery::ApiResourceFromCrdHint}};
    /// # use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
    /// # use std::error::Error;
    /// async fn print_instances(k: &kube::Client, crd: &CustomResourceDefinition) -> Result<(), Box<dyn Error>> {
    ///     let api_resource = ApiResource::from_crd(crd, &ApiResourceFromCrdHint::SingleVersioned)
    ///         .expect("in this example we assume crd has one version");
    ///     let api = Api::<DynamicObject>::all_with(k.clone(), &api_resource);
    ///     for item in api.list(&Default::default()).await? {
    ///         println!("{}", item.name_unchecked());
    ///     }
    ///     Ok(())
    /// }
    /// ```
    /// # Hint
    /// If crd has multiple versions, than it defines several api resources in fact.
    /// Therefore this function takes additional `hint` parameter. See [`ApiResourceFromCrdHint`]
    /// for effect of each variant.
    pub fn from_crd(crd: &CustomResourceDefinition, hint: &ApiResourceFromCrdHint) -> Option<ApiResource> {
        let kind = crd.spec.names.kind.clone();
        let group = crd.spec.group.clone();
        let plural = crd.spec.names.plural.clone();
        let version = hint.select(crd)?.name.clone();
        let gvk = GroupVersionKind { group, version, kind };

        Some(ApiResource::from_gvk_with_plural(&gvk, &plural))
    }
}

/// Hint for [`ApiResource::from_crd`]
pub enum ApiResourceFromCrdHint {
    /// Assume that CRD has only one version.
    /// If this assumption is violated, None will be returned.
    SingleVersioned,
    /// Use given version.
    /// If CRD does not have this version, None will be returned.
    Exact(String),
    /// Use storage version. This hint always succeeds for valid CRDs.
    Storage,
}

impl ApiResourceFromCrdHint {
    fn select<'a>(&self, crd: &'a CustomResourceDefinition) -> Option<&'a CustomResourceDefinitionVersion> {
        match self {
            ApiResourceFromCrdHint::SingleVersioned => {
                if crd.spec.versions.len() == 1 {
                    Some(&crd.spec.versions[0])
                } else {
                    None
                }
            }
            ApiResourceFromCrdHint::Exact(expected) => {
                crd.spec.versions.iter().find(|ver| ver.name == *expected)
            }
            ApiResourceFromCrdHint::Storage => crd.spec.versions.iter().find(|ver| ver.storage),
        }
    }
}

/// Resource scope
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum Scope {
    /// Objects are global
    Cluster,
    /// Each object lives in namespace.
    Namespaced,
}

/// Rbac verbs for ApiCapabilities
pub mod verbs {
    /// Create a resource
    pub const CREATE: &str = "create";
    /// Get single resource
    pub const GET: &str = "get";
    /// List objects
    pub const LIST: &str = "list";
    /// Watch for objects changes
    pub const WATCH: &str = "watch";
    /// Delete single object
    pub const DELETE: &str = "delete";
    /// Delete multiple objects at once
    pub const DELETE_COLLECTION: &str = "deletecollection";
    /// Update an object
    pub const UPDATE: &str = "update";
    /// Patch an object
    pub const PATCH: &str = "patch";
}

/// Contains the capabilities of an API resource
#[derive(Debug, Clone)]
pub struct ApiCapabilities {
    /// Scope of the resource
    pub scope: Scope,
    /// Available subresources.
    ///
    /// Please note that returned ApiResources are not standalone resources.
    /// Their name will be of form `subresource_name`, not `resource_name/subresource_name`.
    /// To work with subresources, use `Request` methods for now.
    pub subresources: Vec<(ApiResource, ApiCapabilities)>,
    /// Supported operations on this resource
    pub operations: Vec<String>,
}

impl ApiCapabilities {
    /// Checks that given verb is supported on this resource.
    pub fn supports_operation(&self, operation: &str) -> bool {
        self.operations.iter().any(|op| op == operation)
    }
}

// Simple pluralizer. Handles the special cases.
fn to_plural(word: &str) -> String {
    if word == "endpoints" || word == "endpointslices" {
        return word.to_owned();
    } else if word == "nodemetrics" {
        return "nodes".to_owned();
    } else if word == "podmetrics" {
        return "pods".to_owned();
    }

    // Words ending in s, x, z, ch, sh will be pluralized with -es (eg. foxes).
    if word.ends_with('s')
        || word.ends_with('x')
        || word.ends_with('z')
        || word.ends_with("ch")
        || word.ends_with("sh")
    {
        return format!("{}es", word);
    }

    // Words ending in y that are preceded by a consonant will be pluralized by
    // replacing y with -ies (eg. puppies).
    if word.ends_with('y') {
        if let Some(c) = word.chars().nth(word.len() - 2) {
            if !matches!(c, 'a' | 'e' | 'i' | 'o' | 'u') {
                // Remove 'y' and add `ies`
                let mut chars = word.chars();
                chars.next_back();
                return format!("{}ies", chars.as_str());
            }
        }
    }

    // All other words will have "s" added to the end (eg. days).
    format!("{}s", word)
}

#[test]
fn test_to_plural_native() {
    // Extracted from `swagger.json`
    #[rustfmt::skip]
    let native_kinds = vec![
        ("APIService", "apiservices"),
        ("Binding", "bindings"),
        ("CertificateSigningRequest", "certificatesigningrequests"),
        ("ClusterRole", "clusterroles"), ("ClusterRoleBinding", "clusterrolebindings"),
        ("ComponentStatus", "componentstatuses"),
        ("ConfigMap", "configmaps"),
        ("ControllerRevision", "controllerrevisions"),
        ("CronJob", "cronjobs"),
        ("CSIDriver", "csidrivers"), ("CSINode", "csinodes"), ("CSIStorageCapacity", "csistoragecapacities"),
        ("CustomResourceDefinition", "customresourcedefinitions"),
        ("DaemonSet", "daemonsets"),
        ("Deployment", "deployments"),
        ("Endpoints", "endpoints"), ("EndpointSlice", "endpointslices"),
        ("Event", "events"),
        ("FlowSchema", "flowschemas"),
        ("HorizontalPodAutoscaler", "horizontalpodautoscalers"),
        ("Ingress", "ingresses"), ("IngressClass", "ingressclasses"),
        ("Job", "jobs"),
        ("Lease", "leases"),
        ("LimitRange", "limitranges"),
        ("LocalSubjectAccessReview", "localsubjectaccessreviews"),
        ("MutatingWebhookConfiguration", "mutatingwebhookconfigurations"),
        ("Namespace", "namespaces"),
        ("NetworkPolicy", "networkpolicies"),
        ("Node", "nodes"),
        ("PersistentVolumeClaim", "persistentvolumeclaims"),
        ("PersistentVolume", "persistentvolumes"),
        ("PodDisruptionBudget", "poddisruptionbudgets"),
        ("Pod", "pods"),
        ("PodSecurityPolicy", "podsecuritypolicies"),
        ("PodTemplate", "podtemplates"),
        ("PriorityClass", "priorityclasses"),
        ("PriorityLevelConfiguration", "prioritylevelconfigurations"),
        ("ReplicaSet", "replicasets"),
        ("ReplicationController", "replicationcontrollers"),
        ("ResourceQuota", "resourcequotas"),
        ("Role", "roles"), ("RoleBinding", "rolebindings"),
        ("RuntimeClass", "runtimeclasses"),
        ("Secret", "secrets"),
        ("SelfSubjectAccessReview", "selfsubjectaccessreviews"),
        ("SelfSubjectRulesReview", "selfsubjectrulesreviews"),
        ("ServiceAccount", "serviceaccounts"),
        ("Service", "services"),
        ("StatefulSet", "statefulsets"),
        ("StorageClass", "storageclasses"), ("StorageVersion", "storageversions"),
        ("SubjectAccessReview", "subjectaccessreviews"),
        ("TokenReview", "tokenreviews"),
        ("ValidatingWebhookConfiguration", "validatingwebhookconfigurations"),
        ("VolumeAttachment", "volumeattachments"),
    ];
    for (kind, plural) in native_kinds {
        assert_eq!(to_plural(&kind.to_ascii_lowercase()), plural);
    }
}
