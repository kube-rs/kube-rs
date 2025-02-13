//! Type information structs for dynamic resources.
use std::str::FromStr;

use crate::TypeMeta;
use k8s_openapi::{api::core::v1::ObjectReference, apimachinery::pkg::apis::meta::v1::OwnerReference};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
#[error("failed to parse group version: {0}")]
/// Failed to parse group version
pub struct ParseGroupVersionError(pub String);

/// Core information about an API Resource.
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct GroupVersionKind {
    /// API group
    pub group: String,
    /// Version
    pub version: String,
    /// Kind
    pub kind: String,
}

impl GroupVersionKind {
    /// Construct from explicit group, version, and kind
    pub fn gvk(group_: &str, version_: &str, kind_: &str) -> Self {
        let version = version_.to_string();
        let group = group_.to_string();
        let kind = kind_.to_string();

        Self { group, version, kind }
    }
}

impl TryFrom<&TypeMeta> for GroupVersionKind {
    type Error = ParseGroupVersionError;

    fn try_from(tm: &TypeMeta) -> Result<Self, Self::Error> {
        Ok(GroupVersion::from_str(&tm.api_version)?.with_kind(&tm.kind))
    }
}
impl TryFrom<TypeMeta> for GroupVersionKind {
    type Error = ParseGroupVersionError;

    fn try_from(tm: TypeMeta) -> Result<Self, Self::Error> {
        Ok(GroupVersion::from_str(&tm.api_version)?.with_kind(&tm.kind))
    }
}

impl From<OwnerReference> for GroupVersionKind {
    fn from(value: OwnerReference) -> Self {
        let (group, version) = match value.api_version.split_once("/") {
            Some((group, version)) => (group, version),
            None => ("", value.api_version.as_str()),
        };
        Self {
            group: group.into(),
            version: version.into(),
            kind: value.kind,
        }
    }
}

impl From<ObjectReference> for GroupVersionKind {
    fn from(value: ObjectReference) -> Self {
        let api_version = value.api_version.unwrap_or_default();
        let (group, version) = match api_version.split_once("/") {
            Some((group, version)) => (group, version),
            None => ("", api_version.as_str()),
        };
        Self {
            group: group.into(),
            version: version.into(),
            kind: value.kind.unwrap_or_default(),
        }
    }
}

/// Core information about a family of API Resources
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupVersion {
    /// API group
    pub group: String,
    /// Version
    pub version: String,
}

impl GroupVersion {
    /// Construct from explicit group and version
    pub fn gv(group_: &str, version_: &str) -> Self {
        let version = version_.to_string();
        let group = group_.to_string();
        Self { group, version }
    }

    /// Upgrade a GroupVersion to a GroupVersionKind
    pub fn with_kind(self, kind: &str) -> GroupVersionKind {
        GroupVersionKind {
            group: self.group,
            version: self.version,
            kind: kind.into(),
        }
    }
}

impl FromStr for GroupVersion {
    type Err = ParseGroupVersionError;

    fn from_str(gv: &str) -> Result<Self, Self::Err> {
        let gvsplit = gv.splitn(2, '/').collect::<Vec<_>>();
        let (group, version) = match *gvsplit.as_slice() {
            [g, v] => (g.to_string(), v.to_string()), // standard case
            [v] => ("".to_string(), v.to_string()),   // core v1 case
            _ => return Err(ParseGroupVersionError(gv.into())),
        };
        Ok(Self { group, version })
    }
}

impl GroupVersion {
    /// Generate the apiVersion string used in a kind's yaml
    pub fn api_version(&self) -> String {
        if self.group.is_empty() {
            self.version.clone()
        } else {
            format!("{}/{}", self.group, self.version)
        }
    }
}
impl GroupVersionKind {
    /// Generate the apiVersion string used in a kind's yaml
    pub fn api_version(&self) -> String {
        if self.group.is_empty() {
            self.version.clone()
        } else {
            format!("{}/{}", self.group, self.version)
        }
    }
}

/// Represents a type-erased object resource.
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupVersionResource {
    /// API group
    pub group: String,
    /// Version
    pub version: String,
    /// Resource
    pub resource: String,
    /// Concatenation of group and version
    #[serde(default)]
    api_version: String,
}

impl GroupVersionResource {
    /// Set the api group, version, and the plural resource name.
    pub fn gvr(group_: &str, version_: &str, resource_: &str) -> Self {
        let version = version_.to_string();
        let group = group_.to_string();
        let resource = resource_.to_string();
        let api_version = if group.is_empty() {
            version.to_string()
        } else {
            format!("{group}/{version}")
        };

        Self {
            group,
            version,
            resource,
            api_version,
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn gvk_yaml() {
        use crate::{GroupVersionKind, TypeMeta};
        let input = r#"---
apiVersion: kube.rs/v1
kind: Example
metadata:
  name: doc1
"#;
        let tm: TypeMeta = serde_yaml::from_str(input).unwrap();
        let gvk = GroupVersionKind::try_from(&tm).unwrap(); // takes ref
        let gvk2: GroupVersionKind = tm.try_into().unwrap(); // takes value
        assert_eq!(gvk.kind, "Example");
        assert_eq!(gvk.group, "kube.rs");
        assert_eq!(gvk.version, "v1");
        assert_eq!(gvk.kind, gvk2.kind);
    }
}
