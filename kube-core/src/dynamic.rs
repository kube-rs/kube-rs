//! Contains types for using resource kinds not known at compile-time.
//!
//! For concrete usage see [examples prefixed with dynamic_](https://github.com/clux/kube-rs/tree/master/examples).

pub use crate::discovery::ApiResource;
use crate::{metadata::TypeMeta, resource::Resource};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use std::borrow::Cow;

/// A dynamic representation of a kubernetes object
///
/// This will work with any non-list type object.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct DynamicObject {
    /// The type fields, not always present
    #[serde(flatten, default)]
    pub types: Option<TypeMeta>,
    /// Object metadata
    pub metadata: ObjectMeta,

    /// All other keys
    #[serde(flatten)]
    pub data: serde_json::Value,
}

impl DynamicObject {
    /// Create a DynamicObject with minimal values set from ApiResource.
    pub fn new(name: &str, resource: &ApiResource) -> Self {
        Self {
            types: Some(TypeMeta {
                api_version: resource.api_version.to_string(),
                kind: resource.kind.to_string(),
            }),
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                ..Default::default()
            },
            data: Default::default(),
        }
    }

    /// Attach dynamic data to a DynamicObject
    pub fn data(mut self, data: serde_json::Value) -> Self {
        self.data = data;
        self
    }

    /// Attach a namespace to a DynamicObject
    pub fn within(mut self, ns: &str) -> Self {
        self.metadata.namespace = Some(ns.into());
        self
    }
}

impl Resource for DynamicObject {
    type DynamicType = ApiResource;

    fn group(dt: &ApiResource) -> Cow<'_, str> {
        dt.group.as_str().into()
    }

    fn version(dt: &ApiResource) -> Cow<'_, str> {
        dt.version.as_str().into()
    }

    fn kind(dt: &ApiResource) -> Cow<'_, str> {
        dt.kind.as_str().into()
    }

    fn api_version(dt: &ApiResource) -> Cow<'_, str> {
        dt.api_version.as_str().into()
    }

    fn plural(dt: &ApiResource) -> Cow<'_, str> {
        dt.plural.as_str().into()
    }

    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

#[cfg(test)]
mod test {
    use crate::{
        api_resource::ApiResource,
        dynamic::DynamicObject,
        gvk::GroupVersionKind,
        params::{Patch, PatchParams, PostParams},
        request::Request,
        resource::Resource,
        Result,
    };
    #[test]
    fn raw_custom_resource() {
        let gvk = GroupVersionKind::gvk("clux.dev", "v1", "Foo");
        let res = ApiResource::from_gvk(&gvk);
        let url = DynamicObject::url_path(&res, Some("myns"));

        let pp = PostParams::default();
        let req = Request::new(&url).create(&pp, vec![]).unwrap();
        assert_eq!(req.uri(), "/apis/clux.dev/v1/namespaces/myns/foos?");
        let patch_params = PatchParams::default();
        let req = Request::new(url)
            .patch("baz", &patch_params, &Patch::Merge(()))
            .unwrap();
        assert_eq!(req.uri(), "/apis/clux.dev/v1/namespaces/myns/foos/baz?");
        assert_eq!(req.method(), "PATCH");
    }

    #[test]
    fn raw_resource_in_default_group() -> Result<()> {
        let gvk = GroupVersionKind::gvk("", "v1", "Service");
        let api_resource = ApiResource::from_gvk(&gvk);
        let url = DynamicObject::url_path(&api_resource, None);
        let pp = PostParams::default();
        let req = Request::new(url).create(&pp, vec![])?;
        assert_eq!(req.uri(), "/api/v1/services?");
        Ok(())
    }

    #[cfg(feature = "derive")]
    #[tokio::test]
    #[ignore] // circle has no kubeconfig
    async fn convenient_custom_resource() {
        use crate as kube_core; // derive macro needs kube in scope
        use crate::{Api, Client, CustomResource};
        use schemars::JsonSchema;
        use serde::{Deserialize, Serialize};
        #[derive(Clone, Debug, CustomResource, Deserialize, Serialize, JsonSchema)]
        #[kube(group = "clux.dev", version = "v1", kind = "Foo", namespaced)]
        struct FooSpec {
            foo: String,
        }
        let client = Client::try_default().await.unwrap();

        let gvk = GroupVersionKind::gvk("clux.dev", "v1", "Foo");
        let api_resource = ApiResource::from_gvk(&gvk);
        let a1: Api<DynamicObject> = Api::namespaced_with(client.clone(), "myns", &api_resource);
        let a2: Api<Foo> = Api::namespaced(client.clone(), "myns");

        // make sure they return the same url_path through their impls
        assert_eq!(a1.request.url_path, a2.request.url_path);
    }
}
