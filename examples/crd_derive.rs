use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::{
    core::object::{HasSpec, HasStatus},
    CustomResource, CustomResourceExt, Resource,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Our spec for Foo
///
/// A struct with our chosen Kind will be created for us, using the following kube attrs
#[derive(CustomResource, Serialize, Deserialize, Default, Debug, PartialEq, Eq, Clone, JsonSchema)]
#[kube(
    group = "clux.dev",
    version = "v1",
    kind = "Foo",
    plural = "fooz",
    root = "FooCrd",
    namespaced,
    doc = "Custom resource representing a Foo",
    status = "FooStatus",
    derive = "PartialEq",
    derive = "Default",
    shortname = "f",
    scale(
        spec_replicas_path = ".spec.replicas",
        status_replicas_path = ".status.replicas"
    ),
    printcolumn = r#"{"name":"Spec", "type":"string", "description":"name of foo", "jsonPath":".spec.name"}"#,
    selectable = "spec.name"
)]
pub struct MyFoo {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    info: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct FooStatus {
    is_bad: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(schema_with = "conditions")]
    pub conditions: Vec<Condition>,
}

fn main() {
    tracing_subscriber::fmt::init();
    println!("Kind {}", FooCrd::kind(&()));
    let mut foo = FooCrd::new("hi", MyFoo {
        name: "hi".into(),
        info: None,
    });
    foo.status = Some(FooStatus {
        is_bad: true,
        conditions: vec![],
    });
    println!("Spec: {:?}", foo.spec);
    let crd = serde_json::to_string_pretty(&FooCrd::crd()).unwrap();
    println!("Foo CRD: \n{crd}");

    println!("Spec (via HasSpec): {:?}", foo.spec());
    println!("Status (via HasStatus): {:?}", foo.status());
}

fn conditions(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    serde_json::from_value(serde_json::json!({
        "type": "array",
        "x-kubernetes-list-type": "map",
        "x-kubernetes-list-map-keys": ["type"],
        "items": {
            "type": "object",
            "properties": {
                "lastTransitionTime": { "format": "date-time", "type": "string" },
                "message": { "type": "string" },
                "observedGeneration": { "type": "integer", "format": "int64", "default": 0 },
                "reason": { "type": "string" },
                "status": { "type": "string" },
                "type": { "type": "string" }
            },
            "required": [
                "lastTransitionTime",
                "message",
                "reason",
                "status",
                "type"
            ],
        },
    }))
    .unwrap()
}

// some tests
// Verify FooCrd::crd
#[test]
fn verify_crd() {
    let output = serde_json::json!({
      "apiVersion": "apiextensions.k8s.io/v1",
      "kind": "CustomResourceDefinition",
      "metadata": {
        "name": "fooz.clux.dev"
      },
      "spec": {
        "group": "clux.dev",
        "names": {
          "kind": "Foo",
          "plural": "fooz",
          "shortNames": ["f"],
          "singular": "foo",
          "categories": []
        },
        "scope": "Namespaced",
        "versions": [
          {
            "name": "v1",
            "served": true,
            "storage": true,
            "additionalPrinterColumns": [
              {
                "description": "name of foo",
                "jsonPath": ".spec.name",
                "name": "Spec",
                "type": "string"
              }
            ],
            "selectableFields": [{
              "jsonPath": "spec.name",
            }],
            "schema": {
              "openAPIV3Schema": {
                "description": "Custom resource representing a Foo",
                "properties": {
                  "spec": {
                    "description": "Our spec for Foo\n\nA struct with our chosen Kind will be created for us, using the following kube attrs",
                    "properties": {
                      "info": {
                        "nullable": true,
                        "type": "string"
                      },
                      "name": {
                        "type": "string"
                      }
                    },
                    "required": [
                      "name"
                    ],
                    "type": "object"
                  },
                  "status": {
                    "nullable": true,
                    "properties": {
                      "is_bad": {
                        "type": "boolean"
                      },
                      "conditions": {
                        "type": "array",
                        "x-kubernetes-list-type": "map",
                        "x-kubernetes-list-map-keys": ["type"],
                        "items": {
                          "type": "object",
                          "properties": {
                            "lastTransitionTime": { "format": "date-time", "type": "string" },
                            "message": { "type": "string" },
                            "observedGeneration": { "type": "integer", "format": "int64", "default": 0 },
                            "reason": { "type": "string" },
                            "status": { "type": "string" },
                            "type": { "type": "string" }
                          },
                          "required": [
                            "lastTransitionTime",
                            "message",
                            "reason",
                            "status",
                            "type"
                          ],
                        },
                      }
                    },
                    "required": [
                      "is_bad"
                    ],
                    "type": "object"
                  }
                },
                "required": [
                  "spec"
                ],
                "title": "FooCrd",
                "type": "object"
              }
            },
            "subresources": {
              "scale": {
                "specReplicasPath": ".spec.replicas",
                "statusReplicasPath": ".status.replicas"
              },
              "status": {}
            },
          }
        ]
      }
    });
    let crd = serde_json::to_value(FooCrd::crd()).unwrap();
    println!("got crd: {}", serde_yaml::to_string(&FooCrd::crd()).unwrap());
    use assert_json_diff::assert_json_include;
    assert_json_include!(actual: output, expected: crd);
}

#[test]
fn verify_resource() {
    use static_assertions::{assert_impl_all, assert_impl_one};
    assert_eq!(FooCrd::kind(&()), "Foo");
    assert_eq!(FooCrd::group(&()), "clux.dev");
    assert_eq!(FooCrd::version(&()), "v1");
    assert_eq!(FooCrd::api_version(&()), "clux.dev/v1");
    assert_impl_all!(FooCrd: Resource, Default);
    assert_impl_one!(MyFoo: JsonSchema);
}

#[tokio::test]
async fn verify_url_gen() {
    let url = FooCrd::url_path(&(), Some("myns".into()));
    assert_eq!(url, "/apis/clux.dev/v1/namespaces/myns/fooz");
}

#[test]
fn verify_default() {
    let fdef = FooCrd::default();
    let ser = serde_yaml::to_string(&fdef).unwrap();
    let exp = r#"
apiVersion: clux.dev/v1
kind: Foo
metadata: {}
spec:
  name: ''
"#;
    assert_eq!(exp.trim(), ser.trim());
}
