//! A crate for kube's derive macros.
#![recursion_limit = "1024"]
extern crate proc_macro;
#[macro_use] extern crate quote;

mod cel_schema;
mod custom_resource;
mod resource;

/// A custom derive for kubernetes custom resource definitions.
///
/// This will generate a **root object** containing your spec and metadata.
/// This root object will implement the [`kube::Resource`] trait
/// so it can be used with [`kube::Api`].
///
/// The generated type will also implement kube's [`kube::CustomResourceExt`] trait to generate the crd
/// and generate [`kube::core::ApiResource`] information for use with the dynamic api.
///
/// # Example
///
/// ```rust
/// use serde::{Serialize, Deserialize};
/// use kube::core::{Resource, CustomResourceExt};
/// use kube_derive::CustomResource;
/// use schemars::JsonSchema;
///
/// #[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
/// #[kube(group = "clux.dev", version = "v1", kind = "Foo", namespaced)]
/// struct FooSpec {
///     info: String,
/// }
///
/// println!("kind = {}", Foo::kind(&())); // impl kube::Resource
/// let f = Foo::new("foo-1", FooSpec {
///     info: "informative info".into(),
/// });
/// println!("foo: {:?}", f); // debug print on root type
/// println!("crd: {}", serde_yaml::to_string(&Foo::crd()).unwrap()); // crd yaml
/// ```
///
/// This example generates a `struct Foo` containing metadata, the spec,
/// and optionally status. The **root** struct `Foo` can be used with the [`kube`] crate
/// as an `Api<Foo>` object (`FooSpec` can not be used with [`Api`][`kube::Api`]).
///
/// ```no_run
///  # use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
///  # use kube_derive::CustomResource;
///  # use kube::{api::{Api, Patch, PatchParams}, Client, CustomResourceExt};
///  # use serde::{Deserialize, Serialize};
///  # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
///  # #[derive(CustomResource, Clone, Debug, Deserialize, Serialize, schemars::JsonSchema)]
///  # #[kube(group = "clux.dev", version = "v1", kind = "Foo", namespaced)]
///  # struct FooSpec {}
///  # let client: Client = todo!();
///  let foos: Api<Foo> = Api::default_namespaced(client.clone());
///  let crds: Api<CustomResourceDefinition> = Api::all(client.clone());
///  crds.patch("foos.clux.dev", &PatchParams::apply("myapp"), &Patch::Apply(Foo::crd())).await;
/// # Ok(())
/// # }
///  ```
///
/// This example posts the generated `::crd` to the `CustomResourceDefinition` API.
/// After this has been accepted (few secs max), you can start using `foos` as a normal
/// kube `Api` object. See the `crd_` prefixed [examples](https://github.com/kube-rs/kube/blob/main/examples/)
/// for details on this.
///
/// # Required properties
///
/// ## `#[kube(group = "mygroup.tld")]`
/// Your cr api group. The part before the slash in the top level `apiVersion` key.
///
/// ## `#[kube(version = "v1")]`
/// Your cr api version. The part after the slash in the top level `apiVersion` key.
///
/// ## `#[kube(kind = "Kind")]`
/// Name of your kind, and implied default for your generated root type.
///
/// # Optional `#[kube]` attributes
///
/// ## `#[kube(singular = "nonstandard-singular")]`
/// To specify the singular name. Defaults to lowercased `.kind` value.
///
/// ## `#[kube(plural = "nonstandard-plural")]`
/// To specify the plural name. Defaults to inferring from singular.
///
/// ## `#[kube(namespaced)]`
/// To specify that this is a namespaced resource rather than cluster level.
///
/// ## `#[kube(root = "StructName")]`
/// Customize the name of the generated root struct (defaults to `.kind` value).
///
/// ## `#[kube(crates(kube_core = "::kube::core"))]`
/// Customize the crate name the generated code will reach into (defaults to `::kube::core`).
/// Should be one of `kube::core`, `kube_client::core` or `kube_core`.
///
/// ## `#[kube(crates(k8s_openapi = "::k8s_openapi"))]`
/// Customize the crate name the generated code will use for [`k8s_openapi`](https://docs.rs/k8s-openapi/) (defaults to `::k8s_openapi`).
///
/// ## `#[kube(crates(schemars = "::schemars"))]`
/// Customize the crate name the generated code will use for [`schemars`](https://docs.rs/schemars/) (defaults to `::schemars`).
///
/// ## `#[kube(crates(serde = "::serde"))]`
/// Customize the crate name the generated code will use for [`serde`](https://docs.rs/serde/) (defaults to `::serde`).
///
/// ## `#[kube(crates(serde_json = "::serde_json"))]`
/// Customize the crate name the generated code will use for [`serde_json`](https://docs.rs/serde_json/) (defaults to `::serde_json`).
///
/// ## `#[kube(status = "StatusStructName")]`
/// Adds a status struct to the top level generated type and enables the status
/// subresource in your crd.
///
/// ## `#[kube(derive = "Trait")]`
/// Adding `#[kube(derive = "PartialEq")]` is required if you want your generated
/// top level type to be able to `#[derive(PartialEq)]`
///
/// ## `#[kube(schema = "mode")]`
/// Defines whether the `JsonSchema` of the top level generated type should be used when generating a `CustomResourceDefinition`.
///
/// Legal values:
/// - `"derived"`: A `JsonSchema` implementation is automatically derived
/// - `"manual"`: `JsonSchema` is not derived, but used when creating the `CustomResourceDefinition` object
/// - `"disabled"`: No `JsonSchema` is used
///
/// This can be used to provide a completely custom schema, or to interact with third-party custom resources
/// where you are not responsible for installing the `CustomResourceDefinition`.
///
/// Defaults to `"derived"`.
///
/// NOTE: `CustomResourceDefinition`s require a schema. If `schema = "disabled"` then
/// `Self::crd()` will not be installable into the cluster as-is.
///
/// ## `#[kube(scale(...))]`
///
/// Allow customizing the scale struct for the [scale subresource](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#subresources).
/// It should be noted, that the status subresource must also be enabled to use the scale subresource. This is because
/// the `statusReplicasPath` only accepts JSONPaths under `.status`.
///
/// ```ignore
/// #[kube(scale(
///     specReplicasPath = ".spec.replicas",
///     statusReplicaPath = ".status.replicas",
///     labelSelectorPath = ".spec.labelSelector"
/// ))]
/// ```
///
/// The deprecated way of customizing the scale subresource using a raw JSON string is still
/// support for backwards-compatibility.
///
/// ## `#[kube(printcolumn = r#"json"#)]`
/// Allows adding straight json to [printcolumns](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#additional-printer-columns).
///
/// ## `#[kube(shortname = "sn")]`
/// Add a single shortname to the generated crd.
///
/// ## `#[kube(category = "apps")]`
/// Add a single category to `crd.spec.names.categories`.
///
/// ## `#[kube(selectable = "fieldSelectorPath")]`
/// Adds a Kubernetes >=1.30 `selectableFields` property ([KEP-4358](https://github.com/kubernetes/enhancements/blob/master/keps/sig-api-machinery/4358-custom-resource-field-selectors/README.md)) to the schema.
/// Unlocks `kubectl get kind --field-selector fieldSelectorPath`.
///
/// ## `#[kube(doc = "description")]`
/// Sets the description of the schema in the generated CRD. If not specified
/// `Auto-generated derived type for {customResourceName} via CustomResource` will be used instead.
///
/// ## `#[kube(annotation("ANNOTATION_KEY", "ANNOTATION_VALUE"))]`
/// Add a single annotation to the generated CRD.
///
/// ## `#[kube(label("LABEL_KEY", "LABEL_VALUE"))]`
/// Add a single label to the generated CRD.
///
/// ## `#[kube(storage = true)]`
/// Sets the `storage` property to `true` or `false`.
///
/// ## `#[kube(served = true)]`
/// Sets the `served` property to `true` or `false`.
///
/// ## `#[kube(rule = Rule::new("self == oldSelf").message("field is immutable"))]`
/// Inject a top level CEL validation rule for the top level generated struct.
/// This attribute is for resources deriving [`CELSchema`] instead of [`schemars::JsonSchema`].
///
/// ## Example with all properties
///
/// ```rust
/// use serde::{Serialize, Deserialize};
/// use kube_derive::CustomResource;
/// use schemars::JsonSchema;
///
/// #[derive(CustomResource, Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
/// #[kube(
///     group = "clux.dev",
///     version = "v1",
///     kind = "Foo",
///     root = "FooCrd",
///     namespaced,
///     doc = "Custom resource representing a Foo",
///     status = "FooStatus",
///     derive = "PartialEq",
///     singular = "foot",
///     plural = "feetz",
///     shortname = "f",
///     scale = r#"{"specReplicasPath":".spec.replicas", "statusReplicasPath":".status.replicas"}"#,
///     printcolumn = r#"{"name":"Spec", "type":"string", "description":"name of foo", "jsonPath":".spec.name"}"#,
///     selectable = "spec.replicasCount"
/// )]
/// #[serde(rename_all = "camelCase")]
/// struct FooSpec {
///     #[schemars(length(min = 3))]
///     data: String,
///     replicas_count: i32
/// }
///
/// #[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
/// struct FooStatus {
///     replicas: i32
/// }
/// ```
///
/// # Enums
///
/// Kubernetes requires that the generated [schema is "structural"](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#specifying-a-structural-schema).
/// This means that the structure of the schema must not depend on the particular values. For enums this imposes a few limitations:
///
/// - Only [externally tagged enums](https://serde.rs/enum-representations.html#externally-tagged) are supported
/// - Unit variants may not be mixed with struct or tuple variants (`enum Foo { Bar, Baz {}, Qux() }` is invalid, for example)
///
/// If these restrictions are not followed then `YourCrd::crd()` may panic, or the Kubernetes API may reject the CRD definition.
///
/// # Generated code
///
/// The example above will **roughly** generate:
/// ```compile_fail
/// #[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
/// #[serde(rename_all = "camelCase")]
/// pub struct FooCrd {
///     api_version: String,
///     kind: String,
///     metadata: ObjectMeta,
///     spec: FooSpec,
///     status: Option<FooStatus>,
/// }
/// impl kube::Resource for FooCrd { .. }
///
/// impl FooCrd {
///     pub fn new(name: &str, spec: FooSpec) -> Self { .. }
///     pub fn crd() -> CustomResourceDefinition { .. }
/// }
/// ```
///
/// # Customizing Schemas
/// Should you need to customize the schemas, you can use:
/// - [Serde/Schemars Attributes](https://graham.cool/schemars/examples/3-schemars_attrs/) (no need to duplicate serde renames)
/// - [`#[schemars(schema_with = "func")]`](https://graham.cool/schemars/examples/7-custom_serialization/) (e.g. like in the [`crd_derive` example](https://github.com/kube-rs/kube/blob/main/examples/crd_derive.rs))
/// - `impl JsonSchema` on a type / newtype around external type. See [#129](https://github.com/kube-rs/kube/issues/129#issuecomment-750852916)
/// - [`#[garde(...)]` field attributes for client-side validation](https://github.com/jprochazk/garde) (see [`crd_api` example](https://github.com/kube-rs/kube/blob/main/examples/crd_api.rs))
///
/// You might need to override parts of the schemas (for fields in question) when you are:
/// - **using complex enums**: enums do not currently generate [structural schemas](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#specifying-a-structural-schema), so kubernetes won't support them by default
/// - **customizing [merge-strategies](https://kubernetes.io/docs/reference/using-api/server-side-apply/#merge-strategy)** (e.g. like in the [`crd_derive_schema` example](https://github.com/kube-rs/kube/blob/main/examples/crd_derive_schema.rs))
///
/// See [kubernetes openapi validation](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#validation) for the format of the OpenAPI v3 schemas.
///
/// If you have to override a lot, [you can opt-out of schema-generation entirely](#kubeschema--mode)
///
/// # Advanced Features
///
/// - **embedding k8s-openapi types** can be done by enabling the `schemars` feature of `k8s-openapi` from [`0.13.0`](https://github.com/Arnavion/k8s-openapi/blob/master/CHANGELOG.md#v0130-2021-08-09)
/// - **adding validation** via [validator crate](https://github.com/Keats/validator) is supported from `schemars` >= [`0.8.5`](https://github.com/GREsau/schemars/blob/master/CHANGELOG.md#085---2021-09-20)
/// - **generating rust code from schemas** can be done via [kopium](https://github.com/kube-rs/kopium) and is supported on stable crds (> 1.16 kubernetes)
///
/// ## Schema Validation
/// There are two main ways of doing validation; **server-side** (embedding validation attributes into the schema for the apiserver to respect), and **client-side** (provides `validate()` methods in your code).
///
/// Client side validation of structs can be achieved by hooking up `#[garde]` attributes in your struct and is a replacement of the now unmaintained [`validator`](https://github.com/Keats/validator/issues/201) crate.
/// Server-side validation require mutation of your generated schema, and can in the basic cases be achieved through the use of `schemars`'s [validation attributes](https://graham.cool/schemars/deriving/attributes/#supported-validator-attributes).
/// For complete control, [parts of the schema can be overridden](https://github.com/kube-rs/kube/blob/e01187e13ba364ccecec452e023316a62fb13e04/examples/crd_derive.rs#L37-L38) to support more advanced [Kubernetes specific validation rules](https://kubernetes.io/blog/2022/09/23/crd-validation-rules-beta/).
///
/// When using `garde` directly, you must add it to your dependencies (with the `derive` feature).
///
/// ### Validation Caveats
/// Make sure your validation rules are static and handled by `schemars`:
/// - validations from `#[garde(custom(my_func))]` will not show up in the schema.
/// - similarly; [nested / must_match / credit_card were unhandled by schemars at time of writing](https://github.com/GREsau/schemars/pull/78)
/// - encoding validations specified through garde (i.e. #[garde(ascii)]), are currently not supported by schemars
/// - to validate required attributes client-side, garde requires a custom validation function (`#[garde(custom(my_required_check))]`)
/// - when using garde, fields that should not be validated need to be explictly skipped through the `#[garde(skip)]` attr
///
/// For sanity, you should review the generated schema before sending it to kubernetes.
///
/// ## Versioning
/// Note that any changes to your struct / validation rules / serialization attributes will require you to re-apply the
/// generated schema to kubernetes, so that the apiserver can validate against the right version of your structs.
///
/// **Backwards compatibility** between schema versions is **recommended** unless you are in a controlled environment
/// where you can migrate manually. I.e. if you add new properties behind options, and simply mark old fields as deprecated,
/// then you can safely roll schema out changes **without bumping** the version.
///
/// If you need **multiple versions**, then you need:
///
/// - one **module** for **each version** of your types (e.g. `v1::MyCrd` and `v2::MyCrd`)
/// - use the [`merge_crds`](https://docs.rs/kube/latest/kube/core/crd/fn.merge_crds.html) fn to combine crds
/// - roll out new schemas utilizing conversion webhooks / manual conversions / or allow kubectl to do its best
///
/// See the [crd_derive_multi](https://github.com/kube-rs/kube/blob/main/examples/crd_derive_multi.rs) example to see
/// how this upgrade flow works without special logic.
///
/// The **upgrade flow** with **breaking changes** involves:
///
/// 1. upgrade version marked as `storage` (from v1 to v2)
/// 2. read instances from the older `Api<v1::MyCrd>`
/// 3. perform conversion in memory and write them to the new `Api<v2::MyCrd>`.
/// 4. remove support for old version
///
/// If you need to maintain support for the old version for some time, then you have to repeat or continuously
/// run steps 2 and 3. I.e. you probably need a **conversion webhook**.
///
/// **NB**: kube does currently [not implement conversion webhooks yet](https://github.com/kube-rs/kube/issues/865).
///
/// ## Debugging
/// Try `cargo-expand` to see your own macro expansion.
///
/// # Installation
/// Enable the `derive` feature on the `kube` crate:
///
/// ```toml
/// kube = { version = "...", features = ["derive"] }
/// ```
///
/// ## Runtime dependencies
/// Due to [rust-lang/rust#54363](https://github.com/rust-lang/rust/issues/54363), we cannot be resilient against crate renames within our generated code.
/// It's therefore **required** that you have the following crates in scope, not renamed:
///
/// - `serde_json`
/// - `k8s_openapi`
/// - `schemars` (by default, unless `schema` feature disabled)
///
/// You are ultimately responsible for maintaining the versions and feature flags of these libraries.
///
/// [`kube`]: https://docs.rs/kube
/// [`kube::Api`]: https://docs.rs/kube/*/kube/struct.Api.html
/// [`kube::Resource`]: https://docs.rs/kube/*/kube/trait.Resource.html
/// [`kube::core::ApiResource`]: https://docs.rs/kube/*/kube/core/struct.ApiResource.html
/// [`kube::CustomResourceExt`]: https://docs.rs/kube/*/kube/trait.CustomResourceExt.html
#[proc_macro_derive(CustomResource, attributes(kube))]
pub fn derive_custom_resource(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    custom_resource::derive(proc_macro2::TokenStream::from(input)).into()
}

/// Generates a JsonSchema implementation a set of CEL validation rules applied on the CRD.
///
/// ```rust
/// use kube::CELSchema;
/// use kube::CustomResource;
/// use serde::Deserialize;
/// use serde::Serialize;
/// use kube::core::crd::CustomResourceExt;
///
/// #[derive(CustomResource, CELSchema, Serialize, Deserialize, Clone, Debug)]
/// #[kube(
///     group = "kube.rs",
///     version = "v1",
///     kind = "Struct",
///     rule = Rule::new("self.matadata.name == 'singleton'"),
/// )]
/// #[cel_validate(rule = Rule::new("self == oldSelf"))]
/// struct MyStruct {
///     #[serde(default = "default")]
///     #[cel_validate(rule = Rule::new("self != ''").message("failure message"))]
///     field: String,
/// }
///
/// fn default() -> String {
///     "value".into()
/// }
///
/// assert!(serde_json::to_string(&Struct::crd()).unwrap().contains("x-kubernetes-validations"));
/// assert!(serde_json::to_string(&Struct::crd()).unwrap().contains(r#""rule":"self == oldSelf""#));
/// assert!(serde_json::to_string(&Struct::crd()).unwrap().contains(r#""rule":"self != ''""#));
/// assert!(serde_json::to_string(&Struct::crd()).unwrap().contains(r#""message":"failure message""#));
/// assert!(serde_json::to_string(&Struct::crd()).unwrap().contains(r#""default":"value""#));
/// assert!(serde_json::to_string(&Struct::crd()).unwrap().contains(r#""rule":"self.matadata.name == 'singleton'""#));
/// ```
#[proc_macro_derive(CELSchema, attributes(cel_validate, schemars))]
pub fn derive_schema_validation(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    cel_schema::derive_validated_schema(input.into()).into()
}

/// A custom derive for inheriting Resource impl for the type.
///
/// This will generate a [`kube::Resource`] trait implementation,
/// inheriting from a specified resource trait implementation.
///
/// This allows strict typing to some typical resources like `Secret` or `ConfigMap`,
/// in cases when implementing CRD is not desirable or it does not fit the use-case.
///
/// Once derived, the type can be used with [`kube::Api`].
///
/// # Example
///
/// ```rust,no_run
/// use kube::api::ObjectMeta;
/// use k8s_openapi::api::core::v1::ConfigMap;
/// use kube_derive::Resource;
/// use kube::Client;
/// use kube::Api;
/// use serde::Deserialize;
///
/// #[derive(Resource, Clone, Debug, Deserialize)]
/// #[resource(inherit = "ConfigMap")]
/// struct FooMap {
///     metadata: ObjectMeta,
///     data: Option<FooMapSpec>,
/// }
///
/// #[derive(Clone, Debug, Deserialize)]
/// struct FooMapSpec {
///     field: String,
/// }
///
/// let client: Client = todo!();
/// let api: Api<FooMap> = Api::default_namespaced(client);
/// let config_map = api.get("with-field");
/// ```
///
/// The example above will generate:
/// ```
/// // impl kube::Resource for FooMap { .. }
/// ```
/// [`kube`]: https://docs.rs/kube
/// [`kube::Api`]: https://docs.rs/kube/*/kube/struct.Api.html
/// [`kube::Resource`]: https://docs.rs/kube/*/kube/trait.Resource.html
/// [`kube::core::ApiResource`]: https://docs.rs/kube/*/kube/core/struct.ApiResource.html
/// [`kube::CustomResourceExt`]: https://docs.rs/kube/*/kube/trait.CustomResourceExt.html
#[proc_macro_derive(Resource, attributes(resource))]
pub fn derive_resource(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    resource::derive(proc_macro2::TokenStream::from(input)).into()
}
