//! Utilities for managing [`CustomResourceDefinition`] schemas
//!
//! [`CustomResourceDefinition`]: `k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition`

// Used in docs
#[allow(unused_imports)] use schemars::generate::SchemaSettings;

use schemars::{
    schema::{InstanceType, Metadata, ObjectValidation, Schema, SchemaObject, SingleOrVec},
    visit::Visitor,
    MapEntry,
};

/// schemars [`Visitor`] that rewrites a [`Schema`] to conform to Kubernetes' "structural schema" rules
///
/// The following two transformations are applied
///  * Rewrite enums from `oneOf` to `object`s with multiple variants ([schemars#84](https://github.com/GREsau/schemars/issues/84))
///  * Rewrite untagged enums from `anyOf` to `object`s with multiple variants ([kube#1028](https://github.com/kube-rs/kube/pull/1028))
///  * Rewrite `additionalProperties` from `#[serde(flatten)]` to `x-kubernetes-preserve-unknown-fields` ([kube#844](https://github.com/kube-rs/kube/issues/844))
///
/// This is used automatically by `kube::derive`'s `#[derive(CustomResource)]`,
/// but it can also be used manually with [`SchemaSettings::with_visitor`].
///
/// # Panics
///
/// The [`Visitor`] functions may panic if the transform could not be applied. For example,
/// there must not be any overlapping properties between `oneOf` branches.
#[derive(Debug, Clone)]
pub struct StructuralSchemaRewriter;

impl Visitor for StructuralSchemaRewriter {
    fn visit_schema_object(&mut self, schema: &mut schemars::schema::SchemaObject) {
        schemars::visit::visit_schema_object(self, schema);

        if let Some(subschemas) = &mut schema.subschemas {
            if let Some(one_of) = subschemas.one_of.as_mut() {
                // Tagged enums are serialized using `one_of`
                hoist_subschema_properties(one_of, &mut schema.object, &mut schema.instance_type);

                // "Plain" enums are serialized using `one_of` if they have doc tags
                hoist_subschema_enum_values(one_of, &mut schema.enum_values, &mut schema.instance_type);

                if one_of.is_empty() {
                    subschemas.one_of = None;
                }
            }

            if let Some(any_of) = &mut subschemas.any_of {
                // Untagged enums are serialized using `any_of`
                hoist_subschema_properties(any_of, &mut schema.object, &mut schema.instance_type);
            }
        }

        // check for maps without with properties (i.e. flattened maps)
        // and allow these to persist dynamically
        if let Some(object) = &mut schema.object {
            if !object.properties.is_empty()
                && object.additional_properties.as_deref() == Some(&Schema::Bool(true))
            {
                object.additional_properties = None;
                schema
                    .extensions
                    .insert("x-kubernetes-preserve-unknown-fields".into(), true.into());
            }
        }

        // As of version 1.30 Kubernetes does not support setting `uniqueItems` to `true`,
        // so we need to remove this fields.
        // Users can still set `x-kubernetes-list-type=set` in case they want the apiserver
        // to do validation, but we can't make an assumption about the Set contents here.
        // See https://kubernetes.io/docs/reference/using-api/server-side-apply/ for details.
        if let Some(array) = &mut schema.array {
            array.unique_items = None;
        }
    }
}

/// Bring all plain enum values up to the root schema,
/// since Kubernetes doesn't allow subschemas to define enum options.
///
/// (Enum here means a list of hard-coded values, not a tagged union.)
fn hoist_subschema_enum_values(
    subschemas: &mut Vec<Schema>,
    common_enum_values: &mut Option<Vec<serde_json::Value>>,
    instance_type: &mut Option<SingleOrVec<InstanceType>>,
) {
    subschemas.retain(|variant| {
        if let Schema::Object(SchemaObject {
            instance_type: variant_type,
            enum_values: Some(variant_enum_values),
            ..
        }) = variant
        {
            if let Some(variant_type) = variant_type {
                match instance_type {
                    None => *instance_type = Some(variant_type.clone()),
                    Some(tpe) => {
                        if tpe != variant_type {
                            panic!("Enum variant set {variant_enum_values:?} has type {variant_type:?} but was already defined as {instance_type:?}. The instance type must be equal for all subschema variants.")
                        }
                    }
                }
            }
            common_enum_values
                .get_or_insert_with(Vec::new)
                .extend(variant_enum_values.iter().cloned());
            false
        } else {
            true
        }
    })
}

/// Bring all property definitions from subschemas up to the root schema,
/// since Kubernetes doesn't allow subschemas to define properties.
fn hoist_subschema_properties(
    subschemas: &mut Vec<Schema>,
    common_obj: &mut Option<Box<ObjectValidation>>,
    instance_type: &mut Option<SingleOrVec<InstanceType>>,
) {
    for variant in subschemas {
        if let Schema::Object(SchemaObject {
            instance_type: variant_type,
            object: Some(variant_obj),
            metadata: variant_metadata,
            ..
        }) = variant
        {
            let common_obj = common_obj.get_or_insert_with(Box::<ObjectValidation>::default);

            if let Some(variant_metadata) = variant_metadata {
                // Move enum variant description from oneOf clause to its corresponding property
                if let Some(description) = std::mem::take(&mut variant_metadata.description) {
                    if let Some(Schema::Object(variant_object)) =
                        only_item(variant_obj.properties.values_mut())
                    {
                        let metadata = variant_object
                            .metadata
                            .get_or_insert_with(Box::<Metadata>::default);
                        metadata.description = Some(description);
                    }
                }
            }

            // Move all properties
            let variant_properties = std::mem::take(&mut variant_obj.properties);
            for (property_name, property) in variant_properties {
                match common_obj.properties.entry(property_name) {
                    MapEntry::Vacant(entry) => {
                        entry.insert(property);
                    }
                    MapEntry::Occupied(entry) => {
                        if &property != entry.get() {
                            panic!("Property {:?} has the schema {:?} but was already defined as {:?} in another subschema. The schemas for a property used in multiple subschemas must be identical",
                            entry.key(),
                            &property,
                            entry.get());
                        }
                    }
                }
            }

            // Kubernetes doesn't allow variants to set additionalProperties
            variant_obj.additional_properties = None;

            merge_metadata(instance_type, variant_type.take());
        }
    }
}

fn only_item<I: Iterator>(mut i: I) -> Option<I::Item> {
    let item = i.next()?;
    if i.next().is_some() {
        return None;
    }
    Some(item)
}

fn merge_metadata(
    instance_type: &mut Option<SingleOrVec<InstanceType>>,
    variant_type: Option<SingleOrVec<InstanceType>>,
) {
    match (instance_type, variant_type) {
        (_, None) => {}
        (common_type @ None, variant_type) => {
            *common_type = variant_type;
        }
        (Some(common_type), Some(variant_type)) => {
            if *common_type != variant_type {
                panic!(
                    "variant defined type {variant_type:?}, conflicting with existing type {common_type:?}"
                );
            }
        }
    }
}
