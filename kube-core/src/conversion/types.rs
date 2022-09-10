use crate::{Status, TypeMeta};
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

/// The `kind` field in [`TypeMeta`]
pub const META_KIND: &str = "ConversionReview";
/// The `api_version` field in [`TypeMeta`] on the v1 version
pub const META_API_VERSION_V1: &str = "apiextensions.k8s.io/v1";

#[derive(Debug, Error)]
#[error("request missing in ConversionReview")]
/// Returned when `ConversionReview` cannot be converted into `ConversionRequest`
pub struct ConvertConversionReviewError;

/// Struct that describes both request and response
#[derive(Serialize, Deserialize)]
pub struct ConversionReview {
    /// Contains the API version and type of the request
    #[serde(flatten)]
    pub types: TypeMeta,
    /// Contains conversion request
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request: Option<ConversionRequest>,
    /// Contains conversion response
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub response: Option<ConversionResponse>,
}

/// Part of ConversionReview which is set on input (i.e. generated by apiserver)
#[derive(Serialize, Deserialize)]
pub struct ConversionRequest {
    /// [`TypeMeta`] of the [`ConversionReview`] this response was created from
    ///  
    /// This field dopied from the corresponding [`ConversionReview`].
    /// It is not part of the Kubernetes API, it's consumed only by `kube`.
    #[serde(skip)]
    pub types: Option<TypeMeta>,
    /// Random uid uniquely identifying this conversion call
    pub uid: String,
    /// The API group and version the objects should be converted to
    #[serde(rename = "desiredAPIVersion")]
    pub desired_api_version: String,
    /// The list of objects to convert
    ///
    /// Note that list may contain one or more objects, in one or more versions.
    // This field uses raw Value instead of Object/DynamicObject to simplify
    // further downcasting.
    pub objects: Vec<serde_json::Value>,
}

impl ConversionRequest {
    /// Extracts request from the [`ConversionReview`]
    pub fn from_review(review: ConversionReview) -> Result<Self, ConvertConversionReviewError> {
        ConversionRequest::try_from(review)
    }
}

impl TryFrom<ConversionReview> for ConversionRequest {
    type Error = ConvertConversionReviewError;

    fn try_from(review: ConversionReview) -> Result<Self, Self::Error> {
        match review.request {
            Some(mut req) => {
                req.types = Some(review.types);
                Ok(req)
            }
            None => Err(ConvertConversionReviewError),
        }
    }
}

/// Part of ConversionReview which is set on output (i.e. generated by conversion webhook)
#[derive(Serialize, Deserialize)]
pub struct ConversionResponse {
    /// [`TypeMeta`] of the [`ConversionReview`] this response was derived from
    ///  
    /// This field is copied from the corresponding [`ConversionRequest`].
    /// It is not part of the Kubernetes API, it's consumed only by `kube`.
    #[serde(skip)]
    pub types: Option<TypeMeta>,
    /// Copy of .request.uid
    pub uid: String,
    /// Outcome of the conversion operation
    ///
    /// Success: all objects were successfully converted
    /// Failure: at least one object could not be converted.
    /// It is recommended that conversion fails as rare as possible.
    pub result: Status,
    /// Converted objects
    ///
    /// This field should contain objects in the same order as in the request
    /// Should be empty if conversion failed.
    #[serde(rename = "convertedObjects")]
    #[serde(deserialize_with = "parse_converted_objects")]
    pub converted_objects: Vec<serde_json::Value>,
}

fn parse_converted_objects<'de, D>(de: D) -> Result<Vec<serde_json::Value>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Helper {
        List(Vec<serde_json::Value>),
        Null(()),
    }

    let h: Helper = Helper::deserialize(de)?;
    let res = match h {
        Helper::List(l) => l,
        Helper::Null(()) => Vec::new(),
    };
    Ok(res)
}

impl ConversionResponse {
    /// Creates a new response, matching provided request
    ///
    /// This response must be finalized with one of:
    /// - [`ConversionResponse::success`] when conversion succeeded
    /// - [`ConversionResponse::failure`] when conversion failed
    pub fn for_request(request: ConversionRequest) -> Self {
        ConversionResponse::from(request)
    }

    /// Creates successful conversion response
    ///
    /// `converted_objects` must specify objects in the exact same order as on input.
    pub fn success(mut self, converted_objects: Vec<serde_json::Value>) -> Self {
        self.result = Status::success();
        self.converted_objects = converted_objects;
        self
    }

    /// Creates failed conversion response (discouraged)
    ///
    /// `request_uid` must be equal to the `.uid` field in the request.
    /// `message` and `reason` will be returned to the apiserver.
    pub fn failure(mut self, message: &str, reason: &str) -> Self {
        self.result = Status::failure(message, reason);
        self
    }

    /// Creates failed conversion response, not matched with any request
    ///
    /// You should only call this function when request couldn't be parsed into [`ConversionRequest`].
    /// Otherwise use `error`.
    pub fn invalid(message: &str, reason: &str) -> Self {
        ConversionResponse {
            types: None,
            uid: String::new(),
            result: Status::failure(message, reason),
            converted_objects: Vec::new(),
        }
    }

    /// Converts response into a [`ConversionReview`] value, ready to be sent as a response
    pub fn into_review(self) -> ConversionReview {
        self.into()
    }
}

impl From<ConversionRequest> for ConversionResponse {
    fn from(request: ConversionRequest) -> Self {
        ConversionResponse {
            types: request.types,
            uid: request.uid,
            result: Status {
                status: None,
                code: 0,
                message: String::new(),
                reason: String::new(),
                details: None,
            },
            converted_objects: Vec::new(),
        }
    }
}

impl From<ConversionResponse> for ConversionReview {
    fn from(mut response: ConversionResponse) -> Self {
        ConversionReview {
            types: response.types.take().unwrap_or_else(|| {
                // we don't know which uid, apiVersion and kind to use, let's just use something
                TypeMeta {
                    api_version: META_API_VERSION_V1.to_string(),
                    kind: META_KIND.to_string(),
                }
            }),
            request: None,
            response: Some(response),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ConversionRequest, ConversionResponse};

    #[test]
    fn simple_request_parses() {
        // this file contains dump of real request generated by kubernetes v1.22
        let data = include_str!("./test_data/simple.json");
        // check that we can parse this review, and all chain of conversion worls
        let review = serde_json::from_str(data).unwrap();
        let req = ConversionRequest::from_review(review).unwrap();
        let res = ConversionResponse::for_request(req);
        let _ = res.into_review();
    }
}
