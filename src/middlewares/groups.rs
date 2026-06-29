use super::IAMService_config::IAMService_config;
use ginger_shared_rs::rocket_utils::Claims;
use rocket::http::Status;
use rocket::request::{FromRequest, Outcome, Request};
use rocket_okapi::request::OpenApiFromRequest;
use rocket_okapi::request::RequestHeaderInput;
use rocket_okapi::OpenApiError;
use serde::{Deserialize, Serialize};
use IAMService::apis::default_api::identity_get_group_memberships;

#[derive(Debug, Serialize, Deserialize)]
pub struct GroupMemberships(pub Vec<String>);

impl GroupMemberships {
    pub fn new(groups: Vec<String>) -> Self {
        GroupMemberships(groups)
    }
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for GroupMemberships {
    type Error = ();

    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let claims = request.guard::<Claims>().await;

        let iam_service_config = request.guard::<IAMService_config>().await;

        if let (Outcome::Success(claims), Outcome::Success(openapi_config)) =
            (claims, iam_service_config)
        {
            // Here you would proxy to the IAM service
            match identity_get_group_memberships(&openapi_config.0).await {
                Ok(groups) => Outcome::Success(GroupMemberships::new(groups)),
                Err(_) => Outcome::Error((Status::InternalServerError, ())),
            }
        } else {
            Outcome::Error((Status::Unauthorized, ()))
        }
    }
}

// Implement OpenApiFromRequest for GroupMemberships
impl<'a> OpenApiFromRequest<'a> for GroupMemberships {
    fn from_request_input(
        _gen: &mut rocket_okapi::gen::OpenApiGenerator,
        _name: String,
        _required: bool,
    ) -> Result<RequestHeaderInput, OpenApiError> {
        Ok(RequestHeaderInput::None)
    }
}
