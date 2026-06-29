use super::IAMService_config::IAMService_config;
use ginger_shared_rs::rocket_utils::Claims;
use rocket::http::Status;
use rocket::request::{FromRequest, Outcome, Request};
use rocket_okapi::request::OpenApiFromRequest;
use rocket_okapi::request::RequestHeaderInput;
use rocket_okapi::OpenApiError;
use serde::{Deserialize, Serialize};
use IAMService::apis::default_api::identity_get_group_ownserships;

#[derive(Debug, Serialize, Deserialize)]
pub struct GroupOwnerships(pub Vec<String>);

impl GroupOwnerships {
    pub fn new(groups: Vec<String>) -> Self {
        GroupOwnerships(groups)
    }
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for GroupOwnerships {
    type Error = ();

    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let claims = request.guard::<Claims>().await;

        let iam_service_config = request.guard::<IAMService_config>().await;

        if let (Outcome::Success(claims), Outcome::Success(openapi_config)) =
            (claims, iam_service_config)
        {
            // Here you would proxy to the IAM service
            match identity_get_group_ownserships(&openapi_config.0).await {
                Ok(groups) => Outcome::Success(GroupOwnerships::new(groups)),
                Err(_) => Outcome::Error((Status::InternalServerError, ())),
            }
        } else {
            Outcome::Error((Status::Unauthorized, ()))
        }
    }
}

// Implement OpenApiFromRequest for GroupOwnerships
impl<'a> OpenApiFromRequest<'a> for GroupOwnerships {
    fn from_request_input(
        _gen: &mut rocket_okapi::gen::OpenApiGenerator,
        _name: String,
        _required: bool,
    ) -> Result<RequestHeaderInput, OpenApiError> {
        Ok(RequestHeaderInput::None)
    }
}
