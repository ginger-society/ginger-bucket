
                        use okapi::openapi3::{Object, SecurityRequirement, SecurityScheme, SecuritySchemeData};
use rocket::http::Status;
use rocket::request::{FromRequest, Outcome, Request};
use rocket_okapi::gen::OpenApiGenerator;
use rocket_okapi::request::{OpenApiFromRequest, RequestHeaderInput};
use IAMService::apis::configuration::{ApiKey, Configuration}; // Adjust based on your crate structure
use IAMService::get_configuration; // Assuming get_configuration exists and returns Configuration

#[derive(Debug)]
pub struct IAMService_config(pub Configuration); // Wrapper struct for Configuration

#[rocket::async_trait]
impl<'r> FromRequest<'r> for IAMService_config {
    type Error = ();

    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let keys: Vec<_> = request.headers().get("Authorization").collect();
        if keys.len() != 1 {
            return Outcome::Error((Status::Unauthorized, ()));
        }

        let token_str = keys[0].trim_start_matches("Bearer ").trim().to_string();
        let mut configuration = get_configuration(); // Assuming Configuration::new or get_configuration exists

        // Assuming Configuration has a method to set api_key
        configuration.api_key = Some(ApiKey {
            key: token_str,
            prefix: None,
        });

        Outcome::Success(IAMService_config(configuration))
    }
}

impl<'a> OpenApiFromRequest<'a> for IAMService_config {
    fn from_request_input(
        _gen: &mut OpenApiGenerator,
        _name: String,
        _required: bool,
    ) -> rocket_okapi::Result<RequestHeaderInput> {
        let security_scheme = SecurityScheme {
            description: Some("Requires a Bearer token to access".to_owned()),
            data: SecuritySchemeData::ApiKey {
                name: "Authorization".to_owned(),
                location: "header".to_owned(),
            },
            extensions: Object::default(),
        };

        let mut security_req = SecurityRequirement::new();
        security_req.insert("BearerAuth".to_owned(), Vec::new());

        Ok(RequestHeaderInput::Security(
            "BearerAuth".to_owned(),
            security_scheme,
            security_req,
        ))
    }

    fn get_responses(
        _gen: &mut rocket_okapi::gen::OpenApiGenerator,
    ) -> rocket_okapi::Result<okapi::openapi3::Responses> {
        Ok(okapi::openapi3::Responses::default())
    }
}
