use ginger_shared_rs::rocket_models::MessageResponse;
use rocket::serde::json::Json;
use rocket::State;
use rocket_okapi::openapi;


#[openapi()]
#[get("/")]
pub fn index() -> Json<MessageResponse> {
    Json(MessageResponse {
        message:"ok".to_string(),
    })
}

