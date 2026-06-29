
use rocket::serde::{Deserialize, Serialize};
use schemars::JsonSchema;

/// Body for POST /start/<bucket_path>/<filename>
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(crate = "rocket::serde")]
pub struct StartUploadRequest {
    /// If false (default) and the target file already exists, /start returns 409.
    /// If true, an existing file at the target path will be truncated and overwritten
    /// once the first chunk lands.
    #[serde(default)]
    pub overwrite: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(crate = "rocket::serde")]
pub struct StartUploadResponse {
    pub upload_id: String,
    /// Seconds until this upload session expires if no chunk is received.
    pub expires_in_seconds: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(crate = "rocket::serde")]
pub struct UploadPartResponse {
    pub upload_id: String,
    /// The part number that was just accepted.
    pub part_number: u64,
    /// The part number the server now expects next.
    pub next_part_number: u64,
    pub bytes_received_total: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(crate = "rocket::serde")]
pub struct CreateUploadResponse {
    pub message: String,
    pub bucket_path: String,
    pub filename: String,
    pub total_parts: u64,
    pub total_bytes: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(crate = "rocket::serde")]
pub struct ErrorResponse {
    pub error: String,
}