use rocket::form::Form;
use rocket::fs::TempFile;
use rocket::http::Status;
use rocket::response::status;
use rocket::serde::json::Json;
use rocket::State;
use rocket_okapi::openapi;
use std::path::Path;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::db::redis::RedisPool;
use crate::models::upload_models::{
    CreateUploadResponse, ErrorResponse, StartUploadRequest, StartUploadResponse,
    UploadPartResponse,
};
use crate::upload_rabbit::{publish_upload_complete, UploadRabbitPoolRef};
use crate::upload_session::{self, UploadSessionError, UploadSessionInfo, UPLOAD_SESSION_TTL_SECONDS};

/// Max size accepted per chunk. Documented as 5MB; allow modest headroom
/// for multipart overhead, reject anything that suggests the client isn't
/// actually chunking.
const MAX_CHUNK_BYTES: u64 = 8 * 1024 * 1024;

fn err_to_status(e: UploadSessionError) -> status::Custom<Json<ErrorResponse>> {
    match e {
        UploadSessionError::NotFound => status::Custom(
            Status::NotFound,
            Json(ErrorResponse {
                error: "Upload session not found or has expired".to_string(),
            }),
        ),
        UploadSessionError::PathLocked => status::Custom(
            Status::Conflict,
            Json(ErrorResponse {
                error: "Another upload is already in progress for this path. Retry shortly."
                    .to_string(),
            }),
        ),
        UploadSessionError::Redis(err) => status::Custom(
            Status::ServiceUnavailable,
            Json(ErrorResponse {
                error: format!("Redis error: {}", err),
            }),
        ),
        UploadSessionError::InvalidKeyEncoding => status::Custom(
            Status::InternalServerError,
            Json(ErrorResponse {
                error: "Internal error decoding upload session".to_string(),
            }),
        ),
    }
}

/// POST /start/<bucket_path>/<filename>
///
/// `bucket_path` may contain slashes; `filename` is a single segment.
///
/// Claims the path atomically (first writer wins — see try_claim_path).
/// Any concurrent /start for the same path while a lock is live gets 409
/// immediately, no race window. This service does not support multiple
/// concurrent uploads to the same path, by design: a failed/cancelled
/// upload is expected to free up via the ~2 minute TTL and the client
/// retries shortly after, rather than the server trying to arbitrate or
/// resume in-flight uploads.
#[openapi()]
#[post("/start/<bucket_path>/<filename>", data = "<body>")]
pub async fn start_upload(
    bucket_path: String,
    filename: String,
    body: Json<StartUploadRequest>,
    redis_pool: &State<RedisPool>,
) -> Result<Json<StartUploadResponse>, status::Custom<Json<ErrorResponse>>> {
    let bucket_path_str = bucket_path.replace("%2F", "/").replace("%2f", "/");

    let relative_path = upload_session::safe_relative_path(&bucket_path_str, &filename)
        .map_err(|e| status::Custom(Status::BadRequest, Json(ErrorResponse { error: e })))?;

    let absolute_path = upload_session::absolute_path_for(&relative_path);

    // Existence check only matters relative to a *finished* file. It does
    // NOT protect against concurrent in-progress uploads — that's the
    // lock's job, claimed next. Order matters: we check overwrite first
    // since it's a cheap rejection that doesn't need the lock; the lock
    // claim is what actually prevents the race for everything else.
    if absolute_path.exists() && !body.overwrite {
        return Err(status::Custom(
            Status::Conflict,
            Json(ErrorResponse {
                error: format!(
                    "File already exists at '{}'. Pass \"overwrite\": true to replace it.",
                    relative_path
                ),
            }),
        ));
    }

    let upload_id = Uuid::new_v4().to_string();

    // The atomic gate. First caller to reach this for a given path wins;
    // everyone else gets PathLocked -> 409, regardless of overwrite.
    upload_session::try_claim_path(redis_pool, &relative_path, &upload_id)
        .await
        .map_err(err_to_status)?;

    // Truncate now (not lazily on first chunk) so stale bytes from a
    // previous file can't leak in if the new upload's first chunk is
    // smaller than the old file.
    if absolute_path.exists() && body.overwrite {
        if let Some(parent) = absolute_path.parent() {
            let _ = fs::create_dir_all(parent).await;
        }
        if let Err(e) = fs::File::create(&absolute_path).await {
            // Roll back the claim we just won so the path isn't stuck
            // locked for the full TTL over a filesystem error.
            let _ = upload_session::release_claim(redis_pool, &relative_path, &upload_id).await;
            return Err(status::Custom(
                Status::InternalServerError,
                Json(ErrorResponse {
                    error: format!("Failed to truncate existing file: {}", e),
                }),
            ));
        }
    }

    Ok(Json(StartUploadResponse {
        upload_id,
        expires_in_seconds: UPLOAD_SESSION_TTL_SECONDS,
    }))
}

#[derive(rocket::form::FromForm, schemars::JsonSchema)]
pub struct UploadPartForm<'r> {
    pub part_number: u64,
    #[schemars(skip)]
    pub chunk: TempFile<'r>,
}

/// POST /upload/<upload_id>
///
/// Accepts one chunk as multipart/form-data: `part_number` (u64, strictly
/// sequential starting at 0) and `chunk` (binary). Validates ordering,
/// appends to the file at its final bucket path, refreshes the session's
/// sliding TTL (~2 min from this call).
#[openapi()]
#[post("/upload/<upload_id>", data = "<form>")]
pub async fn upload_part(
    upload_id: String,
    form: Form<UploadPartForm<'_>>,
    redis_pool: &State<RedisPool>,
) -> Result<Json<UploadPartResponse>, status::Custom<Json<ErrorResponse>>> {
    let (info, _current_next): (UploadSessionInfo, u64) =
        upload_session::find_session(redis_pool, &upload_id)
            .await
            .map_err(err_to_status)?;

    let chunk_len = form.chunk.len();
    if chunk_len > MAX_CHUNK_BYTES {
        return Err(status::Custom(
            Status::PayloadTooLarge,
            Json(ErrorResponse {
                error: format!(
                    "Chunk size {} bytes exceeds max allowed {} bytes",
                    chunk_len, MAX_CHUNK_BYTES
                ),
            }),
        ));
    }

    let advance = upload_session::try_advance_part(redis_pool, &info, form.part_number)
        .await
        .map_err(err_to_status)?;

    let new_next_part = match advance {
        Ok(new_next) => new_next,
        Err(expected) => {
            return Err(status::Custom(
                Status::Conflict,
                Json(ErrorResponse {
                    error: format!(
                        "Out-of-order chunk: expected part_number {}, got {}",
                        expected, form.part_number
                    ),
                }),
            ));
        }
    };

    let absolute_path = info.absolute_path();
    if let Some(parent) = absolute_path.parent() {
        fs::create_dir_all(parent).await.map_err(|e| {
            status::Custom(
                Status::InternalServerError,
                Json(ErrorResponse {
                    error: format!("Failed to create bucket directory: {}", e),
                }),
            )
        })?;
    }

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&absolute_path)
        .await
        .map_err(|e| {
            status::Custom(
                Status::InternalServerError,
                Json(ErrorResponse {
                    error: format!("Failed to open file for append: {}", e),
                }),
            )
        })?;

    let chunk_path = form.chunk.path().ok_or_else(|| {
        status::Custom(
            Status::InternalServerError,
            Json(ErrorResponse {
                error: "Uploaded chunk had no backing temp file".to_string(),
            }),
        )
    })?;

    let chunk_bytes = fs::read(chunk_path).await.map_err(|e| {
        status::Custom(
            Status::InternalServerError,
            Json(ErrorResponse {
                error: format!("Failed to read uploaded chunk: {}", e),
            }),
        )
    })?;

    file.write_all(&chunk_bytes).await.map_err(|e| {
        status::Custom(
            Status::InternalServerError,
            Json(ErrorResponse {
                error: format!("Failed to append chunk to file: {}", e),
            }),
        )
    })?;

    file.flush().await.map_err(|e| {
        status::Custom(
            Status::InternalServerError,
            Json(ErrorResponse {
                error: format!("Failed to flush file: {}", e),
            }),
        )
    })?;

    let total_bytes = fs::metadata(&absolute_path)
        .await
        .map(|m| m.len())
        .unwrap_or(0);

    Ok(Json(UploadPartResponse {
        upload_id,
        part_number: form.part_number,
        next_part_number: new_next_part,
        bytes_received_total: total_bytes,
    }))
}

/// POST /create/<upload_id>
///
/// Finalizes: confirms at least one chunk landed, deletes both session
/// keys (lock + reverse index) so the path is immediately free again and
/// the expiry watcher won't later mistake the finished file for orphaned,
/// then publishes a "upload complete" message to RabbitMQ for downstream
/// processing.
///
/// The publish call itself is real (declared exchange, persistent channel
/// — see upload_rabbit.rs) but the message body/schema and routing key are
/// left as TODOs since that depends on what the downstream consumer
/// expects.
#[openapi()]
#[post("/create/<upload_id>")]
pub async fn create_upload(
    upload_id: String,
    redis_pool: &State<RedisPool>,
    rabbit_pool: &State<UploadRabbitPoolRef>,
) -> Result<Json<CreateUploadResponse>, status::Custom<Json<ErrorResponse>>> {
    let (info, next_part) = upload_session::find_session(redis_pool, &upload_id)
        .await
        .map_err(err_to_status)?;

    if next_part == 0 {
        return Err(status::Custom(
            Status::BadRequest,
            Json(ErrorResponse {
                error: "No chunks have been uploaded for this upload_id yet".to_string(),
            }),
        ));
    }

    let absolute_path = info.absolute_path();
    let total_bytes = fs::metadata(&absolute_path)
        .await
        .map(|m| m.len())
        .map_err(|e| {
            status::Custom(
                Status::InternalServerError,
                Json(ErrorResponse {
                    error: format!("Uploaded file missing at finalize time: {}", e),
                }),
            )
        })?;

    upload_session::delete_session(redis_pool, &info)
        .await
        .map_err(err_to_status)?;

    // TODO: decide the real message schema (likely: bucket_path, filename,
    // total_bytes, content-type/checksum if needed) and a routing key
    // convention if downstream needs to route by upload type. Publishing a
    // placeholder body for now so the queue/exchange wiring is exercised
    // end-to-end rather than silently skipped.
    let message_body = serde_json::json!({
        "event": "upload_complete",
        "bucket_path": info.relative_path,
        "total_bytes": total_bytes,
        "total_parts": next_part,
    })
    .to_string();

    publish_upload_complete(rabbit_pool, "", &message_body).await;

    Ok(Json(CreateUploadResponse {
        message: "Upload finalized successfully".to_string(),
        bucket_path: info.relative_path.clone(),
        filename: Path::new(&info.relative_path)
            .file_name()
            .map(|f| f.to_string_lossy().to_string())
            .unwrap_or_default(),
        total_parts: next_part,
        total_bytes,
    }))
}