use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use redis::AsyncCommands;
use std::path::{Component, Path, PathBuf};

use crate::db::redis::RedisPool;

/// How long an upload session/lock lives without activity before Redis
/// expires it and the path becomes available again. Refreshed on every
/// accepted chunk (sliding expiry). Deliberately short — this service
/// intentionally does NOT support resuming an abandoned upload; a failed
/// or cancelled client is expected to just retry shortly after expiry.
pub const UPLOAD_SESSION_TTL_SECONDS: u64 = 2 * 60;

const LOCK_PREFIX: &str = "upload:lock"; // path -> "upload_id:next_part"   (NX-guarded, the source of truth)
const INDEX_PREFIX: &str = "upload:id"; // upload_id -> relative_path      (pure lookup convenience)

/// Root directory all uploads are confined to. Reads BUCKET_ROOT from the
/// environment, falling back to "./bucket-data" if unset.
pub fn bucket_root() -> String {
    std::env::var("BUCKET_ROOT").unwrap_or_else(|_| "./bucket-data".to_string())
}

#[derive(Debug)]
pub enum UploadSessionError {
    Redis(redis::RedisError),
    NotFound,
    InvalidKeyEncoding,
    PathLocked,
}

impl From<redis::RedisError> for UploadSessionError {
    fn from(e: redis::RedisError) -> Self {
        UploadSessionError::Redis(e)
    }
}

#[derive(Debug, Clone)]
pub struct UploadSessionInfo {
    pub upload_id: String,
    /// Path relative to BUCKET_ROOT, e.g. "videos/2024/clip.mp4"
    pub relative_path: String,
}

impl UploadSessionInfo {
    pub fn absolute_path(&self) -> PathBuf {
        Path::new(&bucket_root()).join(&self.relative_path)
    }
}

fn lock_key(relative_path: &str) -> String {
    let encoded_path = URL_SAFE_NO_PAD.encode(relative_path.as_bytes());
    format!("{LOCK_PREFIX}:{encoded_path}")
}

fn index_key(upload_id: &str) -> String {
    format!("{INDEX_PREFIX}:{upload_id}")
}

/// Parses a lock key (as seen in an `expired` keyspace event) back into its
/// relative path. Returns None if the key doesn't match our scheme.
pub fn parse_lock_key(key: &str) -> Option<String> {
    let encoded_path = key.strip_prefix(&format!("{LOCK_PREFIX}:"))?;
    let decoded = URL_SAFE_NO_PAD.decode(encoded_path).ok()?;
    String::from_utf8(decoded).ok()
}

/// Joins bucket_path + filename, rejects anything that could escape
/// BUCKET_ROOT (.. , absolute paths, drive prefixes).
pub fn safe_relative_path(bucket_path: &str, filename: &str) -> Result<String, String> {
    if filename.contains('/') || filename.contains('\\') {
        return Err("filename must not contain path separators".to_string());
    }
    if filename.is_empty() {
        return Err("filename must not be empty".to_string());
    }

    let combined = format!("{}/{}", bucket_path.trim_matches('/'), filename);
    let candidate = Path::new(&combined);

    let mut normalized = PathBuf::new();
    for component in candidate.components() {
        match component {
            Component::Normal(part) => normalized.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(
                    "bucket_path/filename must not contain '..', be absolute, or use drive prefixes"
                        .to_string(),
                );
            }
        }
    }

    if normalized.as_os_str().is_empty() {
        return Err("resolved path is empty".to_string());
    }

    Ok(normalized.to_string_lossy().replace('\\', "/"))
}

fn encode_lock_value(upload_id: &str, next_part: u64) -> String {
    format!("{upload_id}:{next_part}")
}

fn decode_lock_value(value: &str) -> Option<(String, u64)> {
    let (id, part) = value.rsplit_once(':')?;
    let next_part = part.parse::<u64>().ok()?;
    Some((id.to_string(), next_part))
}

/// Attempts to claim the path for a brand-new upload. This is the single
/// atomic gate: only one caller can ever win the NX SET for a given path
/// while a lock is live. Losers get PathLocked immediately — first writer
/// wins, no race window, and no "resume/take over" path by design.
///
/// On success, also writes the upload_id -> path reverse-index (same TTL)
/// so /upload and /create — which only know the upload_id — can find the
/// lock without scanning.
pub async fn try_claim_path(
    pool: &RedisPool,
    relative_path: &str,
    upload_id: &str,
) -> Result<(), UploadSessionError> {
    let mut conn = (**pool).clone();
    let lock = lock_key(relative_path);
    let value = encode_lock_value(upload_id, 0);

    let claimed: bool = redis::cmd("SET")
        .arg(&lock)
        .arg(&value)
        .arg("EX")
        .arg(UPLOAD_SESSION_TTL_SECONDS)
        .arg("NX")
        .query_async::<_, Option<String>>(&mut conn)
        .await?
        .is_some();

    if !claimed {
        return Err(UploadSessionError::PathLocked);
    }

    // Reverse index. If this fails after the lock succeeded, the lock will
    // simply expire naturally in <=2min and the path frees up again — no
    // permanent damage, just a short-lived orphaned lock.
    let idx = index_key(upload_id);
    let _: () = conn
        .set_ex(&idx, relative_path, UPLOAD_SESSION_TTL_SECONDS)
        .await?;

    Ok(())
}

/// Looks up the session by upload_id via the reverse index, then reads the
/// lock to get current next_part. Returns NotFound if either key is
/// missing (expired, or never existed) — both are "session gone" from the
/// caller's perspective.
pub async fn find_session(
    pool: &RedisPool,
    upload_id: &str,
) -> Result<(UploadSessionInfo, u64), UploadSessionError> {
    let mut conn = (**pool).clone();
    let idx = index_key(upload_id);

    // redis::Value::Nil deserializes to an error for a String target, so a
    // missing key and a real connection error both come back as Err here.
    // Distinguish them explicitly rather than collapsing both into
    // NotFound — a Redis outage should surface as 503, not as "session
    // expired", or a real outage looks identical to normal expiry in logs
    // and to the client.
    let relative_path: Option<String> = conn.get(&idx).await?;
    let Some(relative_path) = relative_path else {
        return Err(UploadSessionError::NotFound);
    };

    let lock = lock_key(&relative_path);
    let value: Option<String> = conn.get(&lock).await?;
    let Some(value) = value else {
        return Err(UploadSessionError::NotFound);
    };

    let (stored_id, next_part) =
        decode_lock_value(&value).ok_or(UploadSessionError::InvalidKeyEncoding)?;

    if stored_id != upload_id {
        // Reverse index pointed at a path whose lock now belongs to a
        // different upload_id — only possible if this session already
        // expired and a brand-new upload won the path in the gap. Treat
        // as gone, not as someone else's data.
        return Err(UploadSessionError::NotFound);
    }

    Ok((
        UploadSessionInfo {
            upload_id: upload_id.to_string(),
            relative_path,
        },
        next_part,
    ))
}

/// Atomically checks the incoming part_number against next-expected; if it
/// matches, increments it and refreshes TTL on both the lock and the
/// reverse-index key (sliding expiry on the whole session).
pub async fn try_advance_part(
    pool: &RedisPool,
    info: &UploadSessionInfo,
    expected_part_number: u64,
) -> Result<Result<u64, u64>, UploadSessionError> {
    let mut conn = (**pool).clone();
    let lock = lock_key(&info.relative_path);
    let idx = index_key(&info.upload_id);

    // Lua script: read lock value, verify it still belongs to this
    // upload_id, compare part number, conditionally bump + refresh TTL on
    // the lock. The reverse-index TTL is refreshed separately right after
    // (cheap, not worth folding into the script).
    const SCRIPT: &str = r#"
        local current = redis.call('GET', KEYS[1])
        if current == false then
            return {-1, -1}
        end
        local sep_pos = nil
        for i = #current, 1, -1 do
            if string.sub(current, i, i) == ':' then
                sep_pos = i
                break
            end
        end
        if sep_pos == nil then
            return {-2, -1}
        end
        local stored_id = string.sub(current, 1, sep_pos - 1)
        local stored_next = tonumber(string.sub(current, sep_pos + 1))
        if stored_id ~= ARGV[1] then
            return {-1, -1}
        end
        local incoming = tonumber(ARGV[2])
        if incoming ~= stored_next then
            return {0, stored_next}
        end
        local new_val = stored_next + 1
        redis.call('SET', KEYS[1], stored_id .. ':' .. new_val, 'EX', ARGV[3])
        return {1, new_val}
    "#;

    let result: Vec<i64> = redis::Script::new(SCRIPT)
        .key(&lock)
        .arg(&info.upload_id)
        .arg(expected_part_number)
        .arg(UPLOAD_SESSION_TTL_SECONDS)
        .invoke_async(&mut conn)
        .await?;

    match result.as_slice() {
        [-1, -1] | [-2, -1] => return Err(UploadSessionError::NotFound),
        [1, new_val] => {
            // Refresh the reverse-index TTL to match (best-effort; if this
            // fails the lock is still correct and the index just expires
            // a hair earlier, which only costs an extra find_session miss).
            let _: Result<(), _> = conn
                .expire(&idx, UPLOAD_SESSION_TTL_SECONDS as i64)
                .await;
            return Ok(Ok(*new_val as u64));
        }
        [0, current] => return Ok(Err(*current as u64)),
        _ => return Err(UploadSessionError::InvalidKeyEncoding),
    }
}

/// Deletes both keys outright on successful /create — no need to wait for
/// natural TTL expiry once the upload is done.
pub async fn delete_session(
    pool: &RedisPool,
    info: &UploadSessionInfo,
) -> Result<(), UploadSessionError> {
    let mut conn = (**pool).clone();
    let lock = lock_key(&info.relative_path);
    let idx = index_key(&info.upload_id);
    let _: i64 = conn.del(&lock).await?;
    let _: i64 = conn.del(&idx).await?;
    Ok(())
}

/// Used by /start's rollback path if file truncation fails after the lock
/// was already claimed.
pub async fn release_claim(
    pool: &RedisPool,
    relative_path: &str,
    upload_id: &str,
) -> Result<(), UploadSessionError> {
    let mut conn = (**pool).clone();
    let lock = lock_key(relative_path);
    let idx = index_key(upload_id);
    let _: i64 = conn.del(&lock).await?;
    let _: i64 = conn.del(&idx).await?;
    Ok(())
}

pub fn absolute_path_for(relative_path: &str) -> PathBuf {
    Path::new(&bucket_root()).join(relative_path)
}