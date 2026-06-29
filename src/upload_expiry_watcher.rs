// src/upload_expiry_watcher.rs
//
// Subscribes to Redis's `__keyevent@<db>__:expired` channel and, for any
// expired key matching our path-lock naming scheme (`upload:lock:*`),
// deletes the partial file it points to.
//
// This is the crash-recovery half of the upload flow: if a client starts an
// upload and never calls /create again within the TTL window (~2 min,
// refreshed per chunk), the lock key expires naturally, this listener
// notices, and the half-written file is removed — both so the bucket
// doesn't accumulate orphaned partial files, and so the path is genuinely
// free for the next attempt.
//
// Note: the upload_id reverse-index key (`upload:id:*`) expires
// independently around the same time but is intentionally ignored here —
// its disappearance is just a dangling pointer with no cleanup action
// needed; only the lock key's expiry signals "this path's file is now
// orphaned."
//
// Requires `notify-keyspace-events` to include `Ex` on the Redis server —
// matches the existing `connect_redis()` setup in shared.rs, which already
// issues that CONFIG SET.
//
// Mirrors the connect/psubscribe/into_on_message/reconnect-loop shape used
// by `start_redis_pubsub_bridge` and `start_pending_call_expiry_watcher` in
// shared.rs, rather than introducing a different pubsub pattern.

use futures::StreamExt;

use crate::upload_session;

/// Spawns the watcher as a background task. `redis_url` should point at the
/// *same logical Redis* used for the lock keys (i.e. whatever URL backs the
/// `RedisPool` passed to upload_session functions) — a separate plain
/// `redis::Client` is opened here rather than reusing the `ConnectionManager`
/// pool, because pubsub needs its own dedicated connection that isn't shared
/// with regular command traffic (same reason shared.rs's watchers do this).
pub fn start_upload_expiry_watcher(redis_url: String, db_index: u8) {
    tokio::spawn(async move {
        loop {
            let client = match redis::Client::open(redis_url.clone()) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("[upload-expiry] client error: {:?} — retrying in 2s", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                    continue;
                }
            };

            let mut pubsub = match client.get_async_pubsub().await {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("[upload-expiry] connect failed: {:?} — retrying in 2s", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                    continue;
                }
            };

            let channel = format!("__keyevent@{db_index}__:expired");
            if let Err(e) = pubsub.psubscribe(&channel).await {
                eprintln!("[upload-expiry] psubscribe failed: {:?} — retrying in 2s", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                continue;
            }

            println!("[upload-expiry] watcher active — subscribed to {}", channel);

            let mut stream = pubsub.into_on_message();

            while let Some(msg) = stream.next().await {
                let expired_key: String = match msg.get_payload() {
                    Ok(k) => k,
                    Err(_) => continue,
                };

                let Some(relative_path) = upload_session::parse_lock_key(&expired_key) else {
                    // Not one of ours — some other key expired in the same
                    // db, or it's our own reverse-index key expiring (no-op).
                    continue;
                };

                let absolute_path = upload_session::absolute_path_for(&relative_path);
                match tokio::fs::remove_file(&absolute_path).await {
                    Ok(()) => {
                        println!(
                            "[upload-expiry] removed orphaned upload file: {}",
                            absolute_path.display(),
                        );
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        // /start was called but no chunk ever landed — nothing to clean up.
                    }
                    Err(e) => {
                        eprintln!(
                            "[upload-expiry] failed to remove {}: {:?}",
                            absolute_path.display(),
                            e
                        );
                    }
                }
            }

            eprintln!("[upload-expiry] stream ended — reconnecting in 2s...");
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        }
    });
}