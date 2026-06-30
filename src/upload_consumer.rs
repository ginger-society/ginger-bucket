// Reads upload-complete events from the "uploads-completed" RabbitMQ exchange
// and mirrors each finished file to S3. Runs as a background tokio task
// inside the main server process (see main.rs), not a separate binary.
//
// `bucket_path` (as produced by upload_routes::create_upload) is of the
// form "<bucket>/<key...>" — the first path segment IS the S3 bucket name,
// not a prefix within one shared bucket. The bucket is derived per-message,
// not from a fixed env var.
//
// Local files are kept (this is our own copy, not scratch space) — the
// consumer only reads and uploads, never deletes.
//
// Retry policy: on S3 failure, the attempt count for that bucket_path is
// tracked in Redis (not in the AMQP message itself, since lapin's basic
// "redeliver" flag doesn't carry a count). Up to 3 attempts total; after
// the 3rd failure the message is moved to a dead-letter queue
// ("uploads-s3-mirror.dead") for manual inspection/replay, the Redis
// counter is cleared, and the original message is acked so it stops
// looping. On success the Redis counter is cleared.
//
// Required env vars:
//   AMPQ_URI                 — RabbitMQ connection string (shared with upload_rabbit.rs)
//   AWS_ACCESS_KEY_ID         — injected via k8s secret
//   AWS_SECRET_ACCESS_KEY     — injected via k8s secret
//   AWS_REGION                — e.g. "ap-south-1"
//   UPLOAD_MIRROR_BUCKETS     — optional comma-separated allow-list of bucket
//                               names to mirror. If unset, ALL buckets are mirrored.

use aws_config::BehaviorVersion;
use aws_sdk_s3::{primitives::ByteStream, Client as S3Client};
use futures_util::stream::StreamExt;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
        ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
    },
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties, ExchangeKind,
};
use redis::AsyncCommands;
use serde::Deserialize;
use std::collections::HashSet;
use std::env;

use crate::db::redis::RedisPool; // Arc<redis::aio::ConnectionManager>
use crate::upload_rabbit::UPLOADS_EXCHANGE;

const CONSUMER_QUEUE: &str = "uploads-s3-mirror";
const DEAD_LETTER_QUEUE: &str = "uploads-s3-mirror.dead";
const MAX_ATTEMPTS: u32 = 3;
const RETRY_COUNTER_TTL_SECONDS: i64 = 60 * 30; // 30 min — generous vs. retry pace

#[derive(Debug, Deserialize, Clone)]
struct UploadCompleteEvent {
    bucket_path: String,
    absolute_path: String,
    #[allow(dead_code)]
    total_bytes: u64,
}

/// Splits "<bucket>/<key...>" into (bucket, key). The first path segment is
/// the S3 bucket name; everything after the first slash is the object key.
fn split_bucket_and_key(bucket_path: &str) -> Result<(&str, &str), String> {
    bucket_path
        .split_once('/')
        .filter(|(b, k)| !b.is_empty() && !k.is_empty())
        .ok_or_else(|| format!("Invalid bucket_path (expected '<bucket>/<key>'): '{}'", bucket_path))
}

/// Reads UPLOAD_MIRROR_BUCKETS into an allow-list. `None` means "mirror
/// everything" (current default, preserves existing behavior).
fn load_bucket_allowlist() -> Option<HashSet<String>> {
    let raw = env::var("UPLOAD_MIRROR_BUCKETS").ok()?;
    let set: HashSet<String> = raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if set.is_empty() {
        None
    } else {
        Some(set)
    }
}

fn retry_key(bucket_path: &str) -> String {
    format!("upload-mirror-retry:{}", bucket_path)
}

/// Increments and returns the attempt count for this bucket_path, setting
/// a TTL so a stuck/abandoned counter doesn't linger in Redis forever.
///
/// `RedisPool` is `Arc<redis::aio::ConnectionManager>` — there's no
/// "checkout", the ConnectionManager is already a shareable handle that
/// manages its own reconnects internally. Cloning it is cheap.
async fn incr_attempt(redis_pool: &RedisPool, bucket_path: &str) -> redis::RedisResult<u32> {
    let mut conn = (**redis_pool).clone();
    let key = retry_key(bucket_path);
    let count: u32 = conn.incr(&key, 1).await?;
    let _: () = conn.expire(&key, RETRY_COUNTER_TTL_SECONDS).await?;
    Ok(count)
}

/// Clears the retry counter — call on success or once a message has been
/// dead-lettered, so a later re-upload of the same path starts fresh.
async fn clear_attempts(redis_pool: &RedisPool, bucket_path: &str) {
    let mut conn = (**redis_pool).clone();
    let _: redis::RedisResult<()> = conn.del(retry_key(bucket_path)).await;
}

/// Spawns the S3-mirror consumer as a background task and returns
/// immediately. Call this once from main() after RabbitMQ and Redis are
/// available. Internally retries forever on connect failure — never
/// panics the caller.
pub fn start_upload_s3_mirror_consumer(redis_pool: RedisPool) {
    tokio::spawn(async move {
        run(redis_pool).await;
    });
}

async fn run(redis_pool: RedisPool) {
    let aws_config = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let s3 = S3Client::new(&aws_config);

    let allowlist = load_bucket_allowlist();
    match &allowlist {
        Some(set) => println!("[upload-s3-mirror] mirroring restricted to buckets: {:?}", set),
        None => println!("[upload-s3-mirror] UPLOAD_MIRROR_BUCKETS not set — mirroring all buckets"),
    }

    let amqp_uri = env::var("AMPQ_URI")
        .unwrap_or_else(|_| "amqp://user:password@localhost:5672/%2f".to_string());

    let conn = connect_with_retry(&amqp_uri).await;
    let channel = match conn.create_channel().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[upload-s3-mirror] failed to create channel: {:?} — consumer not started", e);
            return;
        }
    };

    if let Err(e) = channel
        .exchange_declare(
            UPLOADS_EXCHANGE,
            ExchangeKind::Direct,
            ExchangeDeclareOptions { durable: true, ..Default::default() },
            FieldTable::default(),
        )
        .await
    {
        eprintln!("[upload-s3-mirror] exchange_declare failed: {:?} — consumer not started", e);
        return;
    }

    if let Err(e) = channel
        .queue_declare(
            CONSUMER_QUEUE,
            QueueDeclareOptions { durable: true, ..Default::default() },
            FieldTable::default(),
        )
        .await
    {
        eprintln!("[upload-s3-mirror] queue_declare failed: {:?} — consumer not started", e);
        return;
    }

    // Plain durable queue for messages that exhausted all retry attempts.
    // Not auto-consumed — intended for manual inspection/replay via a
    // management UI or a one-off tool later.
    if let Err(e) = channel
        .queue_declare(
            DEAD_LETTER_QUEUE,
            QueueDeclareOptions { durable: true, ..Default::default() },
            FieldTable::default(),
        )
        .await
    {
        eprintln!("[upload-s3-mirror] dead-letter queue_declare failed: {:?} — consumer not started", e);
        return;
    }

    if let Err(e) = channel
        .queue_bind(
            CONSUMER_QUEUE,
            UPLOADS_EXCHANGE,
            "",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
    {
        eprintln!("[upload-s3-mirror] queue_bind failed: {:?} — consumer not started", e);
        return;
    }

    let mut consumer = match channel
        .basic_consume(
            CONSUMER_QUEUE,
            "upload-s3-mirror-consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[upload-s3-mirror] basic_consume failed: {:?} — consumer not started", e);
            return;
        }
    };

    println!("[upload-s3-mirror] listening on queue '{}'…", CONSUMER_QUEUE);

    while let Some(delivery) = consumer.next().await {
        let delivery = match delivery {
            Ok(d) => d,
            Err(e) => {
                eprintln!("[upload-s3-mirror] delivery error: {:?}", e);
                continue;
            }
        };

        let payload = String::from_utf8_lossy(&delivery.data).to_string();

        let event: UploadCompleteEvent = match serde_json::from_str(&payload) {
            Ok(e) => e,
            Err(e) => {
                eprintln!(
                    "[upload-s3-mirror] malformed message (acking to discard): {:?}\n  payload: {}",
                    e, payload
                );
                delivery.ack(BasicAckOptions::default()).await.ok();
                continue;
            }
        };

        let (bucket, key) = match split_bucket_and_key(&event.bucket_path) {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("[upload-s3-mirror] {} — acking to discard", e);
                delivery.ack(BasicAckOptions::default()).await.ok();
                continue;
            }
        };

        if let Some(set) = &allowlist {
            if !set.contains(bucket) {
                println!(
                    "[upload-s3-mirror] skipping '{}' (not in UPLOAD_MIRROR_BUCKETS) — acking",
                    bucket
                );
                delivery.ack(BasicAckOptions::default()).await.ok();
                continue;
            }
        }

        println!(
            "[upload-s3-mirror] mirroring {} → s3://{}/{}",
            event.absolute_path, bucket, key
        );

        match mirror_to_s3(&s3, bucket, key, &event.absolute_path).await {
            Ok(()) => {
                println!("[upload-s3-mirror] ✅ s3://{}/{}", bucket, key);
                clear_attempts(&redis_pool, &event.bucket_path).await;
                delivery.ack(BasicAckOptions::default()).await.ok();
            }
            Err(e) => {
                let attempt = match incr_attempt(&redis_pool, &event.bucket_path).await {
                    Ok(n) => n,
                    Err(redis_err) => {
                        // Can't reliably count attempts — fail safe by
                        // treating this as the final attempt so we don't
                        // loop forever just because Redis is unreachable.
                        eprintln!(
                            "[upload-s3-mirror] Redis error tracking retries for '{}': {:?} — treating as final attempt",
                            event.bucket_path, redis_err
                        );
                        MAX_ATTEMPTS
                    }
                };

                eprintln!(
                    "[upload-s3-mirror] ❌ S3 upload failed for s3://{}/{} (attempt {}/{}): {:?}",
                    bucket, key, attempt, MAX_ATTEMPTS, e
                );

                if attempt >= MAX_ATTEMPTS {
                    eprintln!(
                        "[upload-s3-mirror] giving up on '{}' after {} attempts — moving to dead-letter queue",
                        event.bucket_path, attempt
                    );

                    if let Err(publish_err) = channel
                        .basic_publish(
                            "",
                            DEAD_LETTER_QUEUE,
                            BasicPublishOptions::default(),
                            &delivery.data,
                            BasicProperties::default(),
                        )
                        .await
                    {
                        // If even the dead-letter publish fails, requeue
                        // the original so the event isn't lost outright.
                        eprintln!(
                            "[upload-s3-mirror] failed to publish to dead-letter queue: {:?} — requeueing original",
                            publish_err
                        );
                        delivery
                            .nack(BasicNackOptions { requeue: true, ..Default::default() })
                            .await
                            .ok();
                        continue;
                    }

                    clear_attempts(&redis_pool, &event.bucket_path).await;
                    delivery.ack(BasicAckOptions::default()).await.ok();
                } else {
                    // Still have attempts left — requeue for another try.
                    delivery
                        .nack(BasicNackOptions { requeue: true, ..Default::default() })
                        .await
                        .ok();
                }
            }
        }
    }

    eprintln!("[upload-s3-mirror] consumer stream ended unexpectedly");
}

/// Streams the local file straight to S3 via ByteStream::from_path, rather
/// than reading the whole file into memory first. This keeps memory flat
/// regardless of file size and lets the SDK handle chunked transfer.
async fn mirror_to_s3(
    s3: &S3Client,
    bucket: &str,
    key: &str,
    absolute_path: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let stream = ByteStream::from_path(absolute_path).await?;

    s3.put_object()
        .bucket(bucket)
        .key(key)
        .body(stream)
        .send()
        .await?;

    Ok(())
}

async fn connect_with_retry(uri: &str) -> Connection {
    loop {
        match Connection::connect(uri, ConnectionProperties::default()).await {
            Ok(conn) => {
                println!("[upload-s3-mirror] RabbitMQ connected");
                return conn;
            }
            Err(e) => {
                eprintln!("[upload-s3-mirror] RabbitMQ connect failed: {:?} — retrying in 5s", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_bucket_and_key() {
        assert_eq!(split_bucket_and_key("tst/ginger-infra").unwrap(), ("tst", "ginger-infra"));
        assert_eq!(
            split_bucket_and_key("tst/nested/path/file.bin").unwrap(),
            ("tst", "nested/path/file.bin")
        );
    }

    #[test]
    fn rejects_missing_separator() {
        assert!(split_bucket_and_key("no-slash-here").is_err());
    }

    #[test]
    fn rejects_empty_bucket_or_key() {
        assert!(split_bucket_and_key("/just-a-key").is_err());
        assert!(split_bucket_and_key("just-a-bucket/").is_err());
    }
}