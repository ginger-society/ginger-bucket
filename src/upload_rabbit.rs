// src/upload_rabbit.rs
//
// RabbitMQ publish-side plumbing for the upload service, following the same
// shape as the existing broker's `RabbitPool` (see shared.rs): a single
// long-lived channel behind a Mutex, reconnect-with-backoff at startup, and a
// thin `publish_*` helper on top.
//
// Exchange: a dedicated direct exchange "uploads-completed" is used rather
// than the broker service's "real-time-updates" fanout — these are different
// services with different consumers, and a direct exchange (routing key per
// downstream queue) is the better fit for "tell exactly one processing
// pipeline this file is ready" rather than "broadcast to every instance."
// Swap ExchangeKind/topology here once the actual downstream consumer(s) are
// decided — this is infrastructure, not the message contract.

use lapin::{
    options::{BasicPublishOptions, ExchangeDeclareOptions},
    BasicProperties, Channel as RabbitChannel, Connection as LapinConnection,
    ConnectionProperties, ExchangeKind,
};
use std::sync::Arc;
use tokio::sync::Mutex;

pub const UPLOADS_EXCHANGE: &str = "uploads-completed";

pub struct UploadRabbitPool {
    pub channel: Arc<Mutex<RabbitChannel>>,
}

pub type UploadRabbitPoolRef = Arc<UploadRabbitPool>;

impl UploadRabbitPool {
    /// Connects and declares the exchange, retrying with a fixed backoff
    /// until it succeeds. Call once at startup and `.manage()` the result —
    /// same pattern as the broker service's `RabbitPool::new()`.
    pub async fn new() -> Self {
        loop {
            match connect_uploads_publisher().await {
                Ok(channel) => {
                    println!("[upload-rabbitmq] persistent publish channel established");
                    return Self {
                        channel: Arc::new(Mutex::new(channel)),
                    };
                }
                Err(e) => {
                    eprintln!(
                        "[upload-rabbitmq] connect failed: {:?} — retrying in 5s",
                        e
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }
}

async fn connect_uploads_publisher() -> Result<RabbitChannel, lapin::Error> {
    let addr = std::env::var("AMPQ_URI")
        .unwrap_or_else(|_| "amqp://user:password@localhost:5672/%2f".to_string());

    let conn = LapinConnection::connect(&addr, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;

    channel
        .exchange_declare(
            UPLOADS_EXCHANGE,
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            Default::default(),
        )
        .await?;

    Ok(channel)
}

/// Publishes a message announcing a finished upload. The message body is a
/// TODO — left as a raw string so the actual schema (file metadata, content
/// type, checksum, whatever downstream needs) can be decided separately
/// without touching this plumbing again.
///
/// `routing_key` is left as a parameter rather than hardcoded so the caller
/// can route by upload type/destination once that's decided (e.g. by file
/// extension, or a `kind` field from the original /start request) — pass
/// "" for now if you don't need routing yet and just want every consumer
/// queue bound with routing_key "" to receive everything.
pub async fn publish_upload_complete(
    pool: &UploadRabbitPoolRef,
    routing_key: &str,
    message_body: &str,
) {
    let ch = pool.channel.lock().await;
    if let Err(e) = ch
        .basic_publish(
            UPLOADS_EXCHANGE,
            routing_key,
            BasicPublishOptions::default(),
            message_body.as_bytes(),
            BasicProperties::default(),
        )
        .await
    {
        eprintln!("[upload-rabbitmq] publish failed: {:?}", e);
    }
}