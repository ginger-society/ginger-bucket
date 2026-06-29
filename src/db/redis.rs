// src/db/redis.rs

use std::sync::Arc;

/// Async Redis pool — really a cheaply-cloneable connection manager that
/// auto-reconnects under the hood. Matches the convention used by the
/// notification-broker reference: `Arc<redis::aio::ConnectionManager>`,
/// cloned per-operation via `(*pool).clone()`.
pub type RedisPool = Arc<redis::aio::ConnectionManager>;

/// Create and return an async Redis connection pool.
pub async fn create_redis_pool(redis_url: String) -> RedisPool {
    let client = match redis::Client::open(redis_url) {
        Ok(c) => c,
        Err(e) => {
            println!("Invalid Redis URL: {:?}", e);
            std::process::exit(1);
        }
    };

    let manager = match redis::aio::ConnectionManager::new(client).await {
        Ok(manager) => manager,
        Err(e) => {
            println!("Failed to connect to redis: {:?}", e);
            std::process::exit(1);
        }
    };

    // enable keyspace notifications for expired events — required for
    // heartbeat::start_expiry_watcher to receive __keyevent@0__:expired.
    // "Ex" = keyspace events (E) for expired keys (x).
    let mut conn = manager.clone();
    let _: Result<(), _> = redis::cmd("CONFIG")
        .arg("SET")
        .arg("notify-keyspace-events")
        .arg("Ex")
        .query_async(&mut conn)
        .await;

    Arc::new(manager)
}