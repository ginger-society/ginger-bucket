#[macro_use]
extern crate rocket;

use dotenv::dotenv;
use rocket::{Build, Rocket};
use rocket_okapi::openapi_get_routes;
use rocket_okapi::swagger_ui::{make_swagger_ui, SwaggerUIConfig};
use rocket_prometheus::PrometheusMetrics;
use std::env;
use std::sync::Arc;
use uuid::Uuid;

use db::redis::create_redis_pool;
use upload_rabbit::UploadRabbitPool;

mod db;
mod fairings;
mod handlers;
mod middlewares;
mod models;
mod routes;
mod upload_expiry_watcher;
mod upload_rabbit;
mod upload_routes;
mod upload_session;

const SERVICE_PREFIX: &str = "api";

#[tokio::main]
async fn main() {
    dotenv().ok();

    println!("Starting server...");

    let prometheus = PrometheusMetrics::new();

    let mut server = rocket::build()
        .attach(fairings::cors::CORS)
        .attach(prometheus.clone())
        .mount(
            format!("/{}/", SERVICE_PREFIX),
            openapi_get_routes![
                routes::index,
                upload_routes::start_upload,
                upload_routes::upload_part,
                upload_routes::create_upload,
            ],
        )
        .mount(
            format!("/{}/api-docs", SERVICE_PREFIX),
            make_swagger_ui(&SwaggerUIConfig {
                url: "../openapi.json".to_owned(),
                ..Default::default()
            }),
        )
        .mount(format!("/{}/metrics", SERVICE_PREFIX), prometheus);

    match env::var("MONGO_URI") {
        Ok(mongo_uri) => match env::var("MONGO_DB_NAME") {
            Ok(mongo_db_name) => {
                println!("Attempting to connect to mongo");
                server = server.manage(db::connect_mongo(mongo_uri, mongo_db_name))
            }
            Err(_) => {
                println!("Not connecting to mongo, missing MONGO_DB_NAME")
            }
        },
        Err(_) => println!("Not connecting to mongo, missing MONGO_URI"),
    };

    match env::var("REDIS_URI") {
        Ok(redis_uri) => {
            println!("Attempting to connect to redis");
            let redis_pool = create_redis_pool(redis_uri.clone()).await;

            // Upload-session expiry watcher needs its own Redis connection
            // (pubsub, not pooled command traffic) — same pattern as the
            // other keyspace-notification watchers in this codebase.
            upload_expiry_watcher::start_upload_expiry_watcher(redis_uri.clone(), 0);

            server = server.manage(redis_pool);
        }
        Err(_) => println!("Not connecting to redis"),
    }

    // RabbitMQ publish pool for upload-complete notifications. Connects
    // (with retry/backoff inside UploadRabbitPool::new()) before the server
    // starts serving requests.
    println!("Connecting to RabbitMQ for upload notifications...");
    let upload_rabbit_pool = Arc::new(UploadRabbitPool::new().await);
    server = server.manage(upload_rabbit_pool);

    server.launch().await.expect("Failed to launch Rocket");
}

// Unit testings
#[cfg(test)]
mod tests;