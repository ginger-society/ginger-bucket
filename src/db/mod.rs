use diesel::r2d2::{self, ConnectionManager};
use diesel::PgConnection;
// use mongodb::bson::{doc, Document};
use mongodb::options::ClientOptions;
use mongodb::{Client, Database};
use rocket::fairing::AdHoc;
use std::env;

pub mod redis;

pub fn connect_mongo(mongo_uri: String, mongo_db_name: String) -> AdHoc {
    AdHoc::on_ignite("Connecting to MongoDB", |rocket| async {
        match connect(mongo_uri, mongo_db_name).await {
            Ok(database) => {
                print!("Connected to mongo");
                rocket.manage(database)
            }
            Err(error) => {
                panic!("Cannot connect to instance:: {:?}", error)
            }
        }
    })
}

async fn connect(mongo_uri: String, mongo_db_name: String) -> mongodb::error::Result<Database> {
    let client_options = ClientOptions::parse(mongo_uri).await?;
    let client = Client::with_options(client_options)?;
    let database = client.database(mongo_db_name.as_str());

    println!("MongoDB Connected!");

    Ok(database)
}

// Step 3: Create a function to establish a connection to the database
pub fn connect_rdb() -> r2d2::Pool<ConnectionManager<PgConnection>> {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let manager = ConnectionManager::<PgConnection>::new(database_url);
    r2d2::Pool::builder()
        .max_size(2)
        .build(manager)
        .expect("Failed to create pool.")
}
