use axum::{routing::get, Router};
use std::{env, ffi::OsStr};

use redis::Commands;

#[tokio::main]
async fn main() {
    let app = Router::new().route("/", get(|| async {
        let client = redis::Client::open("redis://:6284001732@127.0.0.1").unwrap();
        let mut con = client.get_connection().unwrap();
        let _ : () = con.incr("my_key", 1).unwrap();
        let res : i32 = con.get("my_key").unwrap();

        return format!("Hello, world {res}");
    }));

    let port = match env::var_os("PORT") {
        Some(val) => val,
        None => OsStr::new("3000").into()
    };

    let unwrapped = port.to_str().unwrap();

    axum::Server::bind(&format!("0.0.0.0:{unwrapped}").parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
