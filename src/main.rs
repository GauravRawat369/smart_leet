use axum::{Router, routing::get, Json};
use tokio::net::TcpListener;
use backend::types::HealthCheckResponse;

async fn health_check() -> Json<HealthCheckResponse> {
    println!("Health check endpoint called");
    Json(HealthCheckResponse {
        message: "OK".to_string(),
    })
}
async fn create_app() -> Router {
    Router::new().route("/health", get(health_check))
}

#[tokio::main]
async fn main() {
    
    let app = create_app().await;

    let listener = TcpListener::bind("0.0.0.0:8080").await.unwrap();
    axum::serve(listener, app).await.unwrap();

}
