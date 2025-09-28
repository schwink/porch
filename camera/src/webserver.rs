use tokio::task::JoinHandle;

use async_stream::stream;
use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    response::Response,
    routing::{get, get_service},
};
use std::{convert::Infallible, path::PathBuf};
use std::{net::SocketAddr, sync::Arc};
use tower_http::services::ServeDir;

use crate::camera::CameraService;

pub struct WebServer {
    handle: JoinHandle<()>,
}

/**
 * Administration web interface.
 *
 * Serves a static site at / from var/www.
 */
impl WebServer {
    pub async fn new(camera_service: Arc<CameraService>, image_storage_dir: PathBuf) -> Self {
        let handle = tokio::task::spawn(async move {
            let static_file_service = get_service(ServeDir::new("var/www"));

            let images_static_file_service = get_service(ServeDir::new(image_storage_dir));

            let app = Router::new()
                .fallback_service(static_file_service)
                .nest_service("/images", images_static_file_service)
                .route("/live.mjpeg", get(live))
                .route("/peek.mjpeg", get(peek))
                .with_state(camera_service.clone());

            let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            println!("listening on {}", addr);
            axum::serve(listener, app).await.unwrap();
        });

        WebServer { handle }
    }
}

impl Drop for WebServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

fn mjpeg_stream(mut rx: tokio::sync::broadcast::Receiver<Arc<[u8]>>) -> Response<Body> {
    let stream = stream! {
        loop {
            if let Ok(frame) = rx.recv().await {
                let mut headers = http::header::HeaderMap::<http::HeaderValue>::with_capacity(2);
                headers.insert(http::header::CONTENT_TYPE, http::header::HeaderValue::from_static("image/jpeg"));
                headers.insert(http::header::CONTENT_LENGTH, frame.len().into());

                let result: Result<multipart_stream::Part, Infallible> = Ok(multipart_stream::Part {
                    headers: headers,
                    body: Bytes::from_owner(frame),
                });
                yield result
            }
        }
    };

    let multipart_stream = multipart_stream::serialize(stream, "frame");

    let body = Body::from_stream(multipart_stream);

    Response::builder()
        .header("content-type", "multipart/x-mixed-replace; boundary=frame")
        .header("cache-control", "no-cache")
        .header("pragma", "no-cache")
        .body(body)
        .unwrap()
}

/**
 * A live feed of the camera.
 *
 * Opening this stream turns on the camera.
 */
async fn live(State(camera_service): State<Arc<CameraService>>) -> Response<Body> {
    let camera_handle = camera_service.start().await;
    mjpeg_stream(camera_handle.rx.resubscribe())
}

/**
 * A live feed of the most recent frame captured by the camera.
 *
 * Opening this stream waits for any frames captured by other actors, rather than itself turning on
 * the camera.
 */
async fn peek(State(camera_service): State<Arc<CameraService>>) -> Response<Body> {
    mjpeg_stream(camera_service.frame_rx.resubscribe())
}
