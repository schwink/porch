use futures::Stream;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

use crate::{api::ApiError, camera::StreamHandle};
use async_stream::{stream, try_stream};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response, Sse},
    routing::{get, get_service},
};
use log::info;
use std::{convert::Infallible, path::Path, time::Duration};
use std::{net::SocketAddr, sync::Arc};
use tower_http::services::ServeDir;

use crate::api::Api;
use crate::camera::CameraService;
use crate::store::FrameStore;

#[derive(Clone)]
struct WebServerState {
    camera_service: Arc<CameraService>,
    api: Arc<Api>,
    frame_store: Arc<FrameStore>,
}

pub struct WebServer {
    handle: JoinHandle<()>,
}

/**
 * Administration web interface.
 *
 * Serves a static site at / from var/www.
 */
impl WebServer {
    pub async fn new(
        camera_service: Arc<CameraService>,
        api: Arc<Api>,
        frame_store: Arc<FrameStore>,
        image_storage_dir: Box<Path>,
    ) -> Self {
        let handle = tokio::task::spawn(async move {
            let state = WebServerState {
                camera_service,
                api,
                frame_store,
            };

            let static_file_service = get_service(ServeDir::new("var/www"));

            let images_static_file_service = get_service(ServeDir::new(image_storage_dir));

            let app = Router::new()
                .fallback_service(static_file_service)
                .nest_service("/frames", images_static_file_service)
                .route("/api/frames", get(api_frames))
                .route("/api/frames/latest", get(api_frames_latest))
                .route("/live.mjpeg", get(live))
                .route("/peek.mjpeg", get(peek))
                .with_state(state.clone());

            let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            info!("listening on {}", addr);
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

fn mjpeg_stream(
    mut rx: tokio::sync::broadcast::Receiver<crate::camera::Frame>,
    handle: Option<StreamHandle>,
) -> Response<Body> {
    let stream = stream! {
        // Pin the StreamHandle into the generator state so it stays live for the
        // duration of the returned stream.
        let _pinned_handle = handle;

        loop {
            if let Ok(frame) = rx.recv().await {
                let mut headers = http::header::HeaderMap::<http::HeaderValue>::with_capacity(2);
                headers.insert(http::header::CONTENT_TYPE, http::header::HeaderValue::from_static("image/jpeg"));
                headers.insert(http::header::CONTENT_LENGTH, frame.jpeg.len().into());

                let result: Result<multipart_stream::Part, Infallible> = Ok(multipart_stream::Part {
                    headers: headers,
                    body: Bytes::from_owner(frame.jpeg),
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
async fn live(State(state): State<WebServerState>) -> Response<Body> {
    let camera_handle = state.camera_service.start().await;
    let rx = camera_handle.rx.resubscribe();
    mjpeg_stream(rx, Some(camera_handle))
}

/**
 * A live feed of the most recent frame captured by the camera.
 *
 * Opening this stream waits for any frames captured by other actors, rather than itself turning on
 * the camera.
 */
async fn peek(State(state): State<WebServerState>) -> Response<Body> {
    mjpeg_stream(state.camera_service.frame_rx.resubscribe(), None)
}

#[derive(Debug, Serialize)]
pub struct ApiErrorData {
    message: String,
}

#[derive(Deserialize)]
struct ApiFramesQueryParams {
    pub last: Option<usize>,
    pub before: Option<String>,
}

async fn api_frames(
    State(state): State<WebServerState>,
    Query(params): Query<ApiFramesQueryParams>,
) -> Result<Json<crate::api::Frames>, ApiError> {
    state
        .api
        .frames(params.last, params.before)
        .await
        .map(|f| Json(f))
}

#[derive(Deserialize)]
struct ApiFramesLatestQueryParams {
    pub after: Option<String>,
}

#[axum::debug_handler]
async fn api_frames_latest(
    State(state): State<WebServerState>,
    Query(params): Query<ApiFramesLatestQueryParams>,
) -> Result<
    Sse<impl Stream<Item = Result<axum::response::sse::Event, Infallible>>>,
    crate::api::ApiError,
> {
    let mut rx = state.frame_store.frame_rx.resubscribe();

    let catchup_frames = if params.after.is_some() {
        let ls = state
            .frame_store
            .list_frames(None, None, None, params.after)
            .await;
        match ls {
            Ok(f) => f,
            Err(_) => {
                return Err(ApiError {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    message: "Error fetching frames".to_string(),
                });
            }
        }
    } else {
        Vec::new()
    };

    Ok(Sse::new(try_stream! {
        yield axum::response::sse::Event::default()
            .retry(Duration::from_secs(3));

        for metadata in catchup_frames {
            let edge: crate::api::FrameEdge = metadata.into();
            yield edge.into();
        }

        while let Ok(metadata) = rx.recv().await {
            let edge: crate::api::FrameEdge = metadata.into();
            yield edge.into();
        }
    })
    .keep_alive(axum::response::sse::KeepAlive::new().interval(Duration::from_secs(1))))
}

impl IntoResponse for crate::api::ApiError {
    fn into_response(self) -> Response {
        (
            self.code,
            Json(ApiErrorData {
                message: self.message,
            }),
        )
            .into_response()
    }
}

impl Into<crate::api::FrameEdge> for crate::store::FrameMetadata {
    fn into(self) -> crate::api::FrameEdge {
        let frame = crate::api::Frame {
            name: format!("{}.jpg", self.name),
            timestamp: self.timestamp,
            p_hash: self.p_hash,
            p_hash_distance: self.p_hash_distance,
        };
        crate::api::FrameEdge {
            node: frame,
            cursor: self.name,
        }
    }
}

impl Into<axum::response::sse::Event> for crate::api::FrameEdge {
    fn into(self) -> axum::response::sse::Event {
        let json = serde_json::to_string(&self).unwrap();
        axum::response::sse::Event::default().data(json)
    }
}
