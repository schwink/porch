use std::error::Error;
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use log::{error, info, warn};
use tokio::sync::broadcast::error::RecvError;
use tracing::subscriber::DefaultGuard;
use tracing::{Level, span};
use tracing_chrome::{ChromeLayerBuilder, FlushGuard};
use tracing_subscriber::{prelude::*, registry::Registry};

use crate::camera;
use crate::inference;
use crate::store;

pub async fn start_capture<Tz: TimeZone>(
    camera_service: &camera::CameraService,
    frame_store: Arc<store::FrameStore>,
    inference_service: Arc<inference::InferenceService>,
    trace_dir: &Option<std::path::PathBuf>,
    stop_time: chrono::DateTime<Tz>,
    timezone: Tz,
) -> Result<(), Box<dyn Error>> {
    let timestamp = Utc::now();
    info!(
        "Starting capture at {:?}",
        timestamp.with_timezone(&timezone)
    );
    let mut handle: camera::StreamHandle = camera_service.start().await;

    let mut prev_p_hash: Option<String> = None;

    let _trace_guard: Option<(DefaultGuard, FlushGuard)> = match trace_dir {
        Some(trace_dir) => {
            let trace_file_name = format!(
                "{}_capture",
                crate::api::time_to_file_basename::<chrono::Utc>(&timestamp)
            );
            let mut trace_file = trace_dir.join(trace_file_name);
            trace_file.set_extension("json");

            let (chrome_layer, flush_guard) = ChromeLayerBuilder::new().file(trace_file).build();

            // Register the ChromeLayer with the tracing subscriber
            let subscriber = Registry::default().with(chrome_layer);

            let trace_guard = tracing::subscriber::set_default(subscriber);
            Some((trace_guard, flush_guard))
        }
        None => None,
    };

    loop {
        if chrono::Local::now() >= stop_time {
            info!("Stopping capture at {:?}", stop_time);
            break;
        }

        let frame = match handle.rx.recv().await {
            Ok(frame) => frame,
            Err(e) => match e {
                RecvError::Lagged(num_dropped) => {
                    warn!("Dropped {} frames", num_dropped);
                    continue;
                }
                RecvError::Closed => {
                    error!("Camera stream closed");
                    break;
                }
            },
        };

        {
            let span = span!(Level::TRACE, "frame");
            let _enter = span.enter();

            let p_hash_distance = prev_p_hash
                .as_ref()
                .map(|p| hamming::distance(p.as_bytes(), &frame.p_hash.as_bytes()));
            if let Some(distance) = p_hash_distance {
                if distance < 20 {
                    info!(
                        "Skipping frame at {} due to low p hash distance of {}",
                        frame.timestamp, distance
                    );
                    // Skip duplicate frames
                    continue;
                }
            }
            prev_p_hash = Some(frame.p_hash.clone());

            let metadata = {
                let span = span!(Level::TRACE, "store");
                let _enter = span.enter();

                match frame_store
                    .write_frame_capture_data(&frame, p_hash_distance)
                    .await
                {
                    Ok(metadata) => {
                        info!("Persisted frame {}", metadata.name);
                        metadata
                    }
                    Err(e) => {
                        error!("Failed to persist frame: {:?}", e);
                        continue;
                    }
                }
            };

            let inference_service = inference_service.clone();
            let tensor: Arc<[f32]> = frame.inference_tensor.clone();
            {
                let span = span!(Level::TRACE, "inference");
                let _enter = span.enter();

                let results = match inference_service.run(tensor.as_ref()) {
                    Ok(results) => results,
                    Err(e) => {
                        error!("Inference failed for {}: {:?}", metadata.name, e);
                        continue;
                    }
                };

                match frame_store.write_inference(&frame, results).await {
                    Ok(_) => info!("Persisted inference results for {}", metadata.name),
                    Err(e) => {
                        error!(
                            "Failed to store inference results for {}: {:?}",
                            metadata.name, e
                        )
                    }
                };
            }
        }
    }

    Ok(())
}
