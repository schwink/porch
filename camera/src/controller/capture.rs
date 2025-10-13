use std::error::Error;
use std::path::Path;
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use log::{error, info};

use crate::camera;
use crate::pipeline;

pub async fn start_capture<Tz: TimeZone>(
    camera_service: &camera::CameraService,
    image_storage_dir: Arc<Path>,
    stop_time: chrono::DateTime<Tz>,
    timezone: Tz,
) -> Result<(), Box<dyn Error>> {
    info!(
        "Starting capture at {:?}",
        Utc::now().with_timezone(&timezone)
    );
    let mut handle: camera::StreamHandle = camera_service.start().await;

    let mut prev_p_hash: Option<String> = None;

    loop {
        if chrono::Local::now() >= stop_time {
            info!("Stopping capture at {:?}", stop_time);
            break;
        }

        let frame = handle.rx.recv().await?;

        let p_hash_distance =
            prev_p_hash.map(|p| hamming::distance(p.as_bytes(), &frame.p_hash.as_bytes()));
        prev_p_hash = Some(frame.p_hash.clone());

        if let Some(distance) = p_hash_distance {
            if distance < 10 {
                info!(
                    "Skipping frame at {} due to low p hash distance of {}",
                    frame.timestamp, distance
                );
                // Skip duplicate frames
                continue;
            }
        }

        match pipeline::write_frame_capture_data(image_storage_dir.as_ref(), frame, p_hash_distance)
            .await
        {
            Ok(metadata) => info!("Persisted frame {}", metadata.name),
            Err(e) => {
                error!("Failed to persist frame: {:?}", e)
            }
        };
    }

    Ok(())
}
